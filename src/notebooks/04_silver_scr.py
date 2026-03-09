# Databricks notebook source
# MAGIC %md
# MAGIC # 04_silver_scr - Structure SCR risk modules and apply correlation matrix
# MAGIC
# MAGIC Reads bronze risk factors and SCR parameters, applies the EIOPA inter-module
# MAGIC correlation matrix (square-root formula), and produces structured SCR module
# MAGIC data for S.25.01.
# MAGIC
# MAGIC **Source tables:** `bronze_risk_factors`, `bronze_scr_parameters`
# MAGIC **Target table:** `silver_scr_modules`

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_date", "2025-12-31")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")

reporting_year = int(reporting_date[:4])
reporting_period = f"{reporting_year}-Q4"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Reporting period: {reporting_period}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

# COMMAND ----------

import numpy as np
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("04_silver_scr")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read risk factors and aggregate by top-level module

# COMMAND ----------

df_risk_factors = spark.sql(f"""
    SELECT risk_module,
           risk_sub_module,
           gross_capital_charge_eur,
           diversification_within_module_eur,
           net_capital_charge_eur
      FROM {catalog}.{schema}.bronze_risk_factors
     WHERE reporting_period = '{reporting_period}'
""")

risk_factor_count = df_risk_factors.count()
print(f"Risk factor rows loaded: {risk_factor_count}")

# Collect all rows for Python processing
risk_rows = df_risk_factors.collect()

# Build a dict of all sub-module data keyed by (risk_module, risk_sub_module)
sub_module_data = {}
for r in risk_rows:
    key = (r["risk_module"], r["risk_sub_module"])
    sub_module_data[key] = {
        "gross": r["gross_capital_charge_eur"],
        "div": r["diversification_within_module_eur"],
        "net": r["net_capital_charge_eur"],
    }

# Aggregate net capital charge by top-level module
module_net_charges = {}
for r in risk_rows:
    mod = r["risk_module"]
    module_net_charges[mod] = module_net_charges.get(mod, 0.0) + r["net_capital_charge_eur"]

print("\nModule-level net capital charges (EUR M):")
for mod, charge in sorted(module_net_charges.items()):
    print(f"  {mod:<25} {charge / 1e6:>10.1f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read correlation matrix from scr_parameters

# COMMAND ----------

df_corr_params = spark.sql(f"""
    SELECT parameter_name, CAST(parameter_value AS DOUBLE) AS corr_value
      FROM {catalog}.{schema}.bronze_scr_parameters
     WHERE parameter_category = 'correlation'
       AND reporting_period = '{reporting_period}'
""")

corr_rows = df_corr_params.collect()
print(f"Correlation parameters loaded: {len(corr_rows)}")

# Build the 5x5 correlation matrix
# Module order: market, default, life, health, non_life
MODULE_ORDER = ["market", "default", "life", "health", "non_life"]
n_modules = len(MODULE_ORDER)
mod_idx = {m: i for i, m in enumerate(MODULE_ORDER)}

corr_matrix = np.eye(n_modules)
for r in corr_rows:
    name = r["parameter_name"]  # e.g. corr_market_default
    parts = name.replace("corr_", "").split("_", 1)
    if len(parts) == 2:
        m1, m2 = parts[0], parts[1]
        # Handle multi-word module names (non_life)
        if m1 in mod_idx and m2 in mod_idx:
            i, j = mod_idx[m1], mod_idx[m2]
            corr_matrix[i, j] = r["corr_value"]
            corr_matrix[j, i] = r["corr_value"]  # symmetric

# Handle non_life correlations specially since the name contains underscore
# Re-parse with awareness of the known module names
for r in corr_rows:
    name = r["parameter_name"].replace("corr_", "")
    # Try all known module pairs
    for m1 in MODULE_ORDER:
        if name.startswith(m1 + "_"):
            m2_candidate = name[len(m1) + 1:]
            if m2_candidate in MODULE_ORDER:
                i, j = mod_idx[m1], mod_idx[m2_candidate]
                corr_matrix[i, j] = r["corr_value"]
                corr_matrix[j, i] = r["corr_value"]
                break

print("\nInter-module correlation matrix:")
header = f"{'':>12}" + "".join(f"{m:>12}" for m in MODULE_ORDER)
print(header)
for i, m in enumerate(MODULE_ORDER):
    row_str = f"{m:>12}" + "".join(f"{corr_matrix[i, j]:>12.2f}" for j in range(n_modules))
    print(row_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate BSCR using the square-root formula

# COMMAND ----------

# Build the SCR vector in module order
# Life SCR = 0 for this P&C demo but keep the structure
scr_vector = np.array([
    module_net_charges.get("market", 0.0),
    module_net_charges.get("counterparty_default", 0.0),
    0.0,  # life SCR = 0 for P&C demo
    module_net_charges.get("health", 0.0),
    module_net_charges.get("non_life", 0.0),
])

print("SCR vector (EUR M):")
for m, v in zip(MODULE_ORDER, scr_vector):
    print(f"  {m:<25} {v / 1e6:>10.1f}M")

# BSCR = sqrt(sum_i sum_j Corr(i,j) * SCR_i * SCR_j) + intangible_scr
bscr_sum = 0.0
for i in range(n_modules):
    for j in range(n_modules):
        bscr_sum += corr_matrix[i, j] * scr_vector[i] * scr_vector[j]

bscr_pre_intangible = np.sqrt(bscr_sum)
intangible_scr = module_net_charges.get("intangible", 0.0)
bscr = bscr_pre_intangible + intangible_scr

# Sum of undiversified module charges (for diversification benefit)
sum_of_modules = sum(scr_vector) + intangible_scr
inter_module_diversification = bscr - sum_of_modules  # negative = benefit

print(f"\nSum of module SCRs (undiversified): EUR {sum_of_modules / 1e6:.1f}M")
print(f"BSCR (after diversification):      EUR {bscr / 1e6:.1f}M")
print(f"Inter-module diversification:       EUR {inter_module_diversification / 1e6:.1f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate adjustments (operational risk, LAC TP, LAC DT)

# COMMAND ----------

# Operational risk from bronze_risk_factors
operational_risk = module_net_charges.get("operational", 0.0)

# LAC adjustments from bronze_scr_parameters
df_lac = spark.sql(f"""
    SELECT parameter_name, CAST(parameter_value AS DOUBLE) AS param_value
      FROM {catalog}.{schema}.bronze_scr_parameters
     WHERE parameter_name IN ('lac_tp_amount', 'lac_dt_amount')
       AND reporting_period = '{reporting_period}'
""")

lac_params = {r["parameter_name"]: r["param_value"] for r in df_lac.collect()}
lac_tp = -abs(lac_params.get("lac_tp_amount", 0.0))  # ensure negative
lac_dt = -abs(lac_params.get("lac_dt_amount", 0.0))  # ensure negative

print(f"Operational risk:  EUR {operational_risk / 1e6:>10.1f}M")
print(f"LAC TP:            EUR {lac_tp / 1e6:>10.1f}M")
print(f"LAC DT:            EUR {lac_dt / 1e6:>10.1f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Calculate final SCR

# COMMAND ----------

final_scr = bscr + operational_risk + lac_tp + lac_dt

print(f"BSCR:              EUR {bscr / 1e6:>10.1f}M")
print(f"+ Operational:     EUR {operational_risk / 1e6:>10.1f}M")
print(f"+ LAC TP:          EUR {lac_tp / 1e6:>10.1f}M")
print(f"+ LAC DT:          EUR {lac_dt / 1e6:>10.1f}M")
print(f"= Final SCR:       EUR {final_scr / 1e6:>10.1f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Build output table `silver_scr_modules`

# COMMAND ----------

# Display name mapping for modules
MODULE_DISPLAY_NAMES = {
    "market": "Market risk",
    "counterparty_default": "Counterparty default risk",
    "non_life": "Non-life underwriting risk",
    "health": "Health underwriting risk",
    "life": "Life underwriting risk",
}

# Sub-module definitions per module (must match bronze_risk_factors risk_sub_module values)
MODULE_SUB_MODULES = {
    "market": ["interest_rate", "equity", "property", "spread", "currency", "concentration"],
    "counterparty_default": ["type1", "type2"],
    "non_life": ["premium_reserve", "catastrophe", "lapse"],
    "health": ["similar_life", "similar_nl", "catastrophe"],
}

output_rows = []

# --- Sub-module and module-level rows for each BSCR module ---
for mod_key in ["market", "counterparty_default", "non_life", "health"]:
    display_name = MODULE_DISPLAY_NAMES[mod_key]
    sub_modules = MODULE_SUB_MODULES[mod_key]

    mod_gross_total = 0.0
    mod_div_total = 0.0
    mod_net_total = 0.0

    # Individual sub-module rows
    for sub in sub_modules:
        data = sub_module_data.get((mod_key, sub), {"gross": 0.0, "div": 0.0, "net": 0.0})
        mod_gross_total += data["gross"]
        mod_div_total += data["div"]
        mod_net_total += data["net"]

        output_rows.append({
            "module_name": display_name,
            "sub_module_name": sub,
            "level": "sub_module",
            "gross_capital_charge": data["gross"],
            "intra_module_diversification": data["div"],
            "net_capital_charge": data["net"],
            "is_diversification": False,
            "parent_module": display_name,
            "reporting_period": reporting_period,
        })

    # Diversification row within the module
    output_rows.append({
        "module_name": display_name,
        "sub_module_name": "diversification",
        "level": "sub_module",
        "gross_capital_charge": 0.0,
        "intra_module_diversification": mod_div_total,
        "net_capital_charge": mod_div_total,
        "is_diversification": True,
        "parent_module": display_name,
        "reporting_period": reporting_period,
    })

    # Module TOTAL row
    output_rows.append({
        "module_name": display_name,
        "sub_module_name": "TOTAL",
        "level": "module",
        "gross_capital_charge": mod_gross_total,
        "intra_module_diversification": mod_div_total,
        "net_capital_charge": mod_net_total,
        "is_diversification": False,
        "parent_module": None,
        "reporting_period": reporting_period,
    })

# --- BSCR before diversification (sum of module net charges) ---
output_rows.append({
    "module_name": "BSCR (before diversification)",
    "sub_module_name": "sum_of_modules",
    "level": "intermediate",
    "gross_capital_charge": sum_of_modules,
    "intra_module_diversification": 0.0,
    "net_capital_charge": sum_of_modules,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- Inter-module diversification benefit ---
output_rows.append({
    "module_name": "Inter-module diversification",
    "sub_module_name": "diversification",
    "level": "intermediate",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": inter_module_diversification,
    "net_capital_charge": inter_module_diversification,
    "is_diversification": True,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- BSCR total ---
output_rows.append({
    "module_name": "BSCR",
    "sub_module_name": "total",
    "level": "bscr",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": 0.0,
    "net_capital_charge": bscr,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- Operational risk ---
output_rows.append({
    "module_name": "Operational risk",
    "sub_module_name": "total",
    "level": "adjustment",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": 0.0,
    "net_capital_charge": operational_risk,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- LAC technical provisions ---
output_rows.append({
    "module_name": "LAC technical provisions",
    "sub_module_name": "total",
    "level": "adjustment",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": 0.0,
    "net_capital_charge": lac_tp,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- LAC deferred taxes ---
output_rows.append({
    "module_name": "LAC deferred taxes",
    "sub_module_name": "total",
    "level": "adjustment",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": 0.0,
    "net_capital_charge": lac_dt,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

# --- Final SCR ---
output_rows.append({
    "module_name": "SCR",
    "sub_module_name": "total",
    "level": "scr",
    "gross_capital_charge": 0.0,
    "intra_module_diversification": 0.0,
    "net_capital_charge": final_scr,
    "is_diversification": False,
    "parent_module": None,
    "reporting_period": reporting_period,
})

print(f"Output rows built: {len(output_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write silver_scr_modules table

# COMMAND ----------

output_schema = StructType([
    StructField("module_name", StringType(), False),
    StructField("sub_module_name", StringType(), False),
    StructField("level", StringType(), False),
    StructField("gross_capital_charge", DoubleType(), True),
    StructField("intra_module_diversification", DoubleType(), True),
    StructField("net_capital_charge", DoubleType(), True),
    StructField("is_diversification", BooleanType(), False),
    StructField("parent_module", StringType(), True),
    StructField("reporting_period", StringType(), False),
])

df_output = spark.createDataFrame([Row(**r) for r in output_rows], schema=output_schema)

target_table = f"{catalog}.{schema}.silver_scr_modules"
df_output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

row_count_out = spark.table(target_table).count()
print(f"silver_scr_modules written: {row_count_out} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply column and table comments

# COMMAND ----------

table_comment = (
    "SCR Standard Formula module-level waterfall for S.25.01 reporting. "
    "Contains sub-module charges, intra-module diversification, BSCR aggregation "
    "via EIOPA inter-module correlation matrix (square-root formula), operational risk, "
    "LAC adjustments, and final SCR. Grain: one row per module/sub-module component in "
    "the SCR waterfall. Source: bronze_risk_factors + bronze_scr_parameters."
)

column_comments = {
    "module_name": "Display name of the SCR risk module (e.g. Market risk, BSCR, SCR). Used as the primary label in S.25.01 reporting.",
    "sub_module_name": "Sub-module or component identifier within the module (e.g. interest_rate, equity, TOTAL, diversification, total).",
    "level": "Hierarchy level in the SCR waterfall: sub_module (individual risk), module (aggregated risk module), intermediate (BSCR components), bscr (BSCR total), adjustment (op risk, LAC), scr (final SCR).",
    "gross_capital_charge": "Gross (pre-diversification within module) capital charge in EUR. Zero for diversification rows and SCR-level aggregates.",
    "intra_module_diversification": "Intra-module diversification benefit in EUR. Negative value represents capital relief from combining sub-modules. Zero for top-level aggregates.",
    "net_capital_charge": "Net capital charge in EUR after applying intra-module diversification. For BSCR/SCR rows this is the aggregated total. For LAC rows this is negative.",
    "is_diversification": "Boolean flag: true for rows representing diversification benefits (intra-module or inter-module), false for actual risk charges.",
    "parent_module": "Parent module display name for sub-module rows. Null for module totals, BSCR, adjustments, and final SCR.",
    "reporting_period": "Reporting period in YYYY-QN format (e.g. 2025-Q4).",
}

safe_table_comment = table_comment.replace("'", "''")
spark.sql(f"COMMENT ON TABLE {target_table} IS '{safe_table_comment}'")

existing_cols = {f.name for f in spark.table(target_table).schema.fields}
for col_name, col_comment in column_comments.items():
    if col_name in existing_cols:
        safe_comment = col_comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'")

print("Table and column comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

dq_checks = []

# Check 1: BSCR > 0
check_passed = bscr > 0
dq_checks.append(("bscr_positive", "> 0", str(round(bscr, 2)), check_passed))
lineage.log_data_quality(
    step_name="04_silver_scr", table_name=target_table,
    check_name="bscr_positive", check_category="business_rule",
    check_expression="BSCR > 0", expected_value="> 0",
    actual_value=round(bscr, 2), passed=check_passed, severity="critical"
)

# Check 2: Final SCR > 0
check_passed = final_scr > 0
dq_checks.append(("scr_positive", "> 0", str(round(final_scr, 2)), check_passed))
lineage.log_data_quality(
    step_name="04_silver_scr", table_name=target_table,
    check_name="scr_positive", check_category="business_rule",
    check_expression="Final SCR > 0", expected_value="> 0",
    actual_value=round(final_scr, 2), passed=check_passed, severity="critical"
)

# Check 3: SCR < BSCR (diversification and LAC reduce it)
check_passed = final_scr < bscr
dq_checks.append(("scr_less_than_bscr", "SCR < BSCR", f"{round(final_scr, 2)} < {round(bscr, 2)}", check_passed))
lineage.log_data_quality(
    step_name="04_silver_scr", table_name=target_table,
    check_name="scr_less_than_bscr", check_category="business_rule",
    check_expression="Final SCR < BSCR", expected_value="true",
    actual_value=final_scr < bscr, passed=check_passed, severity="critical"
)

# Check 4: Sum of module net charges > BSCR (diversification reduces it)
check_passed = sum_of_modules > bscr
dq_checks.append(("diversification_reduces", "sum_modules > BSCR", f"{round(sum_of_modules, 2)} > {round(bscr, 2)}", check_passed))
lineage.log_data_quality(
    step_name="04_silver_scr", table_name=target_table,
    check_name="diversification_reduces_bscr", check_category="business_rule",
    check_expression="Sum of module SCRs > BSCR", expected_value="true",
    actual_value=sum_of_modules > bscr, passed=check_passed, severity="warning"
)

# Check 5: All modules present
expected_modules = {"Market risk", "Counterparty default risk", "Non-life underwriting risk",
                    "Health underwriting risk", "BSCR", "SCR", "Operational risk",
                    "LAC technical provisions", "LAC deferred taxes"}
actual_modules = {r["module_name"] for r in spark.table(target_table).select("module_name").distinct().collect()}
missing = expected_modules - actual_modules
check_passed = len(missing) == 0
dq_checks.append(("all_modules_present", "no missing", str(missing) if missing else "all present", check_passed))
lineage.log_data_quality(
    step_name="04_silver_scr", table_name=target_table,
    check_name="all_modules_present", check_category="completeness",
    check_expression="All expected modules in output", expected_value="0 missing",
    actual_value=f"{len(missing)} missing: {missing}" if missing else "0 missing",
    passed=check_passed, severity="critical"
)

all_passed = all(c[3] for c in dq_checks)
print(f"\nData quality: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log lineage

# COMMAND ----------

source_tables = [
    f"{catalog}.{schema}.bronze_risk_factors",
    f"{catalog}.{schema}.bronze_scr_parameters",
]

lineage.log_lineage(
    step_name="04_silver_scr",
    step_sequence=4,
    source_tables=source_tables,
    target_table=target_table,
    transformation_type="aggregation",
    transformation_desc=(
        "Read bronze risk factors and SCR parameters. Aggregate sub-module charges by module. "
        "Apply EIOPA inter-module correlation matrix (5x5 square-root formula) to compute BSCR. "
        "Add operational risk, subtract LAC TP and LAC DT to derive final SCR. "
        "Output structured waterfall table for S.25.01 reporting."
    ),
    row_count_in=risk_factor_count,
    row_count_out=row_count_out,
    columns_in=["risk_module", "risk_sub_module", "gross_capital_charge_eur",
                 "diversification_within_module_eur", "net_capital_charge_eur",
                 "parameter_name", "parameter_value", "parameter_category"],
    columns_out=["module_name", "sub_module_name", "level", "gross_capital_charge",
                  "intra_module_diversification", "net_capital_charge",
                  "is_diversification", "parent_module", "reporting_period"],
    parameters={"catalog": catalog, "schema": schema, "reporting_date": reporting_date},
    status="success",
    data_quality_checks=[{"name": c[0], "passed": c[3]} for c in dq_checks],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  SCR STANDARD FORMULA CALCULATION COMPLETE")
print("=" * 70)
print(f"\n{'Component':<40} {'EUR M':>12}")
print("-" * 54)

# Module-level summary
for mod_key in ["market", "counterparty_default", "non_life", "health"]:
    display = MODULE_DISPLAY_NAMES[mod_key]
    net_val = module_net_charges.get(mod_key, 0.0)
    print(f"  {display:<38} {net_val / 1e6:>12.1f}")

print(f"  {'Intangible risk':<38} {intangible_scr / 1e6:>12.1f}")
print("-" * 54)
print(f"  {'Sum of modules (undiversified)':<38} {sum_of_modules / 1e6:>12.1f}")
print(f"  {'Inter-module diversification':<38} {inter_module_diversification / 1e6:>12.1f}")
print(f"  {'BSCR':<38} {bscr / 1e6:>12.1f}")
print("-" * 54)
print(f"  {'Operational risk':<38} {operational_risk / 1e6:>12.1f}")
print(f"  {'LAC technical provisions':<38} {lac_tp / 1e6:>12.1f}")
print(f"  {'LAC deferred taxes':<38} {lac_dt / 1e6:>12.1f}")
print("=" * 54)
print(f"  {'FINAL SCR':<38} {final_scr / 1e6:>12.1f}")
print("=" * 54)
print(f"\n  Diversification benefit: EUR {inter_module_diversification / 1e6:.1f}M ({inter_module_diversification / sum_of_modules * 100:.1f}% of undiversified)")
print(f"  Target table: {target_table} ({row_count_out} rows)")
