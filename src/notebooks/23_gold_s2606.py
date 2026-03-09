# Databricks notebook source
# MAGIC %md
# MAGIC # 23_gold_s2606 — S.26.06 SCR Non-Life Underwriting Risk (Standard Formula)
# MAGIC
# MAGIC This notebook computes the **Solvency Capital Requirement for Non-Life Underwriting Risk**
# MAGIC under the Solvency II Standard Formula and formats the result into the EIOPA S.26.06 QRT template.
# MAGIC
# MAGIC The Non-Life UW Risk module (SCR_nl) consists of three sub-modules:
# MAGIC 1. **Premium & Reserve Risk** (SCR_nl_pr) — risk of adverse premium and reserve development
# MAGIC 2. **Catastrophe Risk** (SCR_nl_cat) — natural, man-made, and other catastrophe scenarios
# MAGIC 3. **Lapse Risk** (SCR_nl_lapse) — mass lapse of non-life policies
# MAGIC
# MAGIC **Source tables:** `bronze_volume_measures`, `silver_best_estimate` (if available)
# MAGIC **Target table:** `gold_s2606`
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage` and `audit_data_quality_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog")
dbutils.widgets.text("schema_name", "solvency2demo", "Schema")
dbutils.widgets.text("reporting_date", "2025-12-31", "Reporting Date")
dbutils.widgets.text("entity_lei", "5493001KJTIIGC8Y1R12", "Entity LEI")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")
entity_lei = dbutils.widgets.get("entity_lei")

from datetime import datetime
_rd = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period = f"{_rd.year}-Q{(_rd.month - 1) // 3 + 1}"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Reporting period: {reporting_period}")
print(f"Entity LEI:       {entity_lei}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("23_gold_s2606")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Volume Measures
# MAGIC
# MAGIC The `bronze_volume_measures` table contains per-LoB premium and reserve volumes:
# MAGIC - **earned_premium_net**: net earned premium for the current period
# MAGIC - **written_premium_net_next_year**: expected net written premium for the following year
# MAGIC - **best_estimate_claims_provision**: best estimate of outstanding claims reserves
# MAGIC - **best_estimate_premium_provision**: best estimate of unearned premium reserves
# MAGIC
# MAGIC If `silver_best_estimate` exists, we use it to override the claims and premium provisions
# MAGIC with validated actuarial figures.

# COMMAND ----------

import math
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)

source_table = f"{catalog}.{schema}.bronze_volume_measures"
df_volume = spark.table(source_table)
row_count_in = df_volume.count()
print(f"bronze_volume_measures: {row_count_in} rows loaded")

# Attempt to enrich from silver_best_estimate if it exists
silver_be_table = f"{catalog}.{schema}.silver_best_estimate"
try:
    df_be = spark.table(silver_be_table)
    print(f"silver_best_estimate found — will use for provision overrides")
    # Join and override provisions where silver values are available
    df_volume = df_volume.alias("v").join(
        df_be.alias("be"),
        df_volume["lob_code"] == df_be["lob_code"],
        "left"
    ).selectExpr(
        "v.lob_code",
        "v.lob_name",
        "v.earned_premium_net",
        "v.written_premium_net_next_year",
        "COALESCE(be.best_estimate_claims_prov, v.best_estimate_claims_provision) AS best_estimate_claims_provision",
        "COALESCE(be.best_estimate_premium_prov, v.best_estimate_premium_provision) AS best_estimate_premium_provision",
    )
    source_tables_list = [source_table, silver_be_table]
except Exception:
    print(f"silver_best_estimate not found — using bronze_volume_measures only")
    source_tables_list = [source_table]

df_volume.cache()
df_volume.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: EIOPA Standard Formula Parameters
# MAGIC
# MAGIC The Standard Formula prescribes **fixed sigma factors** for each Solvency II Non-Life LoB.
# MAGIC These represent the volatility of premium and reserve risk respectively, calibrated by EIOPA
# MAGIC from industry-wide data.
# MAGIC
# MAGIC | LoB | Description | sigma_prem | sigma_res |
# MAGIC |-----|-------------|-----------|----------|
# MAGIC | 1 | Motor vehicle liability | 10.0% | 9.5% |
# MAGIC | 2 | Motor other | 7.0% | 10.0% |
# MAGIC | 3 | Marine aviation transport | 17.0% | 14.0% |
# MAGIC | 4 | Fire and other property | 10.0% | 11.0% |
# MAGIC | 5 | General liability | 14.0% | 11.0% |
# MAGIC | 6 | Credit and suretyship | 12.0% | 19.0% |
# MAGIC | 7 | Legal expenses | 6.5% | 9.0% |
# MAGIC | 8 | Assistance | 5.0% | 11.0% |
# MAGIC
# MAGIC The **correlation factor** between any two different LoBs for premium & reserve risk
# MAGIC is set to **0.5** (a reasonable simplification of the full EIOPA correlation matrix).

# COMMAND ----------

# EIOPA-prescribed sigma factors (Delegated Regulation Annex II)
SIGMA_FACTORS = {
    1: {"sigma_prem": 0.100, "sigma_res": 0.095, "name": "Motor vehicle liability"},
    2: {"sigma_prem": 0.070, "sigma_res": 0.100, "name": "Motor other"},
    3: {"sigma_prem": 0.170, "sigma_res": 0.140, "name": "Marine aviation transport"},
    4: {"sigma_prem": 0.100, "sigma_res": 0.110, "name": "Fire and other property"},
    5: {"sigma_prem": 0.140, "sigma_res": 0.110, "name": "General liability"},
    6: {"sigma_prem": 0.120, "sigma_res": 0.190, "name": "Credit and suretyship"},
    7: {"sigma_prem": 0.065, "sigma_res": 0.090, "name": "Legal expenses"},
    8: {"sigma_prem": 0.050, "sigma_res": 0.110, "name": "Assistance"},
}

# Off-diagonal correlation between LoBs (simplified)
CORR_OFF_DIAG = 0.5

print("EIOPA sigma factors loaded for 8 LoBs")
print(f"Off-diagonal LoB correlation: {CORR_OFF_DIAG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Per-LoB Volume Measures and Combined Sigma
# MAGIC
# MAGIC For each LoB *s*, the Standard Formula defines:
# MAGIC
# MAGIC **Volume measure for premium risk:**
# MAGIC > V(prem,s) = max(earned_premium_net, written_premium_net_next_year) + best_estimate_premium_provision
# MAGIC
# MAGIC **Volume measure for reserve risk:**
# MAGIC > V(res,s) = best_estimate_claims_provision
# MAGIC
# MAGIC **Combined volume:**
# MAGIC > V(s) = V(prem,s) + V(res,s)
# MAGIC
# MAGIC **Combined standard deviation** (the key formula from Art. 117 of the Delegated Regulation):
# MAGIC > sigma(s) = sqrt( sigma_prem(s)^2 * V(prem,s)^2
# MAGIC >                 + sigma_prem(s) * sigma_res(s) * V(prem,s) * V(res,s)
# MAGIC >                 + sigma_res(s)^2 * V(res,s)^2 ) / V(s)
# MAGIC
# MAGIC This formula captures the within-LoB correlation between premium and reserve risk.

# COMMAND ----------

# Collect volume data to Python for the calculation
volume_rows = df_volume.collect()

# Build per-LoB calculation results
lob_results = []

for row in volume_rows:
    lob_code = int(row["lob_code"])
    lob_name = row["lob_name"]

    # Skip LoBs not in the Standard Formula sigma table
    if lob_code not in SIGMA_FACTORS:
        print(f"  WARNING: LoB {lob_code} ({lob_name}) not in sigma factor table — skipping")
        continue

    sf = SIGMA_FACTORS[lob_code]
    sigma_prem = sf["sigma_prem"]
    sigma_res = sf["sigma_res"]

    earned_prem = float(row["earned_premium_net"])
    written_prem_ny = float(row["written_premium_net_next_year"])
    be_claims = float(row["best_estimate_claims_provision"])
    be_premium = float(row["best_estimate_premium_provision"])

    # Volume measures
    v_prem = max(earned_prem, written_prem_ny) + be_premium
    v_res = be_claims
    v_combined = v_prem + v_res

    # Combined sigma (within-LoB aggregation of premium and reserve risk)
    if v_combined > 0:
        variance_within = (
            (sigma_prem ** 2) * (v_prem ** 2)
            + sigma_prem * sigma_res * v_prem * v_res
            + (sigma_res ** 2) * (v_res ** 2)
        )
        sigma_combined = math.sqrt(variance_within) / v_combined
    else:
        sigma_combined = 0.0

    lob_results.append({
        "lob_code": lob_code,
        "lob_name": lob_name,
        "v_prem": v_prem,
        "v_res": v_res,
        "v_combined": v_combined,
        "sigma_prem": sigma_prem,
        "sigma_res": sigma_res,
        "sigma_combined": sigma_combined,
    })

    print(f"  LoB {lob_code} ({lob_name}):")
    print(f"    V(prem) = {v_prem:>15,.2f}  |  V(res) = {v_res:>15,.2f}  |  V = {v_combined:>15,.2f}")
    print(f"    sigma_prem = {sigma_prem:.1%}  |  sigma_res = {sigma_res:.1%}  |  sigma = {sigma_combined:.2%}")

print(f"\n  {len(lob_results)} LoBs included in calculation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Aggregate Across LoBs — Premium & Reserve Risk SCR
# MAGIC
# MAGIC The aggregation uses a **correlation matrix** across LoBs. For any two LoBs *s* and *t*:
# MAGIC - CorrS(s,s) = 1.0 (diagonal)
# MAGIC - CorrS(s,t) = 0.5 (off-diagonal, simplified)
# MAGIC
# MAGIC **Overall volume:**
# MAGIC > V_total = sum of V(s) across all LoBs
# MAGIC
# MAGIC **Overall standard deviation** (cross-LoB aggregation via double summation):
# MAGIC > sigma_total = (1 / V_total) * sqrt( sum_s sum_t CorrS(s,t) * sigma(s) * V(s) * sigma(t) * V(t) )
# MAGIC
# MAGIC **SCR for Premium & Reserve Risk** (3-sigma for 99.5% VaR under log-normal assumption):
# MAGIC > SCR_nl_pr = 3 * sigma_total * V_total

# COMMAND ----------

# Overall volume
v_total = sum(r["v_combined"] for r in lob_results)

# Double summation for cross-LoB aggregation
double_sum = 0.0
for r_s in lob_results:
    for r_t in lob_results:
        if r_s["lob_code"] == r_t["lob_code"]:
            corr = 1.0
        else:
            corr = CORR_OFF_DIAG
        double_sum += corr * r_s["sigma_combined"] * r_s["v_combined"] * r_t["sigma_combined"] * r_t["v_combined"]

# Overall sigma
sigma_total = math.sqrt(double_sum) / v_total if v_total > 0 else 0.0

# SCR for premium & reserve risk (3-sigma)
scr_nl_pr = 3.0 * sigma_total * v_total

# Diversification effect: difference between aggregated SCR and sum of standalone SCRs
sum_standalone_scr = sum(3.0 * r["sigma_combined"] * r["v_combined"] for r in lob_results)
diversification_pr = scr_nl_pr - sum_standalone_scr

print(f"  V_total       = {v_total:>18,.2f} EUR")
print(f"  sigma_total   = {sigma_total:>18.4%}")
print(f"  SCR_nl_pr     = {scr_nl_pr:>18,.2f} EUR  (3 * {sigma_total:.4%} * V_total)")
print(f"")
print(f"  Sum standalone= {sum_standalone_scr:>18,.2f} EUR  (sum of 3*sigma(s)*V(s))")
print(f"  Diversification= {diversification_pr:>17,.2f} EUR  (negative = risk reduction)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Catastrophe Risk (Simplified)
# MAGIC
# MAGIC The Non-Life catastrophe risk sub-module covers three perils:
# MAGIC - **Natural catastrophe** (windstorm, earthquake, flood): 2% of total net earned premium
# MAGIC - **Man-made catastrophe** (fire, motor, marine, aviation, liability): 1% of total net earned premium
# MAGIC - **Other catastrophe** (mass accident, pandemic): 0.5% of total net earned premium
# MAGIC
# MAGIC These are aggregated assuming independence:
# MAGIC > SCR_nl_cat = sqrt(nat^2 + man^2 + other^2)

# COMMAND ----------

total_earned_premium = sum(float(row["earned_premium_net"]) for row in volume_rows)

cat_natural = 0.02 * total_earned_premium
cat_manmade = 0.01 * total_earned_premium
cat_other = 0.005 * total_earned_premium
scr_nl_cat = math.sqrt(cat_natural**2 + cat_manmade**2 + cat_other**2)

print(f"  Total net earned premium = {total_earned_premium:>18,.2f} EUR")
print(f"")
print(f"  Natural cat risk         = {cat_natural:>18,.2f} EUR  (2.0%)")
print(f"  Man-made cat risk        = {cat_manmade:>18,.2f} EUR  (1.0%)")
print(f"  Other cat risk           = {cat_other:>18,.2f} EUR  (0.5%)")
print(f"  SCR_nl_cat               = {scr_nl_cat:>18,.2f} EUR  (sqrt of sum of squares)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Lapse Risk (Simplified)
# MAGIC
# MAGIC The Non-Life lapse risk captures the impact of a **mass lapse** scenario where
# MAGIC policyholders discontinue contracts, causing loss of expected future profit.
# MAGIC
# MAGIC Under the simplified approach:
# MAGIC > SCR_nl_lapse = 40% of expected profit from future premiums
# MAGIC
# MAGIC Approximated as 2% of total net earned premium.

# COMMAND ----------

scr_nl_lapse = 0.02 * total_earned_premium

print(f"  SCR_nl_lapse = {scr_nl_lapse:>18,.2f} EUR  (2% of total net earned premium)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Aggregate Non-Life UW Risk SCR
# MAGIC
# MAGIC The three sub-modules are aggregated using the prescribed correlation structure:
# MAGIC - Premium & Reserve risk and Catastrophe risk have correlation **0.25**
# MAGIC - Lapse risk is **additive** (independent of the other two)
# MAGIC
# MAGIC > SCR_nl = sqrt(SCR_nl_pr^2 + 2 * 0.25 * SCR_nl_pr * SCR_nl_cat + SCR_nl_cat^2) + SCR_nl_lapse
# MAGIC
# MAGIC The diversification between sub-modules is the difference between the total and the
# MAGIC simple sum of components.

# COMMAND ----------

# Aggregation of premium/reserve and catastrophe risk (correlation = 0.25)
corr_pr_cat = 0.25
scr_pr_cat_agg = math.sqrt(
    scr_nl_pr**2
    + 2 * corr_pr_cat * scr_nl_pr * scr_nl_cat
    + scr_nl_cat**2
)

# Total SCR Non-Life UW Risk (lapse is additive)
scr_nl_total = scr_pr_cat_agg + scr_nl_lapse

# Diversification between sub-modules
sum_sub_modules = scr_nl_pr + scr_nl_cat + scr_nl_lapse
diversification_between = scr_nl_total - sum_sub_modules

print(f"  SCR_nl_pr        = {scr_nl_pr:>18,.2f} EUR")
print(f"  SCR_nl_cat       = {scr_nl_cat:>18,.2f} EUR")
print(f"  SCR_nl_lapse     = {scr_nl_lapse:>18,.2f} EUR")
print(f"  Sum of sub-modules = {sum_sub_modules:>16,.2f} EUR")
print(f"")
print(f"  SCR_nl (aggregated) = {scr_nl_total:>15,.2f} EUR")
print(f"  Diversification     = {diversification_between:>15,.2f} EUR")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Build S.26.06 Template Rows
# MAGIC
# MAGIC The output follows the EIOPA S.26.06 template in **long format**, with one row per
# MAGIC template line item. Each row contains:
# MAGIC - `template_row_id` — EIOPA row reference (R0010, R0110, etc.)
# MAGIC - `template_row_label` — human-readable description
# MAGIC - `c0010_standard_formula_value` — the calculated Standard Formula value
# MAGIC - `c0020_gross_value` — gross of reinsurance (same as c0010 in this simplified model)
# MAGIC - `c0030_usp_specific_params` — NULL (we use standard, not undertaking-specific parameters)

# COMMAND ----------

template_rows = []

def add_row(row_id, label, value):
    """Helper to append a template row."""
    template_rows.append({
        "reporting_date": reporting_date,
        "entity_lei": entity_lei,
        "template_row_id": row_id,
        "template_row_label": label,
        "c0010_standard_formula_value": round(value, 2),
        "c0020_gross_value": round(value, 2),
        "c0030_usp_specific_params": None,
    })

# --- Overall summary rows ---
add_row("R0010", "Premium & reserve risk (net)", scr_nl_pr)
add_row("R0020", "Overall volume measure V", v_total)
add_row("R0030", "Overall combined sigma", sigma_total)

# --- Per-LoB detail rows (R0110-R0185) ---
for r in sorted(lob_results, key=lambda x: x["lob_code"]):
    s = r["lob_code"]
    name = r["lob_name"]
    prefix = f"R0{s + 10}"  # R0110, R0120, ..., R0180

    add_row(f"{prefix}0", f"LoB {s} ({name}) — Volume measure premium V(prem,{s})", r["v_prem"])
    add_row(f"{prefix}1", f"LoB {s} ({name}) — Volume measure reserve V(res,{s})", r["v_res"])
    add_row(f"{prefix}2", f"LoB {s} ({name}) — Combined volume V({s})", r["v_combined"])
    add_row(f"{prefix}3", f"LoB {s} ({name}) — Sigma premium (prem,{s})", r["sigma_prem"])
    add_row(f"{prefix}4", f"LoB {s} ({name}) — Sigma reserve (res,{s})", r["sigma_res"])
    add_row(f"{prefix}5", f"LoB {s} ({name}) — Combined sigma ({s})", r["sigma_combined"])

# --- Diversification within premium & reserve risk ---
add_row("R0900", "Diversification within premium & reserve risk", diversification_pr)

# --- Catastrophe risk ---
add_row("R1000", "Non-life catastrophe risk — total", scr_nl_cat)
add_row("R1010", "Natural catastrophe risk", cat_natural)
add_row("R1020", "Man-made catastrophe risk", cat_manmade)
add_row("R1030", "Other catastrophe risk", cat_other)

# --- Lapse risk ---
add_row("R1100", "Non-life lapse risk", scr_nl_lapse)

# --- Final aggregation ---
add_row("R1200", "Diversification between NL UW sub-modules", diversification_between)
add_row("R1300", "Total SCR Non-Life Underwriting Risk", scr_nl_total)

print(f"  {len(template_rows)} template rows prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Write gold_s2606 Table

# COMMAND ----------

target_table = f"{catalog}.{schema}.gold_s2606"

# Create DataFrame from template rows
from pyspark.sql.types import DateType

schema_s2606 = StructType([
    StructField("reporting_date", StringType(), False),
    StructField("entity_lei", StringType(), False),
    StructField("template_row_id", StringType(), False),
    StructField("template_row_label", StringType(), False),
    StructField("c0010_standard_formula_value", DoubleType(), True),
    StructField("c0020_gross_value", DoubleType(), True),
    StructField("c0030_usp_specific_params", DoubleType(), True),
])

df_output = spark.createDataFrame(template_rows, schema=schema_s2606)

# Cast reporting_date string to DATE type
df_output = df_output.selectExpr(
    "CAST(reporting_date AS DATE) AS reporting_date",
    "entity_lei",
    "template_row_id",
    "template_row_label",
    "c0010_standard_formula_value",
    "c0020_gross_value",
    "c0030_usp_specific_params",
)

# Write as Delta table
(
    df_output
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

row_count_out = spark.table(target_table).count()
print(f"  gold_s2606 written: {row_count_out} rows to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Apply Table and Column Comments

# COMMAND ----------

table_comment = (
    "EIOPA S.26.06 SCR Non-Life Underwriting Risk - Standard Formula QRT template. "
    "Long format with one row per template line item. "
    "Covers premium & reserve risk (per-LoB and aggregated), catastrophe risk, lapse risk, "
    "and the total SCR Non-Life UW Risk with diversification effects. "
    "Source: bronze_volume_measures, silver_best_estimate (if available)."
)

column_comments = {
    "reporting_date": "Reporting reference date for the QRT submission.",
    "entity_lei": "Legal Entity Identifier (LEI) of the reporting undertaking.",
    "template_row_id": "EIOPA S.26.06 template row reference code.",
    "template_row_label": "Human-readable label describing the template row.",
    "c0010_standard_formula_value": "C0010 - Standard Formula value in EUR (or as a ratio for sigma rows).",
    "c0020_gross_value": "C0020 - Gross of reinsurance value (equals C0010 in this simplified model).",
    "c0030_usp_specific_params": "C0030 - Undertaking-specific parameters value. NULL when standard parameters are used.",
}

safe_table_comment = table_comment.replace("'", "''")
spark.sql(f"COMMENT ON TABLE {target_table} IS '{safe_table_comment}'")

existing_cols = {f.name for f in spark.table(target_table).schema.fields}
for col_name, col_comment in column_comments.items():
    if col_name in existing_cols:
        safe_comment = col_comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'")

print("  Table and column comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Data Quality Checks
# MAGIC
# MAGIC Four checks validate the calculation integrity:
# MAGIC 1. All 8 LoBs present in the per-LoB detail rows
# MAGIC 2. SCR_nl_pr > 0 (premium & reserve risk must be positive)
# MAGIC 3. Diversification effect is negative (diversification must reduce risk)
# MAGIC 4. Total SCR_nl < sum of components (proves cross-module diversification works)

# COMMAND ----------

dq_checks = []

# Check 1: All 8 LoBs present in calculation
lob_codes_in_output = {
    r["lob_code"] for r in lob_results
}
expected_lobs = {1, 2, 3, 4, 5, 6, 7, 8}
missing_lobs = expected_lobs - lob_codes_in_output
check1_passed = len(missing_lobs) == 0
dq_checks.append(("all_8_lobs_present", check1_passed))
lineage.log_data_quality(
    step_name="23_gold_s2606", table_name=target_table,
    check_name="all_8_lobs_present", check_category="completeness",
    check_expression="All 8 SII Non-Life LoBs present in per-LoB detail",
    expected_value="0 missing",
    actual_value=f"{len(missing_lobs)} missing: {missing_lobs}" if missing_lobs else "0 missing",
    passed=check1_passed, severity="critical"
)

# Check 2: SCR_nl_pr > 0
check2_passed = scr_nl_pr > 0
dq_checks.append(("scr_nl_pr_positive", check2_passed))
lineage.log_data_quality(
    step_name="23_gold_s2606", table_name=target_table,
    check_name="scr_nl_pr_positive", check_category="business_rule",
    check_expression="SCR_nl_pr > 0",
    expected_value="> 0",
    actual_value=f"{scr_nl_pr:,.2f}",
    passed=check2_passed, severity="critical"
)

# Check 3: Diversification effect is negative (risk reduction)
check3_passed = diversification_pr < 0
dq_checks.append(("diversification_negative", check3_passed))
lineage.log_data_quality(
    step_name="23_gold_s2606", table_name=target_table,
    check_name="diversification_within_pr_negative", check_category="business_rule",
    check_expression="Diversification within premium & reserve risk < 0",
    expected_value="< 0",
    actual_value=f"{diversification_pr:,.2f}",
    passed=check3_passed, severity="critical"
)

# Check 4: Total SCR_nl < sum of components (cross-module diversification)
check4_passed = scr_nl_total < sum_sub_modules
dq_checks.append(("total_less_than_sum", check4_passed))
lineage.log_data_quality(
    step_name="23_gold_s2606", table_name=target_table,
    check_name="total_scr_lt_sum_components", check_category="business_rule",
    check_expression="Total SCR_nl < sum(SCR_nl_pr + SCR_nl_cat + SCR_nl_lapse)",
    expected_value=f"< {sum_sub_modules:,.2f}",
    actual_value=f"{scr_nl_total:,.2f}",
    passed=check4_passed, severity="critical"
)

all_passed = all(c[1] for c in dq_checks)
print(f"\n  Data quality: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Log Audit Lineage

# COMMAND ----------

lineage.log_lineage(
    step_name="23_gold_s2606",
    step_sequence=23,
    source_tables=source_tables_list,
    target_table=target_table,
    transformation_type="scr_calculation",
    transformation_desc=(
        "Computes S.26.06 SCR Non-Life Underwriting Risk using the Solvency II Standard Formula. "
        "Premium & reserve risk calculated per-LoB using EIOPA sigma factors, then aggregated "
        "across LoBs with 0.5 off-diagonal correlation. Catastrophe risk (natural, man-made, other) "
        "and lapse risk computed as simplified percentages of net earned premium. "
        "Sub-modules aggregated with 0.25 correlation between premium/reserve and catastrophe risk, "
        "lapse risk additive."
    ),
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    columns_in=["lob_code", "lob_name", "earned_premium_net", "written_premium_net_next_year",
                 "best_estimate_claims_provision", "best_estimate_premium_provision"],
    columns_out=["reporting_date", "entity_lei", "template_row_id", "template_row_label",
                  "c0010_standard_formula_value", "c0020_gross_value", "c0030_usp_specific_params"],
    parameters={
        "catalog": catalog, "schema": schema, "reporting_date": reporting_date,
        "entity_lei": entity_lei, "corr_off_diagonal": CORR_OFF_DIAG,
        "corr_pr_cat": corr_pr_cat,
    },
    status="success",
    data_quality_checks=[{"name": c[0], "passed": c[1]} for c in dq_checks],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S.26.06 — Non-Life Underwriting Risk Summary

# COMMAND ----------

print("=" * 90)
print("  S.26.06 — SCR NON-LIFE UNDERWRITING RISK (STANDARD FORMULA)")
print("=" * 90)
print(f"  Reporting date:  {reporting_date}")
print(f"  Entity LEI:      {entity_lei}")
print("=" * 90)

# --- Premium & Reserve Risk ---
print(f"\n  {'Row':<10} {'Description':<55} {'Value':>18}")
print("  " + "-" * 83)
print(f"  {'R0010':<10} {'Premium & reserve risk (net)':<55} {scr_nl_pr / 1e6:>17.2f}M")
print(f"  {'R0020':<10} {'Overall volume measure V':<55} {v_total / 1e6:>17.2f}M")
print(f"  {'R0030':<10} {'Overall combined sigma':<55} {sigma_total:>17.4%}")
print()

# Per-LoB detail
print("  Per-LoB Detail:")
print("  " + "-" * 83)
print(f"  {'LoB':<6} {'Name':<28} {'V(prem)':>14} {'V(res)':>14} {'V(total)':>14} {'sigma':>10}")
print("  " + "-" * 83)
for r in sorted(lob_results, key=lambda x: x["lob_code"]):
    print(
        f"  {r['lob_code']:<6} {r['lob_name']:<28} "
        f"{r['v_prem'] / 1e6:>13.1f}M {r['v_res'] / 1e6:>13.1f}M "
        f"{r['v_combined'] / 1e6:>13.1f}M {r['sigma_combined']:>9.2%}"
    )
print("  " + "-" * 83)
print(f"  {'Total':<6} {'':<28} {sum(r['v_prem'] for r in lob_results) / 1e6:>13.1f}M "
      f"{sum(r['v_res'] for r in lob_results) / 1e6:>13.1f}M "
      f"{v_total / 1e6:>13.1f}M {sigma_total:>9.2%}")

print(f"\n  {'R0900':<10} {'Diversification within P&R risk':<55} {diversification_pr / 1e6:>17.2f}M")

# --- Catastrophe Risk ---
print(f"\n  {'R1000':<10} {'Non-life catastrophe risk — total':<55} {scr_nl_cat / 1e6:>17.2f}M")
print(f"  {'R1010':<10} {'  Natural catastrophe':<55} {cat_natural / 1e6:>17.2f}M")
print(f"  {'R1020':<10} {'  Man-made catastrophe':<55} {cat_manmade / 1e6:>17.2f}M")
print(f"  {'R1030':<10} {'  Other catastrophe':<55} {cat_other / 1e6:>17.2f}M")

# --- Lapse Risk ---
print(f"\n  {'R1100':<10} {'Non-life lapse risk':<55} {scr_nl_lapse / 1e6:>17.2f}M")

# --- Total ---
print()
print("  " + "=" * 83)
print(f"  {'R1200':<10} {'Diversification between NL UW sub-modules':<55} {diversification_between / 1e6:>17.2f}M")
print(f"  {'R1300':<10} {'TOTAL SCR NON-LIFE UNDERWRITING RISK':<55} {scr_nl_total / 1e6:>17.2f}M")
print("  " + "=" * 83)

print(f"\n  Target table: {target_table} ({row_count_out} rows)")
print(f"  Data quality: {'ALL PASSED' if all_passed else 'CHECKS FAILED — review above'}")
