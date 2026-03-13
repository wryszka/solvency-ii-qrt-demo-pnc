# Databricks notebook source
# MAGIC %md
# MAGIC # 07_gold_s2501 - Format SCR standard formula into EIOPA S.25.01 QRT template
# MAGIC
# MAGIC Reads `silver_scr_modules` and produces `gold_qrt_s2501` — the EIOPA S.25.01
# MAGIC "Solvency Capital Requirement — Standard Formula" QRT template in long format.
# MAGIC
# MAGIC **Source table:** `silver_scr_modules`
# MAGIC **Target table:** `gold_qrt_s2501`

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_classic_aws_us_catalog")
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_date", "2025-12-31")
dbutils.widgets.text("entity_lei", "5493001KJTIIGC8Y1R12")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")
entity_lei = dbutils.widgets.get("entity_lei")

reporting_year = int(reporting_date[:4])
reporting_period = f"{reporting_year}-Q4"

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
lineage.start_step("07_gold_s2501")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read silver_scr_modules and verify source data

# COMMAND ----------

source_table = f"{catalog}.{schema}.silver_scr_modules"
df_source = spark.table(source_table)
row_count_in = df_source.count()
print(f"silver_scr_modules rows loaded: {row_count_in}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build S.25.01 template using UNION ALL queries
# MAGIC
# MAGIC Each row maps a silver_scr_modules record to an EIOPA template row reference.
# MAGIC For module-level rows (R0010-R0050), gross_capital_charge maps to C0110 and
# MAGIC net_capital_charge maps to C0100. For diversification, BSCR, adjustments, and
# MAGIC SCR totals, both C0110 and C0100 use net_capital_charge.

# COMMAND ----------

target_table = f"{catalog}.{schema}.gold_qrt_s2501"

spark.sql(f"""
    CREATE OR REPLACE TABLE {target_table} AS

    -- R0010: Market risk
    SELECT
        DATE('{reporting_date}') AS reporting_reference_date,
        '{entity_lei}' AS reporting_entity_lei,
        'R0010' AS template_row_id,
        'Market risk' AS template_row_label,
        gross_capital_charge AS c0110_gross_scr,
        'None' AS c0080_ust_simplifications,
        net_capital_charge AS c0100_net_scr
    FROM {source_table}
    WHERE level = 'module' AND module_name = 'Market risk'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0020: Counterparty default risk
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0020',
        'Counterparty default risk',
        gross_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'module' AND module_name = 'Counterparty default risk'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0030: Life underwriting risk (hardcoded 0 for P&C demo)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0030',
        'Life underwriting risk',
        CAST(0.0 AS DOUBLE),
        'None',
        CAST(0.0 AS DOUBLE)

    UNION ALL

    -- R0040: Health underwriting risk
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0040',
        'Health underwriting risk',
        gross_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'module' AND module_name = 'Health underwriting risk'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0050: Non-life underwriting risk
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0050',
        'Non-life underwriting risk',
        gross_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'module' AND module_name = 'Non-life underwriting risk'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0060: Diversification (negative value)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0060',
        'Diversification',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'intermediate' AND module_name = 'Inter-module diversification'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0070: Intangible asset risk (hardcoded 0)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0070',
        'Intangible asset risk',
        CAST(0.0 AS DOUBLE),
        'None',
        CAST(0.0 AS DOUBLE)

    UNION ALL

    -- R0100: Basic Solvency Capital Requirement
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0100',
        'Basic Solvency Capital Requirement',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'bscr'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0130: Operational risk
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0130',
        'Operational risk',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'adjustment' AND module_name = 'Operational risk'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0140: Loss-absorbing capacity of technical provisions
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0140',
        'Loss-absorbing capacity of technical provisions',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'adjustment' AND module_name = 'LAC technical provisions'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0150: Loss-absorbing capacity of deferred taxes
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0150',
        'Loss-absorbing capacity of deferred taxes',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'adjustment' AND module_name = 'LAC deferred taxes'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0160: Capital requirement for Art 4 of Directive 2003/41/EC (hardcoded 0)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0160',
        'Capital requirement for business operated in accordance with Art. 4 of Directive 2003/41/EC',
        CAST(0.0 AS DOUBLE),
        'None',
        CAST(0.0 AS DOUBLE)

    UNION ALL

    -- R0200: Solvency Capital Requirement
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0200',
        'Solvency Capital Requirement',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'scr'
      AND reporting_period = '{reporting_period}'

    UNION ALL

    -- R0210: Other information - Capital add-on already set (hardcoded 0)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0210',
        'Other information on SCR - Capital add-on already set',
        CAST(0.0 AS DOUBLE),
        'None',
        CAST(0.0 AS DOUBLE)

    UNION ALL

    -- R0220: SCR including capital add-on (same as R0200, no add-on in demo)
    SELECT
        DATE('{reporting_date}'),
        '{entity_lei}',
        'R0220',
        'Solvency Capital Requirement including capital add-on',
        net_capital_charge,
        'None',
        net_capital_charge
    FROM {source_table}
    WHERE level = 'scr'
      AND reporting_period = '{reporting_period}'
""")

row_count_out = spark.table(target_table).count()
print(f"gold_qrt_s2501 written: {row_count_out} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply table and column comments

# COMMAND ----------

table_comment = (
    "EIOPA S.25.01 Solvency Capital Requirement - Standard Formula QRT template. "
    "Long format with one row per EIOPA template row reference (R0010-R0220). "
    "Columns C0110 (gross SCR) and C0100 (net SCR) follow the official template layout. "
    "Source: silver_scr_modules."
)

column_comments = {
    "reporting_reference_date": "Reporting reference date for the QRT submission (end of reporting period).",
    "reporting_entity_lei": "Legal Entity Identifier (LEI) of the reporting insurance undertaking.",
    "template_row_id": "EIOPA template row reference code (R0010-R0220) as defined in the S.25.01 LOG instructions.",
    "template_row_label": "Human-readable label for the template row, matching the EIOPA S.25.01 template description.",
    "c0110_gross_scr": "C0110 - Gross solvency capital requirement in EUR. For module-level rows this is the gross (pre-diversification) charge. For aggregates and adjustments this equals the net value.",
    "c0080_ust_simplifications": "C0080 - Indication of whether USP (undertaking-specific parameters) or simplifications are used. Set to None for this demo.",
    "c0100_net_scr": "C0100 - Net solvency capital requirement in EUR after risk mitigation and diversification within each module.",
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
# MAGIC ## Step 4: Data quality checks

# COMMAND ----------

dq_checks = []

# Check 1: All expected row IDs present
expected_row_ids = {
    "R0010", "R0020", "R0030", "R0040", "R0050", "R0060", "R0070",
    "R0100", "R0130", "R0140", "R0150", "R0160", "R0200", "R0210", "R0220"
}
actual_row_ids = {
    r["template_row_id"]
    for r in spark.sql(f"SELECT DISTINCT template_row_id FROM {target_table}").collect()
}
missing_ids = expected_row_ids - actual_row_ids
check_passed = len(missing_ids) == 0
dq_checks.append(("all_row_ids_present", check_passed))
lineage.log_data_quality(
    step_name="07_gold_s2501", table_name=target_table,
    check_name="all_row_ids_present", check_category="completeness",
    check_expression="All expected EIOPA row IDs (R0010-R0220) present",
    expected_value="0 missing",
    actual_value=f"{len(missing_ids)} missing: {missing_ids}" if missing_ids else "0 missing",
    passed=check_passed, severity="critical"
)

# Check 2: R0200 (SCR) > 0
scr_value = spark.sql(f"""
    SELECT c0100_net_scr FROM {target_table} WHERE template_row_id = 'R0200'
""").first()[0]
check_passed = scr_value is not None and scr_value > 0
dq_checks.append(("scr_positive", check_passed))
lineage.log_data_quality(
    step_name="07_gold_s2501", table_name=target_table,
    check_name="scr_r0200_positive", check_category="business_rule",
    check_expression="R0200 SCR > 0", expected_value="> 0",
    actual_value=round(scr_value, 2) if scr_value else "NULL",
    passed=check_passed, severity="critical"
)

# Check 3: R0100 (BSCR) > R0200 (SCR)
bscr_value = spark.sql(f"""
    SELECT c0100_net_scr FROM {target_table} WHERE template_row_id = 'R0100'
""").first()[0]
check_passed = bscr_value is not None and scr_value is not None and bscr_value > scr_value
dq_checks.append(("bscr_greater_than_scr", check_passed))
lineage.log_data_quality(
    step_name="07_gold_s2501", table_name=target_table,
    check_name="bscr_r0100_gt_scr_r0200", check_category="business_rule",
    check_expression="R0100 BSCR > R0200 SCR",
    expected_value=f"BSCR > SCR",
    actual_value=f"{round(bscr_value, 2)} > {round(scr_value, 2)}" if bscr_value and scr_value else "NULL",
    passed=check_passed, severity="critical"
)

# Check 4: R0060 (Diversification) < 0
div_value = spark.sql(f"""
    SELECT c0100_net_scr FROM {target_table} WHERE template_row_id = 'R0060'
""").first()[0]
check_passed = div_value is not None and div_value < 0
dq_checks.append(("diversification_negative", check_passed))
lineage.log_data_quality(
    step_name="07_gold_s2501", table_name=target_table,
    check_name="diversification_r0060_negative", check_category="business_rule",
    check_expression="R0060 Diversification < 0", expected_value="< 0",
    actual_value=round(div_value, 2) if div_value else "NULL",
    passed=check_passed, severity="critical"
)

all_passed = all(c[1] for c in dq_checks)
print(f"\nData quality: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Log lineage

# COMMAND ----------

lineage.log_lineage(
    step_name="07_gold_s2501",
    step_sequence=7,
    source_tables=[source_table],
    target_table=target_table,
    transformation_type="qrt_formatting",
    transformation_desc=(
        "Map silver_scr_modules rows to EIOPA S.25.01 template row references (R0010-R0220). "
        "Module-level rows use gross_capital_charge for C0110 and net_capital_charge for C0100. "
        "Aggregate and adjustment rows use net_capital_charge for both columns. "
        "Life underwriting risk, intangible risk, Art 4, and capital add-on hardcoded to 0 for P&C demo."
    ),
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    columns_in=["module_name", "sub_module_name", "level", "gross_capital_charge",
                 "net_capital_charge", "reporting_period"],
    columns_out=["reporting_reference_date", "reporting_entity_lei", "template_row_id",
                  "template_row_label", "c0110_gross_scr", "c0080_ust_simplifications",
                  "c0100_net_scr"],
    parameters={"catalog": catalog, "schema": schema, "reporting_date": reporting_date,
                "entity_lei": entity_lei},
    status="success",
    data_quality_checks=[{"name": c[0], "passed": c[1]} for c in dq_checks],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S.25.01 SCR Waterfall Summary

# COMMAND ----------

# Collect all template rows for formatted display
rows = spark.sql(f"""
    SELECT template_row_id, template_row_label, c0110_gross_scr, c0100_net_scr
      FROM {target_table}
     ORDER BY template_row_id
""").collect()

row_map = {r["template_row_id"]: r for r in rows}

print("=" * 80)
print("  S.25.01 — SOLVENCY CAPITAL REQUIREMENT (STANDARD FORMULA)")
print("=" * 80)
print(f"  Reporting date:  {reporting_date}")
print(f"  Entity LEI:      {entity_lei}")
print("=" * 80)
print(f"\n  {'Row':<8} {'Description':<52} {'C0110 Gross':>12} {'C0100 Net':>12}")
print("  " + "-" * 84)

# Risk modules section
for rid in ["R0010", "R0020", "R0030", "R0040", "R0050"]:
    r = row_map[rid]
    gross = r["c0110_gross_scr"] or 0.0
    net = r["c0100_net_scr"] or 0.0
    label = r["template_row_label"]
    print(f"  {rid:<8} {label:<52} {gross / 1e6:>11.1f}M {net / 1e6:>11.1f}M")

# Diversification
r = row_map["R0060"]
val = r["c0100_net_scr"] or 0.0
print(f"  {'R0060':<8} {'Diversification':<52} {val / 1e6:>11.1f}M {val / 1e6:>11.1f}M")

# Intangible
r = row_map["R0070"]
val = r["c0100_net_scr"] or 0.0
print(f"  {'R0070':<8} {'Intangible asset risk':<52} {val / 1e6:>11.1f}M {val / 1e6:>11.1f}M")

print("  " + "-" * 84)

# BSCR
r = row_map["R0100"]
val = r["c0100_net_scr"] or 0.0
print(f"  {'R0100':<8} {'Basic Solvency Capital Requirement':<52} {val / 1e6:>11.1f}M {val / 1e6:>11.1f}M")

print("  " + "-" * 84)

# Adjustments
for rid, label in [("R0130", "Operational risk"),
                   ("R0140", "LAC technical provisions"),
                   ("R0150", "LAC deferred taxes"),
                   ("R0160", "Art 4 Directive 2003/41/EC")]:
    r = row_map[rid]
    val = r["c0100_net_scr"] or 0.0
    print(f"  {rid:<8} {label:<52} {val / 1e6:>11.1f}M {val / 1e6:>11.1f}M")

print("  " + "=" * 84)

# SCR
r = row_map["R0200"]
scr_val = r["c0100_net_scr"] or 0.0
print(f"  {'R0200':<8} {'SOLVENCY CAPITAL REQUIREMENT':<52} {scr_val / 1e6:>11.1f}M {scr_val / 1e6:>11.1f}M")

print("  " + "=" * 84)

# Additional info
for rid, label in [("R0210", "Capital add-on already set"),
                   ("R0220", "SCR including capital add-on")]:
    r = row_map[rid]
    val = r["c0100_net_scr"] or 0.0
    print(f"  {rid:<8} {label:<52} {val / 1e6:>11.1f}M {val / 1e6:>11.1f}M")

print(f"\n  Target table: {target_table} ({row_count_out} rows)")
print(f"  Data quality: {'ALL PASSED' if all_passed else 'CHECKS FAILED — review above'}")
