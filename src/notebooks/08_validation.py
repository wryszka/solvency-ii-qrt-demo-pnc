# Databricks notebook source
# MAGIC %md
# MAGIC # 08_validation — Final Pipeline Validation & Summary Dashboard
# MAGIC
# MAGIC Runs cross-table validation checks across all gold QRT tables and produces a
# MAGIC summary dashboard showing pipeline health, key metrics, and data quality results.
# MAGIC
# MAGIC **Gold tables validated:**
# MAGIC - `gold_qrt_s0602` — S.06.02 List of Assets
# MAGIC - `gold_qrt_s0501` — S.05.01 Non-Life Premiums, Claims and Expenses
# MAGIC - `gold_qrt_s2501` — S.25.01 Solvency Capital Requirement (Standard Formula)
# MAGIC
# MAGIC **Cross-table checks:**
# MAGIC - Total assets SII value (S.06.02) > SCR (S.25.01) — basic solvency sanity check
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage` and `audit_data_quality_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

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

# MAGIC %md
# MAGIC ## Initialise Lineage Logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("08_validation")

fqn = lambda table: f"{catalog}.{schema}.{table}"

# Track all checks for final summary
all_checks = []  # list of (table, check_name, passed, detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Validate gold_qrt_s0602 (S.06.02 — List of Assets)

# COMMAND ----------

s0602_table = fqn("gold_qrt_s0602")

# Check 1a: Row count > 0
s0602_count = spark.table(s0602_table).count()
passed = s0602_count > 0
all_checks.append(("gold_qrt_s0602", "row_count_positive", passed, f"{s0602_count} rows"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0602_table,
    check_name="s0602_row_count_positive", check_category="completeness",
    check_expression="COUNT(*) > 0",
    expected_value="> 0", actual_value=str(s0602_count),
    passed=passed, severity="critical",
)

# Check 1b: Total SII value > 0
total_sii = spark.sql(f"""
    SELECT COALESCE(SUM(c0170_total_sii_amount), 0) AS total FROM {s0602_table}
""").first()["total"]
passed = total_sii > 0
all_checks.append(("gold_qrt_s0602", "total_sii_value_positive", passed, f"EUR {total_sii:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0602_table,
    check_name="s0602_total_sii_positive", check_category="validity",
    check_expression="SUM(c0170_total_sii_amount) > 0",
    expected_value="> 0", actual_value=f"{total_sii:,.2f}",
    passed=passed, severity="critical",
)

# Check 1c: No null asset IDs
null_asset_ids = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {s0602_table} WHERE c0040_asset_id IS NULL
""").first()["cnt"]
passed = null_asset_ids == 0
all_checks.append(("gold_qrt_s0602", "no_null_asset_ids", passed, f"{null_asset_ids} nulls"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0602_table,
    check_name="s0602_no_null_asset_ids", check_category="completeness",
    check_expression="COUNT(*) WHERE c0040_asset_id IS NULL = 0",
    expected_value="0", actual_value=str(null_asset_ids),
    passed=passed, severity="critical",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate gold_qrt_s0501 (S.05.01 — Non-Life Premiums, Claims and Expenses)

# COMMAND ----------

s0501_table = fqn("gold_qrt_s0501")

# Check 2a: All expected template row IDs present (R0110 - R1300)
expected_s0501_ids = {
    "R0110", "R0140", "R0200",
    "R0210", "R0240", "R0300",
    "R0310", "R0340", "R0400",
    "R0410", "R0440", "R0500",
    "R0510", "R0540",
    "R0550", "R0610", "R0620", "R0630", "R0640", "R0680",
    "R1200", "R1300",
}
actual_s0501_ids = {
    r["template_row_id"]
    for r in spark.sql(f"SELECT DISTINCT template_row_id FROM {s0501_table}").collect()
}
missing_s0501 = expected_s0501_ids - actual_s0501_ids
passed = len(missing_s0501) == 0
all_checks.append(("gold_qrt_s0501", "all_template_row_ids_present", passed,
                    f"missing: {sorted(missing_s0501)}" if missing_s0501 else "all 22 present"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0501_table,
    check_name="s0501_all_row_ids_present", check_category="completeness",
    check_expression="All 22 expected EIOPA row IDs (R0110-R1300) present",
    expected_value="0 missing",
    actual_value=f"{len(missing_s0501)} missing" if missing_s0501 else "0 missing",
    passed=passed, severity="critical",
)

# Check 2b: Net premiums written (R0200 Total) > 0
nwp_row = spark.sql(f"""
    SELECT amount_eur FROM {s0501_table}
    WHERE template_row_id = 'R0200' AND lob_code = 0
""").first()
total_nwp = float(nwp_row["amount_eur"]) if nwp_row else 0.0
passed = total_nwp > 0
all_checks.append(("gold_qrt_s0501", "net_premiums_positive", passed, f"EUR {total_nwp:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0501_table,
    check_name="s0501_net_premiums_positive", check_category="validity",
    check_expression="R0200 (Net Premiums Written, Total) > 0",
    expected_value="> 0", actual_value=f"{total_nwp:,.2f}",
    passed=passed, severity="critical",
)

# Check 2c: Combined ratio is reasonable (50% - 150%)
npe_row = spark.sql(f"""
    SELECT amount_eur FROM {s0501_table}
    WHERE template_row_id = 'R0300' AND lob_code = 0
""").first()
total_npe = float(npe_row["amount_eur"]) if npe_row else 0.0

net_claims_row = spark.sql(f"""
    SELECT amount_eur FROM {s0501_table}
    WHERE template_row_id = 'R0400' AND lob_code = 0
""").first()
total_net_claims = float(net_claims_row["amount_eur"]) if net_claims_row else 0.0

expenses_row = spark.sql(f"""
    SELECT amount_eur FROM {s0501_table}
    WHERE template_row_id = 'R1300' AND lob_code = 0
""").first()
total_expenses = float(expenses_row["amount_eur"]) if expenses_row else 0.0

if total_npe > 0:
    combined_ratio = ((total_net_claims + total_expenses) / total_npe) * 100
else:
    combined_ratio = 0.0

passed = 50.0 <= combined_ratio <= 150.0
all_checks.append(("gold_qrt_s0501", "combined_ratio_reasonable", passed, f"{combined_ratio:.1f}%"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s0501_table,
    check_name="s0501_combined_ratio_reasonable", check_category="business_rule",
    check_expression="Combined ratio between 50% and 150%",
    expected_value="50% - 150%", actual_value=f"{combined_ratio:.1f}%",
    passed=passed, severity="warning",
    details="Combined ratio = (Net Claims + Total Expenses) / Net Premiums Earned",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate gold_qrt_s2501 (S.25.01 — SCR Standard Formula)

# COMMAND ----------

s2501_table = fqn("gold_qrt_s2501")

# Check 3a: All expected template row IDs present (R0010 - R0220)
expected_s2501_ids = {
    "R0010", "R0020", "R0030", "R0040", "R0050", "R0060", "R0070",
    "R0100", "R0130", "R0140", "R0150", "R0160", "R0200", "R0210", "R0220",
}
actual_s2501_ids = {
    r["template_row_id"]
    for r in spark.sql(f"SELECT DISTINCT template_row_id FROM {s2501_table}").collect()
}
missing_s2501 = expected_s2501_ids - actual_s2501_ids
passed = len(missing_s2501) == 0
all_checks.append(("gold_qrt_s2501", "all_template_row_ids_present", passed,
                    f"missing: {sorted(missing_s2501)}" if missing_s2501 else "all 15 present"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s2501_table,
    check_name="s2501_all_row_ids_present", check_category="completeness",
    check_expression="All 15 expected EIOPA row IDs (R0010-R0220) present",
    expected_value="0 missing",
    actual_value=f"{len(missing_s2501)} missing" if missing_s2501 else "0 missing",
    passed=passed, severity="critical",
)

# Check 3b: SCR (R0200) > 0
scr_row = spark.sql(f"""
    SELECT c0100_net_scr FROM {s2501_table} WHERE template_row_id = 'R0200'
""").first()
scr_value = float(scr_row["c0100_net_scr"]) if scr_row and scr_row["c0100_net_scr"] else 0.0
passed = scr_value > 0
all_checks.append(("gold_qrt_s2501", "scr_positive", passed, f"EUR {scr_value:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s2501_table,
    check_name="s2501_scr_positive", check_category="business_rule",
    check_expression="R0200 SCR > 0",
    expected_value="> 0", actual_value=f"{scr_value:,.2f}",
    passed=passed, severity="critical",
)

# Check 3c: BSCR (R0100) > SCR (R0200)
bscr_row = spark.sql(f"""
    SELECT c0100_net_scr FROM {s2501_table} WHERE template_row_id = 'R0100'
""").first()
bscr_value = float(bscr_row["c0100_net_scr"]) if bscr_row and bscr_row["c0100_net_scr"] else 0.0
passed = bscr_value > scr_value
all_checks.append(("gold_qrt_s2501", "bscr_greater_than_scr", passed,
                    f"BSCR={bscr_value:,.2f} > SCR={scr_value:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s2501_table,
    check_name="s2501_bscr_gt_scr", check_category="business_rule",
    check_expression="R0100 BSCR > R0200 SCR",
    expected_value="BSCR > SCR",
    actual_value=f"BSCR={bscr_value:,.2f}, SCR={scr_value:,.2f}",
    passed=passed, severity="critical",
)

# Check 3d: Diversification (R0060) < 0
div_row = spark.sql(f"""
    SELECT c0100_net_scr FROM {s2501_table} WHERE template_row_id = 'R0060'
""").first()
div_value = float(div_row["c0100_net_scr"]) if div_row and div_row["c0100_net_scr"] else 0.0
passed = div_value < 0
all_checks.append(("gold_qrt_s2501", "diversification_negative", passed, f"EUR {div_value:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name=s2501_table,
    check_name="s2501_diversification_negative", check_category="business_rule",
    check_expression="R0060 Diversification < 0",
    expected_value="< 0", actual_value=f"{div_value:,.2f}",
    passed=passed, severity="critical",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Cross-Table Validation

# COMMAND ----------

# Cross-check: Total assets SII value (S.06.02) > SCR (S.25.01)
# This is a basic solvency sanity check — an insurer's assets must exceed its capital requirement.
passed = total_sii > scr_value
all_checks.append(("cross_table", "assets_sii_gt_scr", passed,
                    f"Assets={total_sii:,.2f} > SCR={scr_value:,.2f}"))
lineage.log_data_quality(
    step_name="08_validation", table_name="cross_table",
    check_name="cross_assets_sii_gt_scr", check_category="business_rule",
    check_expression="S.06.02 total SII value > S.25.01 SCR",
    expected_value="Total assets > SCR",
    actual_value=f"Assets={total_sii:,.2f}, SCR={scr_value:,.2f}",
    passed=passed, severity="critical",
    details="Basic solvency sanity check: total asset SII value from S.06.02 must exceed SCR from S.25.01.",
)

# Solvency ratio
if scr_value > 0:
    solvency_ratio = (total_sii / scr_value) * 100
else:
    solvency_ratio = 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Log Lineage for Validation Step

# COMMAND ----------

source_tables = [s0602_table, s0501_table, s2501_table]

total_checks = len(all_checks)
passed_checks = sum(1 for c in all_checks if c[2])
failed_checks = total_checks - passed_checks

lineage.log_lineage(
    step_name="08_validation",
    step_sequence=8,
    source_tables=source_tables,
    target_table=None,
    transformation_type="validation",
    transformation_desc=(
        "Cross-table validation of all gold QRT tables (S.06.02, S.05.01, S.25.01). "
        "Checks completeness, business rules, and cross-table solvency consistency. "
        f"{total_checks} checks executed: {passed_checks} passed, {failed_checks} failed."
    ),
    row_count_in=None,
    row_count_out=None,
    parameters={
        "catalog": catalog,
        "schema": schema,
        "reporting_date": reporting_date,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
    },
    status="success" if failed_checks == 0 else "warning",
    data_quality_checks=[{"name": c[1], "table": c[0], "passed": c[2]} for c in all_checks],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Schema Table Inventory

# COMMAND ----------

# List all tables in the schema with row counts
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
table_inventory = []

for row in tables_df.collect():
    tbl_name = row["tableName"]
    try:
        cnt = spark.table(f"{catalog}.{schema}.{tbl_name}").count()
        table_inventory.append((tbl_name, cnt))
    except Exception:
        table_inventory.append((tbl_name, -1))

table_inventory.sort(key=lambda x: x[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary Dashboard

# COMMAND ----------

# Gather key metrics
gwp_row = spark.sql(f"""
    SELECT amount_eur FROM {s0501_table}
    WHERE template_row_id = 'R0110' AND lob_code = 0
""").first()
total_gwp = float(gwp_row["amount_eur"]) if gwp_row else 0.0

print("=" * 90)
print("  SOLVENCY II QRT PIPELINE — FINAL VALIDATION DASHBOARD")
print("=" * 90)
print(f"  Reporting date:     {reporting_date}")
print(f"  Reporting period:   {reporting_period}")
print(f"  Catalog:            {catalog}")
print(f"  Schema:             {schema}")
print(f"  Pipeline run ID:    {lineage.pipeline_run_id}")

# --- Pipeline Run Status ---
print("\n" + "-" * 90)
print("  PIPELINE RUN STATUS")
print("-" * 90)

gold_tables = {
    "gold_qrt_s0602": s0602_count,
    "gold_qrt_s0501": spark.table(s0501_table).count(),
    "gold_qrt_s2501": spark.table(s2501_table).count(),
}

all_tables_ok = all(cnt > 0 for cnt in gold_tables.values())
status_label = "ALL TABLES POPULATED" if all_tables_ok else "ISSUES DETECTED"
print(f"\n  Overall status: {status_label}")
for tbl, cnt in gold_tables.items():
    marker = "OK" if cnt > 0 else "EMPTY"
    print(f"    [{marker:>5}] {tbl:<30} {cnt:>8,} rows")

# --- Key Metrics ---
print("\n" + "-" * 90)
print("  KEY METRICS")
print("-" * 90)
print(f"\n  S.06.02 — Assets")
print(f"    Total assets:                  {s0602_count:>10,}")
print(f"    Total SII value:               EUR {total_sii:>18,.2f}")

print(f"\n  S.05.01 — Premiums, Claims, Expenses")
print(f"    Gross written premiums (GWP):  EUR {total_gwp:>18,.2f}")
print(f"    Net written premiums (NWP):    EUR {total_nwp:>18,.2f}")
print(f"    Net claims incurred:           EUR {total_net_claims:>18,.2f}")
print(f"    Total expenses:                EUR {total_expenses:>18,.2f}")
print(f"    Combined ratio:                {combined_ratio:>18.1f}%")

print(f"\n  S.25.01 — Solvency Capital Requirement")
print(f"    BSCR:                          EUR {bscr_value:>18,.2f}")
print(f"    SCR:                           EUR {scr_value:>18,.2f}")
print(f"    Diversification:               EUR {div_value:>18,.2f}")
print(f"    Solvency ratio (Assets/SCR):   {solvency_ratio:>18.1f}%")

# --- Data Quality Check Results ---
print("\n" + "-" * 90)
print("  DATA QUALITY CHECK RESULTS")
print("-" * 90)
print(f"\n  Total checks:  {total_checks}")
print(f"  Passed:        {passed_checks}")
print(f"  Failed:        {failed_checks}")
print()

for tbl, check_name, passed, detail in all_checks:
    marker = "PASS" if passed else "FAIL"
    print(f"    [{marker}] {tbl:<20} {check_name:<35} {detail}")

# --- Schema Table Inventory ---
print("\n" + "-" * 90)
print("  SCHEMA TABLE INVENTORY")
print("-" * 90)
print(f"\n  {catalog}.{schema}")
print()
for tbl_name, cnt in table_inventory:
    if cnt >= 0:
        print(f"    {tbl_name:<45} {cnt:>10,} rows")
    else:
        print(f"    {tbl_name:<45} {'ERROR':>10}")

# --- Footer ---
print("\n" + "=" * 90)
overall_status = "PASSED" if (failed_checks == 0 and all_tables_ok) else "ISSUES DETECTED"
print(f"  VALIDATION RESULT: {overall_status}")
print(f"  {total_checks} checks executed | {passed_checks} passed | {failed_checks} failed")
print("=" * 90)
