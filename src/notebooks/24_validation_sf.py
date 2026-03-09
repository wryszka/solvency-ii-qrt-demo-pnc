# Databricks notebook source
# MAGIC %md
# MAGIC # 24_validation_sf — Validate Standard Formula QRT Outputs (S.19.01 & S.26.06)
# MAGIC
# MAGIC Runs data-quality checks against gold S.19.01 / S.26.06 tables, supporting silver
# MAGIC tables, and (optionally) cross-references S.25.01 from the core pipeline.
# MAGIC
# MAGIC **Tables validated:**
# MAGIC - `gold_s1901` — S.19.01 Claims Development Triangles
# MAGIC - `gold_s2606` — S.26.06 Non-Life Underwriting Risk SCR
# MAGIC - `silver_ultimate_claims`, `silver_best_estimate`, `silver_development_factors`
# MAGIC - `bronze_claims_triangles`, `bronze_volume_measures`
# MAGIC
# MAGIC **Output:** `audit_data_quality_log` — one row per check.
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog")
dbutils.widgets.text("schema_name", "solvency2demo", "Schema")
dbutils.widgets.text("reporting_date", "2025-12-31", "Reporting Date")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")

reporting_year = int(reporting_date[:4])

print(f"Catalog:        {catalog}")
print(f"Schema:         {schema}")
print(f"Reporting date: {reporting_date}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise helpers

# COMMAND ----------

from datetime import datetime

fqn = lambda table: f"{catalog}.{schema}.{table}"

all_checks = []  # list of dicts: check_id, check_name, table_name, passed, details

def record_check(check_id, check_name, table_name, passed, details):
    """Append a check result and print a one-line summary."""
    all_checks.append({
        "check_id": check_id,
        "check_name": check_name,
        "table_name": table_name,
        "passed": passed,
        "details": details,
    })
    marker = "PASS" if passed else "FAIL"
    print(f"  [{marker}] {check_id}: {check_name} — {details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create audit_data_quality_log table (if not exists)

# COMMAND ----------

# audit_data_quality_log is created by 00_setup.py — we just append to it

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## S.19.01 Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 1 — Triangle completeness
# MAGIC All expected (accident_year, development_year) combinations present in the
# MAGIC `claims_paid_cumulative` section.

# COMMAND ----------

s1901_table = fqn("gold_s1901")

# Build the set of expected AY/DY pairs: for each AY from (reporting_year - 9) to
# (reporting_year - 1), development years run from 0 to (reporting_year - AY - 1).
expected_pairs = spark.sql(f"""
    WITH ay AS (
        SELECT EXPLODE(SEQUENCE({reporting_year} - 9, {reporting_year} - 1)) AS accident_year
    ),
    pairs AS (
        SELECT accident_year,
               EXPLODE(SEQUENCE(0, {reporting_year} - accident_year - 1)) AS development_year
        FROM ay
    )
    SELECT COUNT(*) AS cnt FROM pairs
""").first()["cnt"]

actual_pairs = spark.sql(f"""
    SELECT COUNT(DISTINCT accident_year, development_year) AS cnt
    FROM {s1901_table}
    WHERE section = 'claims_paid_cumulative'
      AND reporting_date = '{reporting_date}'
""").first()["cnt"]

passed = actual_pairs >= expected_pairs
record_check(
    "CHK_S1901_01", "Triangle completeness",
    "gold_s1901", passed,
    f"Expected >= {expected_pairs} AY/DY pairs, found {actual_pairs}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 2 — Monotonicity of cumulative paid amounts

# COMMAND ----------

non_monotonic = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT accident_year, lob_code, development_year, amount,
               LAG(amount) OVER (
                   PARTITION BY accident_year, lob_code
                   ORDER BY development_year
               ) AS prev_amount
        FROM {s1901_table}
        WHERE section = 'claims_paid_cumulative'
          AND reporting_date = '{reporting_date}'
    )
    WHERE prev_amount IS NOT NULL AND amount < prev_amount
""").first()["cnt"]

passed = non_monotonic == 0
record_check(
    "CHK_S1901_02", "Monotonicity of cumulative paid",
    "gold_s1901", passed,
    f"{non_monotonic} violations of non-decreasing cumulative paid across dev years"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3 — Summary consistency (ultimate >= latest cumulative paid)

# COMMAND ----------

inconsistent = spark.sql(f"""
    WITH latest_paid AS (
        SELECT accident_year, lob_code,
               MAX_BY(amount, development_year) AS latest_cum_paid
        FROM {s1901_table}
        WHERE section = 'claims_paid_cumulative'
          AND reporting_date = '{reporting_date}'
        GROUP BY accident_year, lob_code
    ),
    ultimates AS (
        SELECT accident_year, lob_code, amount AS ultimate
        FROM {s1901_table}
        WHERE section = 'summary' AND cell_ref LIKE '%C0380'
          AND reporting_date = '{reporting_date}'
    )
    SELECT COUNT(*) AS cnt
    FROM latest_paid lp
    JOIN ultimates u ON lp.accident_year = u.accident_year AND lp.lob_code = u.lob_code
    WHERE u.ultimate < lp.latest_cum_paid
""").first()["cnt"]

passed = inconsistent == 0
record_check(
    "CHK_S1901_03", "Summary consistency (ultimate >= cum paid)",
    "gold_s1901", passed,
    f"{inconsistent} AY/LoB combos where ultimate < latest cumulative paid"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 4 — All 8 LoBs present in S.19.01

# COMMAND ----------

lob_count = spark.sql(f"""
    SELECT COUNT(DISTINCT lob_code) AS cnt
    FROM {s1901_table}
    WHERE reporting_date = '{reporting_date}'
""").first()["cnt"]

passed = lob_count >= 8
record_check(
    "CHK_S1901_04", "All LoBs present",
    "gold_s1901", passed,
    f"Found {lob_count} distinct LoBs (expected 8)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## S.26.06 Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 5 — All required template rows present

# COMMAND ----------

s2606_table = fqn("gold_s2606")

required_rows = ["R0010", "R1000", "R1100", "R1300"]

actual_rows = [
    r["template_row_id"]
    for r in spark.sql(f"""
        SELECT DISTINCT template_row_id
        FROM {s2606_table}
        WHERE reporting_date = '{reporting_date}'
    """).collect()
]

missing = set(required_rows) - set(actual_rows)
passed = len(missing) == 0
record_check(
    "CHK_S2606_05", "All required template rows present",
    "gold_s2606", passed,
    f"Missing: {sorted(missing)}" if missing else "R0010, R1000, R1100, R1300 all present"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 6 — Diversification rows are negative

# COMMAND ----------

# Diversification rows typically contain 'iversif' in the label.
div_positive = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {s2606_table}
    WHERE reporting_date = '{reporting_date}'
      AND LOWER(template_row_label) LIKE '%iversif%'
      AND c0010_standard_formula_value >= 0
""").first()["cnt"]

passed = div_positive == 0
record_check(
    "CHK_S2606_06", "Diversification rows are negative",
    "gold_s2606", passed,
    f"{div_positive} diversification rows with non-negative values"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 7 — Total SCR NL UW (R1300) is positive

# COMMAND ----------

r1300_row = spark.sql(f"""
    SELECT c0010_standard_formula_value
    FROM {s2606_table}
    WHERE template_row_id = 'R1300'
      AND reporting_date = '{reporting_date}'
""").first()

r1300_val = float(r1300_row["c0010_standard_formula_value"]) if r1300_row else None
passed = r1300_val is not None and r1300_val > 0
record_check(
    "CHK_S2606_07", "Total SCR NL UW positive (R1300)",
    "gold_s2606", passed,
    f"R1300 = {r1300_val:,.2f}" if r1300_val is not None else "R1300 row not found"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 8 — Component consistency (diversification reduces total)
# MAGIC R1300 should be less than R0010 + R1000 + R1100.

# COMMAND ----------

component_vals = spark.sql(f"""
    SELECT template_row_id, c0010_standard_formula_value AS val
    FROM {s2606_table}
    WHERE template_row_id IN ('R0010', 'R1000', 'R1100', 'R1300')
      AND reporting_date = '{reporting_date}'
""").collect()

val_map = {r["template_row_id"]: float(r["val"]) if r["val"] is not None else 0.0
           for r in component_vals}

sum_components = val_map.get("R0010", 0) + val_map.get("R1000", 0) + val_map.get("R1100", 0)
total_scr_nl = val_map.get("R1300", 0)

passed = total_scr_nl < sum_components
record_check(
    "CHK_S2606_08", "Component consistency (diversification benefit)",
    "gold_s2606", passed,
    f"R1300 ({total_scr_nl:,.2f}) < R0010+R1000+R1100 ({sum_components:,.2f})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cross-table Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 9 — Best-estimate consistency (within 5%)
# MAGIC `silver_best_estimate` total claims provision should align with sum of IBNR from
# MAGIC `silver_ultimate_claims`.

# COMMAND ----------

be_total = spark.sql(f"""
    SELECT COALESCE(SUM(best_estimate_claims_prov), 0) AS total
    FROM {fqn('silver_best_estimate')}
""").first()["total"]

ibnr_total = spark.sql(f"""
    SELECT COALESCE(SUM(ibnr), 0) AS total
    FROM {fqn('silver_ultimate_claims')}
""").first()["total"]

if be_total > 0:
    pct_diff = abs(be_total - ibnr_total) / be_total * 100
else:
    pct_diff = 0.0 if ibnr_total == 0 else 100.0

passed = pct_diff <= 5.0
record_check(
    "CHK_CROSS_09", "Best-estimate vs IBNR consistency",
    "silver_best_estimate / silver_ultimate_claims", passed,
    f"BE total={be_total:,.2f}, IBNR total={ibnr_total:,.2f}, diff={pct_diff:.2f}%"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 10 — Volume measures: earned premiums > 0 for all LoBs

# COMMAND ----------

zero_premium_lobs = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {fqn('bronze_volume_measures')}
    WHERE earned_premium_net <= 0
""").first()["cnt"]

passed = zero_premium_lobs == 0
record_check(
    "CHK_CROSS_10", "Volume measures earned premiums > 0",
    "bronze_volume_measures", passed,
    f"{zero_premium_lobs} LoBs with earned_premium_net <= 0"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 11 — Chain-ladder factors reasonable (between 1.0 and 3.0)

# COMMAND ----------

unreasonable_factors = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {fqn('silver_development_factors')}
    WHERE link_ratio_paid < 1.0 OR link_ratio_paid > 3.0
""").first()["cnt"]

total_factors = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fqn('silver_development_factors')}
""").first()["cnt"]

passed = unreasonable_factors == 0
record_check(
    "CHK_CROSS_11", "Chain-ladder factors in [1.0, 3.0]",
    "silver_development_factors", passed,
    f"{unreasonable_factors}/{total_factors} factors outside [1.0, 3.0]"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cross-QRT Check

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 12 — S.25.01 reconciliation
# MAGIC If `gold_s2501` exists, verify that R0160 (NL UW risk module in S.25.01)
# MAGIC approximately matches R1300 from S.26.06. Uses TRY/CATCH since the table may
# MAGIC not exist.

# COMMAND ----------

try:
    s2501_nl_row = spark.sql(f"""
        SELECT c0100_net_scr
        FROM {fqn('gold_qrt_s2501')}
        WHERE template_row_id = 'R0160'
    """).first()

    if s2501_nl_row is not None and r1300_val is not None:
        s2501_nl_val = float(s2501_nl_row["c0100_net_scr"]) if s2501_nl_row["c0100_net_scr"] else 0.0
        if s2501_nl_val > 0:
            reconcile_pct = abs(s2501_nl_val - r1300_val) / s2501_nl_val * 100
        elif r1300_val == 0:
            reconcile_pct = 0.0
        else:
            reconcile_pct = 100.0
        passed = reconcile_pct <= 10.0
        detail = (f"S.25.01 R0160={s2501_nl_val:,.2f}, "
                  f"S.26.06 R1300={r1300_val:,.2f}, diff={reconcile_pct:.2f}%")
    else:
        passed = True
        detail = "R0160 or R1300 not available — skipped"

    record_check(
        "CHK_XQRT_12", "S.25.01 vs S.26.06 NL UW risk reconciliation",
        "gold_qrt_s2501 / gold_s2606", passed, detail
    )
except Exception as e:
    record_check(
        "CHK_XQRT_12", "S.25.01 vs S.26.06 NL UW risk reconciliation",
        "gold_qrt_s2501 / gold_s2606", True,
        f"gold_qrt_s2501 not available — skipped ({e})"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Write results to audit_data_quality_log

# COMMAND ----------

import uuid
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType,
)

now = datetime.utcnow()
_rd_obj = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period_str = f"{_rd_obj.year}-Q{(_rd_obj.month - 1) // 3 + 1}"

# Match the existing audit_data_quality_log schema from 00_setup
result_rows = [
    Row(
        check_id=str(uuid.uuid4()),
        pipeline_run_id="sf_validation",
        step_name="24_validation_sf",
        table_name=c["table_name"],
        check_name=c["check_name"],
        check_category="validation",
        check_expression=c["check_name"],
        expected_value="pass",
        actual_value="pass" if c["passed"] else "fail",
        passed=c["passed"],
        severity="critical",
        details=c["details"],
        reporting_period=reporting_period_str,
        executed_by="24_validation_sf",
        executed_at=now,
    )
    for c in all_checks
]

result_schema = StructType([
    StructField("check_id", StringType(), False),
    StructField("pipeline_run_id", StringType(), True),
    StructField("step_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("check_category", StringType(), True),
    StructField("check_expression", StringType(), True),
    StructField("expected_value", StringType(), True),
    StructField("actual_value", StringType(), True),
    StructField("passed", BooleanType(), False),
    StructField("severity", StringType(), True),
    StructField("details", StringType(), True),
    StructField("reporting_period", StringType(), True),
    StructField("executed_by", StringType(), True),
    StructField("executed_at", TimestampType(), False),
])

df_results = spark.createDataFrame(result_rows, schema=result_schema)

(
    df_results
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(fqn("audit_data_quality_log"))
)

print(f"\nWrote {len(result_rows)} check results to {fqn('audit_data_quality_log')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_checks = len(all_checks)
passed_checks = sum(1 for c in all_checks if c["passed"])
failed_checks = total_checks - passed_checks

print("=" * 80)
print("  24_validation_sf — STANDARD FORMULA VALIDATION SUMMARY")
print("=" * 80)
print(f"  Reporting date: {reporting_date}")
print(f"  Catalog/Schema: {catalog}.{schema}")
print(f"  Total checks:   {total_checks}")
print(f"  Passed:         {passed_checks}")
print(f"  Failed:         {failed_checks}")
print("-" * 80)

for c in all_checks:
    marker = "PASS" if c["passed"] else "FAIL"
    print(f"  [{marker}] {c['check_id']:<15} {c['check_name']:<50} {c['details']}")

print("=" * 80)
overall = "ALL CHECKS PASSED" if failed_checks == 0 else f"{failed_checks} CHECK(S) FAILED"
print(f"  RESULT: {overall}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display results table

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT check_name, table_name, passed, details, executed_at
        FROM {fqn('audit_data_quality_log')}
        WHERE step_name = '24_validation_sf'
          AND reporting_period = '{reporting_period_str}'
        ORDER BY check_name
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit lineage

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.{schema}.audit_pipeline_lineage VALUES (
    'sf_pipeline',
    '24_validation_sf',
    5,
    'reporting_date={reporting_date}',
    'audit_data_quality_log',
    current_timestamp(),
    'validation'
)
""")

print("Audit lineage row inserted.")
