# Databricks notebook source
# MAGIC %md
# MAGIC # 22_gold_s1901 — Format claims development triangles into EIOPA S.19.01 QRT template
# MAGIC
# MAGIC Reads `bronze_claims_triangles` and `silver_ultimate_claims` and produces `gold_s1901` —
# MAGIC the EIOPA S.19.01 "Non-Life Insurance Claims Information" QRT template in long format.
# MAGIC
# MAGIC **Sections produced:**
# MAGIC 1. **Claims paid (cumulative)** — development triangle of cumulative gross claims paid
# MAGIC 2. **RBNS claims (gross)** — Reported But Not Settled (case reserves) at each development point
# MAGIC 3. **Summary** — Total RBNS, Total IBNR, and Ultimate claims per accident year
# MAGIC
# MAGIC **Source tables:** `bronze_claims_triangles`, `silver_ultimate_claims`
# MAGIC **Target table:** `gold_s1901`

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

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

# Derive reporting period (YYYY-QN) from reporting_date
from datetime import datetime
_rd = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period = f"{_rd.year}-Q{(_rd.month - 1) // 3 + 1}"
reporting_year = _rd.year

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Reporting period: {reporting_period}")
print(f"Entity LEI:       {entity_lei}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise Lineage Logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)

source_triangle = f"{catalog}.{schema}.bronze_claims_triangles"
source_ultimate = f"{catalog}.{schema}.silver_ultimate_claims"
target_table = f"{catalog}.{schema}.gold_s1901"
step_name = "gold_s1901_claims_information"

lineage.start_step(step_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

df_triangles = spark.table(source_triangle)
row_count_triangles = df_triangles.count()
print(f"bronze_claims_triangles: {row_count_triangles} rows")

df_ultimate = spark.table(source_ultimate)
row_count_ultimate = df_ultimate.count()
print(f"silver_ultimate_claims:  {row_count_ultimate} rows")

row_count_in = row_count_triangles + row_count_ultimate
columns_in = list(set(df_triangles.columns + df_ultimate.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Accident Year Range and Row/Column Mapping
# MAGIC
# MAGIC EIOPA S.19.01 cell reference pattern:
# MAGIC - **Rows:** R0100 (oldest accident year) through R0260 (most recent), incrementing by 10.
# MAGIC   Up to 17 accident years (development years 0 through 16).
# MAGIC - **Claims paid / RBNS columns:** C0010 (dev year 0) through C0170 (dev year 16), incrementing by 10.
# MAGIC - **Summary columns:** C0360 (year-end total RBNS), C0370 (total IBNR), C0380 (ultimate claims).

# COMMAND ----------

# Determine the accident year range present in the data
ay_range = spark.sql(f"""
    SELECT MIN(accident_year) AS min_ay, MAX(accident_year) AS max_ay
    FROM {source_triangle}
""").first()

min_ay = ay_range["min_ay"]
max_ay = ay_range["max_ay"]
num_years = max_ay - min_ay + 1

print(f"Accident year range: {min_ay} to {max_ay} ({num_years} years)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build S.19.01 QRT Table
# MAGIC
# MAGIC Long-format table with UNION ALL blocks for each section:
# MAGIC 1. Claims paid cumulative triangle
# MAGIC 2. RBNS (case reserves) triangle
# MAGIC 3. Summary rows (total RBNS, IBNR, ultimate)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {target_table} AS

-- =========================================================================
-- Section 1: Claims paid (cumulative) — development triangle
-- =========================================================================
SELECT
    DATE('{reporting_date}')                                    AS reporting_date,
    '{entity_lei}'                                              AS entity_lei,
    CAST(lob_code AS INT)                                       AS lob_code,
    lob_name                                                    AS lob_name,
    'claims_paid_cumulative'                                    AS section,
    CAST(accident_year AS INT)                                  AS accident_year,
    CAST(development_period - 1 AS INT)                         AS development_year,
    CONCAT(
        'R', LPAD(CAST(100 + (accident_year - {min_ay}) * 10 AS STRING), 4, '0'),
        'C', LPAD(CAST(10 + (development_period - 1) * 10 AS STRING), 4, '0')
    )                                                           AS cell_ref,
    CAST(cumulative_paid AS DOUBLE)                             AS amount
FROM {source_triangle}

UNION ALL

-- =========================================================================
-- Section 2: RBNS claims (gross) — case reserves at each development point
-- =========================================================================
SELECT
    DATE('{reporting_date}')                                    AS reporting_date,
    '{entity_lei}'                                              AS entity_lei,
    CAST(lob_code AS INT)                                       AS lob_code,
    lob_name                                                    AS lob_name,
    'rbns_claims'                                               AS section,
    CAST(accident_year AS INT)                                  AS accident_year,
    CAST(development_period - 1 AS INT)                         AS development_year,
    CONCAT(
        'R', LPAD(CAST(100 + (accident_year - {min_ay}) * 10 AS STRING), 4, '0'),
        'C', LPAD(CAST(10 + (development_period - 1) * 10 AS STRING), 4, '0')
    )                                                           AS cell_ref,
    CAST(cumulative_incurred - cumulative_paid AS DOUBLE)       AS amount
FROM {source_triangle}

UNION ALL

-- =========================================================================
-- Section 3a: Summary — Total RBNS at year end (latest development year)
-- Uses the case reserves from the most recent development year for each AY
-- =========================================================================
SELECT
    DATE('{reporting_date}')                                    AS reporting_date,
    '{entity_lei}'                                              AS entity_lei,
    CAST(t.lob_code AS INT)                                     AS lob_code,
    t.lob_name                                                  AS lob_name,
    'summary'                                                   AS section,
    CAST(t.accident_year AS INT)                                AS accident_year,
    CAST(NULL AS INT)                                           AS development_year,
    CONCAT(
        'R', LPAD(CAST(100 + (t.accident_year - {min_ay}) * 10 AS STRING), 4, '0'),
        'C0360'
    )                                                           AS cell_ref,
    CAST(t.cumulative_incurred - t.cumulative_paid AS DOUBLE)   AS amount
FROM {source_triangle} t
INNER JOIN (
    SELECT lob_code, accident_year, MAX(development_period) AS max_dp
    FROM {source_triangle}
    GROUP BY lob_code, accident_year
) latest
    ON  t.lob_code = latest.lob_code
    AND t.accident_year = latest.accident_year
    AND t.development_period = latest.max_dp

UNION ALL

-- =========================================================================
-- Section 3b: Summary — Total IBNR
-- =========================================================================
SELECT
    DATE('{reporting_date}')                                    AS reporting_date,
    '{entity_lei}'                                              AS entity_lei,
    CAST(u.lob_code AS INT)                                     AS lob_code,
    t.lob_name                                                  AS lob_name,
    'summary'                                                   AS section,
    CAST(u.accident_year AS INT)                                AS accident_year,
    CAST(NULL AS INT)                                           AS development_year,
    CONCAT(
        'R', LPAD(CAST(100 + (u.accident_year - {min_ay}) * 10 AS STRING), 4, '0'),
        'C0370'
    )                                                           AS cell_ref,
    CAST(u.ibnr AS DOUBLE)                                      AS amount
FROM {source_ultimate} u
INNER JOIN (
    SELECT DISTINCT lob_code, lob_name, accident_year
    FROM {source_triangle}
) t
    ON u.lob_code = t.lob_code AND u.accident_year = t.accident_year

UNION ALL

-- =========================================================================
-- Section 3c: Summary — Ultimate claims
-- =========================================================================
SELECT
    DATE('{reporting_date}')                                    AS reporting_date,
    '{entity_lei}'                                              AS entity_lei,
    CAST(u.lob_code AS INT)                                     AS lob_code,
    t.lob_name                                                  AS lob_name,
    'summary'                                                   AS section,
    CAST(u.accident_year AS INT)                                AS accident_year,
    CAST(NULL AS INT)                                           AS development_year,
    CONCAT(
        'R', LPAD(CAST(100 + (u.accident_year - {min_ay}) * 10 AS STRING), 4, '0'),
        'C0380'
    )                                                           AS cell_ref,
    CAST(u.ultimate_paid AS DOUBLE)                             AS amount
FROM {source_ultimate} u
INNER JOIN (
    SELECT DISTINCT lob_code, lob_name, accident_year
    FROM {source_triangle}
) t
    ON u.lob_code = t.lob_code AND u.accident_year = t.accident_year
""")

row_count_out = spark.table(target_table).count()
columns_out = spark.table(target_table).columns

print(f"Table {target_table} created successfully ({row_count_out} rows, {len(columns_out)} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Column Comments (Unity Catalog Governance)

# COMMAND ----------

COLUMN_COMMENTS = {
    "reporting_date":   "S.19.01 header — Reporting reference date (end of reporting period).",
    "entity_lei":       "S.19.01 header — LEI of the reporting undertaking.",
    "lob_code":         "EIOPA line of business numeric code.",
    "lob_name":         "EIOPA line of business name.",
    "section":          "S.19.01 section: claims_paid_cumulative, rbns_claims, or summary.",
    "accident_year":    "Accident / underwriting year of the claims cohort.",
    "development_year": "Development year (0 = same year as accident). NULL for summary rows.",
    "cell_ref":         "EIOPA cell reference (e.g. R0100C0010). Row encodes accident year, column encodes development year or summary metric.",
    "amount":           "Monetary amount in EUR for the given cell reference.",
}

for col_name, comment in COLUMN_COMMENTS.items():
    try:
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col_name} COMMENT '{safe_comment}'")
    except Exception as e:
        print(f"  Could not comment column '{col_name}': {str(e)[:80]}")

table_comment = (
    "EIOPA S.19.01 Non-Life Insurance Claims Information QRT template. "
    "Long format with sections for cumulative claims paid triangle, RBNS triangle, "
    "and summary (total RBNS, IBNR, ultimate claims). "
    "Source: bronze_claims_triangles, silver_ultimate_claims."
)
safe_table_comment = table_comment.replace("'", "''")
spark.sql(f"COMMENT ON TABLE {target_table} IS '{safe_table_comment}'")

print(f"Column and table comments applied to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

dq_checks = []

# --- Check 1: All accident years present in triangle ---
# Every accident year from min_ay to max_ay should have at least one row in the output
expected_ays = set(range(min_ay, max_ay + 1))
actual_ays = {
    r["accident_year"]
    for r in spark.sql(f"""
        SELECT DISTINCT accident_year FROM {target_table}
        WHERE section = 'claims_paid_cumulative'
    """).collect()
}
missing_ays = expected_ays - actual_ays
check1_passed = len(missing_ays) == 0
dq_checks.append(("all_accident_years_present", check1_passed))
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="all_accident_years_present",
    check_category="completeness",
    check_expression=f"All accident years {min_ay}-{max_ay} present in claims_paid_cumulative section",
    expected_value="0 missing",
    actual_value=f"{len(missing_ays)} missing: {missing_ays}" if missing_ays else "0 missing",
    passed=check1_passed,
    severity="error",
    details="Every accident year in the reporting range must appear in the claims paid triangle."
)

# --- Check 2: Cumulative paid amounts are non-decreasing across development periods ---
non_decreasing_violations = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT
            lob_code, accident_year, development_year,
            amount,
            LAG(amount) OVER (
                PARTITION BY lob_code, accident_year
                ORDER BY development_year
            ) AS prev_amount
        FROM {target_table}
        WHERE section = 'claims_paid_cumulative'
    ) sub
    WHERE prev_amount IS NOT NULL AND amount < prev_amount
""").first()["cnt"]

check2_passed = (non_decreasing_violations == 0)
dq_checks.append(("cumulative_paid_non_decreasing", check2_passed))
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="cumulative_paid_non_decreasing",
    check_category="validity",
    check_expression="Cumulative paid amounts must not decrease across development years",
    expected_value="0 violations",
    actual_value=f"{non_decreasing_violations} violations",
    passed=check2_passed,
    severity="error",
    details="Cumulative claims paid should be monotonically non-decreasing as development years progress."
)

# --- Check 3: Ultimate >= cumulative paid for all AY/LoB combinations ---
ultimate_vs_paid = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT u.lob_code, u.accident_year, u.amount AS ultimate,
               p.amount AS latest_paid
        FROM {target_table} u
        INNER JOIN (
            SELECT lob_code, accident_year, amount,
                   ROW_NUMBER() OVER (
                       PARTITION BY lob_code, accident_year
                       ORDER BY development_year DESC
                   ) AS rn
            FROM {target_table}
            WHERE section = 'claims_paid_cumulative'
        ) p
            ON  u.lob_code = p.lob_code
            AND u.accident_year = p.accident_year
            AND p.rn = 1
        WHERE u.section = 'summary'
          AND u.cell_ref LIKE '%C0380'
          AND u.amount < p.amount
    ) violations
""").first()["cnt"]

check3_passed = (ultimate_vs_paid == 0)
dq_checks.append(("ultimate_gte_cumulative_paid", check3_passed))
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="ultimate_gte_cumulative_paid",
    check_category="business_rule",
    check_expression="Ultimate claims (C0380) >= latest cumulative paid for each AY/LoB",
    expected_value="0 violations",
    actual_value=f"{ultimate_vs_paid} violations",
    passed=check3_passed,
    severity="error",
    details="Ultimate claims should always be greater than or equal to the latest cumulative paid amount."
)

all_passed = all(c[1] for c in dq_checks)
print(f"\nData quality: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Lineage

# COMMAND ----------

lineage.log_lineage(
    step_name=step_name,
    step_sequence=22,
    source_tables=[source_triangle, source_ultimate],
    target_table=target_table,
    transformation_type="qrt_formatting",
    transformation_desc=(
        "Maps bronze_claims_triangles and silver_ultimate_claims to EIOPA S.19.01 "
        "'Non-Life Insurance Claims Information' QRT template. Long format with sections "
        "for cumulative claims paid triangle, RBNS triangle, and summary "
        "(total RBNS C0360, IBNR C0370, ultimate claims C0380). "
        "EIOPA cell references R0100-R0260 x C0010-C0170 for triangle, C0360-C0380 for summary."
    ),
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    columns_in=columns_in,
    columns_out=columns_out,
    parameters={
        "reporting_date": reporting_date,
        "reporting_period": reporting_period,
        "entity_lei": entity_lei,
        "min_accident_year": min_ay,
        "max_accident_year": max_ay,
    },
    status="success",
    data_quality_checks=[{"name": c[0], "passed": c[1]} for c in dq_checks],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Claims Paid Triangle for LoB 1

# COMMAND ----------

# Show pivot-like display of claims paid triangle for lob_code=1 (largest LoB)
print("=" * 90)
print("  S.19.01 — NON-LIFE INSURANCE CLAIMS INFORMATION")
print("=" * 90)
print(f"  Reporting date:  {reporting_date}")
print(f"  Entity LEI:      {entity_lei}")
print(f"  Target table:    {target_table}")
print(f"  Total rows:      {row_count_out:,}")
print("=" * 90)

# Section row counts
section_counts = spark.sql(f"""
    SELECT section, COUNT(*) AS cnt
    FROM {target_table}
    GROUP BY section
    ORDER BY section
""").collect()

print("\n  Rows by section:")
for r in section_counts:
    print(f"    {r['section']:<30s} {r['cnt']:>8,}")

# LoB row counts
lob_counts = spark.sql(f"""
    SELECT lob_code, lob_name, COUNT(*) AS cnt
    FROM {target_table}
    GROUP BY lob_code, lob_name
    ORDER BY lob_code
""").collect()

print(f"\n  Rows by LoB:")
for r in lob_counts:
    print(f"    LoB {r['lob_code']}: {r['lob_name']:<35s} {r['cnt']:>8,}")

# Pivot-like display for lob_code = 1 — claims paid cumulative
print(f"\n  Claims Paid Triangle (Cumulative, Gross) — LoB 1:")
print("-" * 90)

# Get max development year for column headers
max_dy_row = spark.sql(f"""
    SELECT MAX(development_year) AS max_dy
    FROM {target_table}
    WHERE section = 'claims_paid_cumulative' AND lob_code = 1
""").first()
max_dy = max_dy_row["max_dy"] if max_dy_row["max_dy"] is not None else 0

# Build header
header = f"  {'AY':<6}"
for dy in range(max_dy + 1):
    header += f"{'DY' + str(dy):>14}"
print(header)
print("  " + "-" * (6 + 14 * (max_dy + 1)))

# Collect triangle data
triangle_data = spark.sql(f"""
    SELECT accident_year, development_year, amount
    FROM {target_table}
    WHERE section = 'claims_paid_cumulative' AND lob_code = 1
    ORDER BY accident_year, development_year
""").collect()

# Organise into dict for display
from collections import defaultdict
triangle_dict = defaultdict(dict)
for r in triangle_data:
    triangle_dict[r["accident_year"]][r["development_year"]] = r["amount"]

for ay in sorted(triangle_dict.keys()):
    row_str = f"  {ay:<6}"
    for dy in range(max_dy + 1):
        if dy in triangle_dict[ay]:
            val = triangle_dict[ay][dy]
            row_str += f"{val / 1e6:>13.1f}M"
        else:
            row_str += f"{'':>14}"
    print(row_str)

print("-" * 90)
print(f"  Data quality: {'ALL PASSED' if all_passed else 'CHECKS FAILED — review above'}")
print("=" * 90)
print("  22_gold_s1901 complete.")
print("=" * 90)
