# Databricks notebook source
# MAGIC %md
# MAGIC # 06_gold_s0501 — EIOPA S.05.01 Non-Life Premiums, Claims and Expenses
# MAGIC
# MAGIC Reads the silver premiums, claims, and expenses tables and produces `gold_qrt_s0501` —
# MAGIC the S.05.01 QRT template in **long format** (one row per template-row / line-of-business combination).
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `reporting_reference_date` | Reporting reference date (e.g. 2025-12-31) |
# MAGIC | `reporting_entity_lei` | LEI of the reporting entity |
# MAGIC | `template_row_id` | EIOPA row reference (R0110 - R1300) |
# MAGIC | `template_row_label` | Human-readable row description |
# MAGIC | `lob_code` | Solvency II LoB code (0 = Total across all LoB) |
# MAGIC | `lob_label` | Line of business name |
# MAGIC | `amount_eur` | Amount in EUR |

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

# MAGIC %md
# MAGIC ## Initialise lineage logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("06_gold_s0501")

fqn = lambda table: f"{catalog}.{schema}.{table}"

target_table = "gold_qrt_s0501"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build S.05.01 QRT in long format
# MAGIC
# MAGIC Each template row maps to a specific metric from the silver layer.
# MAGIC We build the full template as a single SQL using UNION ALL — one SELECT per row ID.
# MAGIC A **Total** column (lob_code = 0) aggregates across all lines of business.

# COMMAND ----------

# --- Helper: generates one SELECT block for a premium/claims/expenses metric ---
# The silver tables store data per quarter; for the annual QRT we SUM all quarters
# within the reporting year using: WHERE reporting_period LIKE '{reporting_year}%'

s0501_sql = f"""
CREATE OR REPLACE TABLE {fqn(target_table)} AS

WITH premiums AS (
    SELECT
        lob_code,
        line_of_business,
        SUM(gross_premiums_written)   AS gross_premiums_written,
        SUM(gross_premiums_earned)    AS gross_premiums_earned,
        SUM(reinsurers_share_written) AS reinsurers_share_written,
        SUM(reinsurers_share_earned)  AS reinsurers_share_earned,
        SUM(net_premiums_written)     AS net_premiums_written,
        SUM(net_premiums_earned)      AS net_premiums_earned
    FROM {fqn('silver_premiums_by_lob')}
    WHERE reporting_period LIKE '{reporting_year}%'
    GROUP BY lob_code, line_of_business
),

claims AS (
    SELECT
        lob_code,
        line_of_business,
        SUM(gross_claims_incurred)       AS gross_claims_incurred,
        SUM(gross_claims_paid)           AS gross_claims_paid,
        SUM(reinsurers_share_incurred)   AS reinsurers_share_incurred,
        SUM(reinsurers_share_paid)       AS reinsurers_share_paid,
        SUM(net_claims_incurred)         AS net_claims_incurred,
        SUM(net_claims_paid)             AS net_claims_paid,
        SUM(gross_changes_in_provisions) AS gross_changes_in_provisions,
        SUM(net_changes_in_provisions)   AS net_changes_in_provisions
    FROM {fqn('silver_claims_by_lob')}
    WHERE reporting_period LIKE '{reporting_year}%'
    GROUP BY lob_code, line_of_business
),

expenses AS (
    SELECT
        lob_code,
        line_of_business,
        SUM(acquisition_expenses)          AS acquisition_expenses,
        SUM(administrative_expenses)       AS administrative_expenses,
        SUM(claims_management_expenses)    AS claims_management_expenses,
        SUM(overhead_expenses)             AS overhead_expenses,
        SUM(investment_management_expenses) AS investment_management_expenses,
        SUM(other_expenses)                AS other_expenses,
        SUM(total_expenses)                AS total_expenses
    FROM {fqn('silver_expenses_by_lob')}
    WHERE reporting_period LIKE '{reporting_year}%'
    GROUP BY lob_code, line_of_business
)

-- =========================================================================
-- R0110: Premiums written - Gross - Direct Business
-- =========================================================================
SELECT
    DATE('{reporting_date}') AS reporting_reference_date,
    '{entity_lei}'           AS reporting_entity_lei,
    'R0110'                  AS template_row_id,
    'Premiums written - Gross - Direct Business' AS template_row_label,
    lob_code,
    line_of_business         AS lob_label,
    gross_premiums_written   AS amount_eur
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0110',
       'Premiums written - Gross - Direct Business',
       0, 'Total', SUM(gross_premiums_written) FROM premiums

-- =========================================================================
-- R0140: Premiums written - Reinsurers' share
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0140',
       'Premiums written - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_written
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0140',
       'Premiums written - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_written) FROM premiums

-- =========================================================================
-- R0200: Premiums written - Net
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0200',
       'Premiums written - Net',
       lob_code, line_of_business, net_premiums_written
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0200',
       'Premiums written - Net',
       0, 'Total', SUM(net_premiums_written) FROM premiums

-- =========================================================================
-- R0210: Premiums earned - Gross - Direct Business
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0210',
       'Premiums earned - Gross - Direct Business',
       lob_code, line_of_business, gross_premiums_earned
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0210',
       'Premiums earned - Gross - Direct Business',
       0, 'Total', SUM(gross_premiums_earned) FROM premiums

-- =========================================================================
-- R0240: Premiums earned - Reinsurers' share
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0240',
       'Premiums earned - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_earned
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0240',
       'Premiums earned - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_earned) FROM premiums

-- =========================================================================
-- R0300: Premiums earned - Net
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0300',
       'Premiums earned - Net',
       lob_code, line_of_business, net_premiums_earned
FROM premiums

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0300',
       'Premiums earned - Net',
       0, 'Total', SUM(net_premiums_earned) FROM premiums

-- =========================================================================
-- R0310: Claims incurred - Gross - Direct Business
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0310',
       'Claims incurred - Gross - Direct Business',
       lob_code, line_of_business, gross_claims_incurred
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0310',
       'Claims incurred - Gross - Direct Business',
       0, 'Total', SUM(gross_claims_incurred) FROM claims

-- =========================================================================
-- R0340: Claims incurred - Reinsurers' share
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0340',
       'Claims incurred - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_incurred
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0340',
       'Claims incurred - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_incurred) FROM claims

-- =========================================================================
-- R0400: Claims incurred - Net
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0400',
       'Claims incurred - Net',
       lob_code, line_of_business, net_claims_incurred
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0400',
       'Claims incurred - Net',
       0, 'Total', SUM(net_claims_incurred) FROM claims

-- =========================================================================
-- R0410: Claims paid - Gross - Direct Business
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0410',
       'Claims paid - Gross - Direct Business',
       lob_code, line_of_business, gross_claims_paid
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0410',
       'Claims paid - Gross - Direct Business',
       0, 'Total', SUM(gross_claims_paid) FROM claims

-- =========================================================================
-- R0440: Claims paid - Reinsurers' share
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0440',
       'Claims paid - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_paid
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0440',
       'Claims paid - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_paid) FROM claims

-- =========================================================================
-- R0500: Claims paid - Net
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0500',
       'Claims paid - Net',
       lob_code, line_of_business, net_claims_paid
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0500',
       'Claims paid - Net',
       0, 'Total', SUM(net_claims_paid) FROM claims

-- =========================================================================
-- R0510: Changes in other technical provisions - Gross
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0510',
       'Changes in other technical provisions - Gross',
       lob_code, line_of_business, gross_changes_in_provisions
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0510',
       'Changes in other technical provisions - Gross',
       0, 'Total', SUM(gross_changes_in_provisions) FROM claims

-- =========================================================================
-- R0540: Changes in other technical provisions - Net
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0540',
       'Changes in other technical provisions - Net',
       lob_code, line_of_business, net_changes_in_provisions
FROM claims

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0540',
       'Changes in other technical provisions - Net',
       0, 'Total', SUM(net_changes_in_provisions) FROM claims

-- =========================================================================
-- R0550: Expenses incurred
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0550',
       'Expenses incurred',
       lob_code, line_of_business, total_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0550',
       'Expenses incurred',
       0, 'Total', SUM(total_expenses) FROM expenses

-- =========================================================================
-- R0610: Administrative expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0610',
       'Administrative expenses',
       lob_code, line_of_business, administrative_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0610',
       'Administrative expenses',
       0, 'Total', SUM(administrative_expenses) FROM expenses

-- =========================================================================
-- R0620: Investment management expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0620',
       'Investment management expenses',
       lob_code, line_of_business, investment_management_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0620',
       'Investment management expenses',
       0, 'Total', SUM(investment_management_expenses) FROM expenses

-- =========================================================================
-- R0630: Claims management expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0630',
       'Claims management expenses',
       lob_code, line_of_business, claims_management_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0630',
       'Claims management expenses',
       0, 'Total', SUM(claims_management_expenses) FROM expenses

-- =========================================================================
-- R0640: Acquisition expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0640',
       'Acquisition expenses',
       lob_code, line_of_business, acquisition_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0640',
       'Acquisition expenses',
       0, 'Total', SUM(acquisition_expenses) FROM expenses

-- =========================================================================
-- R0680: Overhead expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0680',
       'Overhead expenses',
       lob_code, line_of_business, overhead_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R0680',
       'Overhead expenses',
       0, 'Total', SUM(overhead_expenses) FROM expenses

-- =========================================================================
-- R1200: Other expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R1200',
       'Other expenses',
       lob_code, line_of_business, other_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R1200',
       'Other expenses',
       0, 'Total', SUM(other_expenses) FROM expenses

-- =========================================================================
-- R1300: Total expenses
-- =========================================================================
UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R1300',
       'Total expenses',
       lob_code, line_of_business, total_expenses
FROM expenses

UNION ALL
SELECT DATE('{reporting_date}'), '{entity_lei}', 'R1300',
       'Total expenses',
       0, 'Total', SUM(total_expenses) FROM expenses
"""

spark.sql(s0501_sql)

row_count = spark.table(fqn(target_table)).count()
print(f"{target_table}: {row_count} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column comments

# COMMAND ----------

column_comments = {
    "reporting_reference_date": "Solvency II reporting reference date (year-end)",
    "reporting_entity_lei": "Legal Entity Identifier (LEI) of the reporting undertaking",
    "template_row_id": "EIOPA S.05.01 row reference code (R0110 - R1300)",
    "template_row_label": "Human-readable description of the template row",
    "lob_code": "Solvency II line of business code per Annex I (0 = Total across all LoB)",
    "lob_label": "Line of business name (Total for the cross-LoB aggregate)",
    "amount_eur": "Monetary amount in EUR",
}

for col_name, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {fqn(target_table)} ALTER COLUMN {col_name} COMMENT '{comment}'")

print(f"Column comments applied to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

# Check 1: Row count > 0
lineage.log_data_quality(
    step_name="gold_qrt_s0501",
    table_name=fqn(target_table),
    check_name="row_count_positive",
    check_category="completeness",
    check_expression="COUNT(*) > 0",
    expected_value="> 0",
    actual_value=str(row_count),
    passed=row_count > 0,
    severity="error",
)

# Check 2: All expected template row IDs are present
expected_row_ids = [
    'R0110', 'R0140', 'R0200',
    'R0210', 'R0240', 'R0300',
    'R0310', 'R0340', 'R0400',
    'R0410', 'R0440', 'R0500',
    'R0510', 'R0540',
    'R0550', 'R0610', 'R0620', 'R0630', 'R0640', 'R0680',
    'R1200', 'R1300',
]

actual_row_ids = sorted([
    row["template_row_id"] for row in
    spark.sql(f"SELECT DISTINCT template_row_id FROM {fqn(target_table)}").collect()
])
missing_row_ids = set(expected_row_ids) - set(actual_row_ids)

lineage.log_data_quality(
    step_name="gold_qrt_s0501",
    table_name=fqn(target_table),
    check_name="all_template_row_ids_present",
    check_category="completeness",
    check_expression=f"DISTINCT template_row_id contains all {len(expected_row_ids)} expected IDs",
    expected_value=str(expected_row_ids),
    actual_value=str(actual_row_ids),
    passed=len(missing_row_ids) == 0,
    severity="error",
    details=f"Missing row IDs: {sorted(missing_row_ids)}" if missing_row_ids else None,
)

# Check 3: Total net premiums written > 0 (sanity check)
total_nwp = spark.sql(f"""
    SELECT amount_eur FROM {fqn(target_table)}
    WHERE template_row_id = 'R0200' AND lob_code = 0
""").first()
total_nwp_val = float(total_nwp["amount_eur"]) if total_nwp else 0.0

lineage.log_data_quality(
    step_name="gold_qrt_s0501",
    table_name=fqn(target_table),
    check_name="total_net_premiums_written_positive",
    check_category="validity",
    check_expression="R0200 (Net Premiums Written) Total > 0",
    expected_value="> 0",
    actual_value=f"{total_nwp_val:,.2f}",
    passed=total_nwp_val > 0,
    severity="error",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage

# COMMAND ----------

source_tables = [
    fqn("silver_premiums_by_lob"),
    fqn("silver_claims_by_lob"),
    fqn("silver_expenses_by_lob"),
]

# Count source rows across all three silver tables
source_row_count = sum(
    spark.table(t).count() for t in source_tables
)

lineage.log_lineage(
    step_name="gold_qrt_s0501",
    step_sequence=1,
    source_tables=source_tables,
    target_table=fqn(target_table),
    transformation_type="qrt_formatting",
    transformation_desc=(
        "Transform silver premiums, claims, and expenses tables into EIOPA S.05.01 "
        "Non-Life Premiums, Claims and Expenses QRT template in long format. "
        "Aggregates quarterly data to annual totals and adds cross-LoB Total rows."
    ),
    row_count_in=source_row_count,
    row_count_out=row_count,
    columns_out=list(column_comments.keys()),
    parameters={
        "catalog": catalog,
        "schema": schema,
        "reporting_date": reporting_date,
        "reporting_year": reporting_year,
        "entity_lei": entity_lei,
    },
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 90)
print("GOLD S.05.01 — NON-LIFE PREMIUMS, CLAIMS AND EXPENSES — SUMMARY")
print("=" * 90)

# Fetch key totals (lob_code = 0 is the Total column)
summary = spark.sql(f"""
    SELECT template_row_id, template_row_label, amount_eur
    FROM {fqn(target_table)}
    WHERE lob_code = 0
    ORDER BY template_row_id
""").collect()

summary_map = {row["template_row_id"]: row for row in summary}

total_gwp        = float(summary_map["R0110"]["amount_eur"]) if "R0110" in summary_map else 0.0
total_nwp        = float(summary_map["R0200"]["amount_eur"]) if "R0200" in summary_map else 0.0
total_npe        = float(summary_map["R0300"]["amount_eur"]) if "R0300" in summary_map else 0.0
total_net_claims = float(summary_map["R0400"]["amount_eur"]) if "R0400" in summary_map else 0.0
total_expenses   = float(summary_map["R1300"]["amount_eur"]) if "R1300" in summary_map else 0.0

print(f"\n  Total GWP (R0110):          {total_gwp:>18,.2f} EUR")
print(f"  Total Net Premiums (R0200): {total_nwp:>18,.2f} EUR")
print(f"  Total Net Claims (R0400):   {total_net_claims:>18,.2f} EUR")
print(f"  Total Expenses (R1300):     {total_expenses:>18,.2f} EUR")

if total_npe > 0:
    combined_ratio = ((total_net_claims + total_expenses) / total_npe) * 100
    print(f"\n  Combined Ratio (Net Claims + Expenses) / Net Premiums Earned:")
    print(f"    ({total_net_claims:,.2f} + {total_expenses:,.2f}) / {total_npe:,.2f} = {combined_ratio:.1f}%")
else:
    print("\n  Combined Ratio: N/A (net premiums earned = 0)")

print(f"\n  Template rows: {len(expected_row_ids)} distinct IDs")
print(f"  Total records: {row_count}")

print("\n--- All Total-column values ---")
for row in summary:
    print(f"  {row['template_row_id']}  {row['template_row_label']:<52}  {float(row['amount_eur']):>18,.2f} EUR")

print("\n" + "=" * 90)
print(f"Table written: {fqn(target_table)} ({row_count} rows)")
print("=" * 90)
