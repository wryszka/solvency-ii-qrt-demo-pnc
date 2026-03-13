# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver_premiums_claims_expenses - Aggregate premiums, claims, and expenses by line of business
# MAGIC
# MAGIC Reads bronze premium, claims, expense, and policy tables and produces three aggregated silver tables
# MAGIC for S.05.01 (Non-Life Premiums, Claims, and Expenses) reporting:
# MAGIC
# MAGIC | Target Table | Description |
# MAGIC |---|---|
# MAGIC | `silver_premiums_by_lob` | Gross/net/reinsurer premiums written and earned by LoB |
# MAGIC | `silver_claims_by_lob` | Gross/net/reinsurer claims incurred, paid, and reserve changes by LoB |
# MAGIC | `silver_expenses_by_lob` | Expense breakdown by category and LoB |

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_classic_aws_us_catalog")
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
# MAGIC ## Initialise lineage logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("03_silver_premiums_claims_expenses")

fqn = lambda table: f"{catalog}.{schema}.{table}"

expected_lob_codes = [1, 2, 4, 5, 7, 8, 12]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Premiums by LoB

# COMMAND ----------

lineage.start_step("silver_premiums_by_lob")

premiums_sql = f"""
SELECT
  p.reporting_period,
  pol.lob_code,
  pol.line_of_business,
  SUM(CASE WHEN p.transaction_type = 'written' THEN p.gross_amount_eur ELSE 0 END) as gross_premiums_written,
  SUM(CASE WHEN p.transaction_type = 'earned' THEN p.gross_amount_eur ELSE 0 END) as gross_premiums_earned,
  SUM(CASE WHEN p.transaction_type = 'written' THEN p.reinsurance_amount_eur ELSE 0 END) as reinsurers_share_written,
  SUM(CASE WHEN p.transaction_type = 'earned' THEN p.reinsurance_amount_eur ELSE 0 END) as reinsurers_share_earned,
  SUM(CASE WHEN p.transaction_type = 'written' THEN p.net_amount_eur ELSE 0 END) as net_premiums_written,
  SUM(CASE WHEN p.transaction_type = 'earned' THEN p.net_amount_eur ELSE 0 END) as net_premiums_earned
FROM {schema}.bronze_premiums_transactions p
JOIN {schema}.bronze_policies pol ON p.policy_id = pol.policy_id
WHERE p.transaction_type IN ('written', 'earned')
GROUP BY p.reporting_period, pol.lob_code, pol.line_of_business
ORDER BY p.reporting_period, pol.lob_code
"""

df_premiums = spark.sql(premiums_sql)
df_premiums.write.format("delta").mode("overwrite").saveAsTable(fqn("silver_premiums_by_lob"))

premiums_count = spark.table(fqn("silver_premiums_by_lob")).count()
print(f"silver_premiums_by_lob: {premiums_count} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column comments for silver_premiums_by_lob

# COMMAND ----------

premium_column_comments = {
    "reporting_period": "Reporting period in YYYY-QN format (e.g. 2025-Q4)",
    "lob_code": "Solvency II line of business code per Annex I",
    "line_of_business": "Solvency II line of business name",
    "gross_premiums_written": "Total gross premiums written in EUR",
    "gross_premiums_earned": "Total gross premiums earned in EUR",
    "reinsurers_share_written": "Reinsurers share of premiums written in EUR",
    "reinsurers_share_earned": "Reinsurers share of premiums earned in EUR",
    "net_premiums_written": "Net premiums written (gross minus reinsurers share) in EUR",
    "net_premiums_earned": "Net premiums earned (gross minus reinsurers share) in EUR",
}

for col_name, comment in premium_column_comments.items():
    spark.sql(f"ALTER TABLE {fqn('silver_premiums_by_lob')} ALTER COLUMN {col_name} COMMENT '{comment}'")

print("Column comments applied to silver_premiums_by_lob")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality checks — premiums

# COMMAND ----------

# Check 1: Row count > 0
lineage.log_data_quality(
    step_name="silver_premiums_by_lob",
    table_name=fqn("silver_premiums_by_lob"),
    check_name="row_count_positive",
    check_category="completeness",
    check_expression="COUNT(*) > 0",
    expected_value="> 0",
    actual_value=str(premiums_count),
    passed=premiums_count > 0,
    severity="error",
)

# Check 2: Total gross written premium > 0
gwp_total = spark.sql(f"SELECT SUM(gross_premiums_written) as total FROM {fqn('silver_premiums_by_lob')}").first()["total"]
lineage.log_data_quality(
    step_name="silver_premiums_by_lob",
    table_name=fqn("silver_premiums_by_lob"),
    check_name="total_gwp_positive",
    check_category="validity",
    check_expression="SUM(gross_premiums_written) > 0",
    expected_value="> 0",
    actual_value=f"{gwp_total:,.2f}",
    passed=gwp_total is not None and gwp_total > 0,
    severity="error",
)

# Check 3: All expected LoB codes present
actual_lob_codes = sorted([
    row["lob_code"] for row in
    spark.sql(f"SELECT DISTINCT lob_code FROM {fqn('silver_premiums_by_lob')}").collect()
])
missing_lobs = set(expected_lob_codes) - set(actual_lob_codes)
lineage.log_data_quality(
    step_name="silver_premiums_by_lob",
    table_name=fqn("silver_premiums_by_lob"),
    check_name="all_lob_codes_present",
    check_category="completeness",
    check_expression=f"DISTINCT lob_code contains {expected_lob_codes}",
    expected_value=str(expected_lob_codes),
    actual_value=str(actual_lob_codes),
    passed=len(missing_lobs) == 0,
    severity="error",
    details=f"Missing LoB codes: {sorted(missing_lobs)}" if missing_lobs else None,
)

# Check 4: Net = Gross - Reinsurance (spot check on written premiums)
reconciliation = spark.sql(f"""
    SELECT
        SUM(gross_premiums_written) as total_gross,
        SUM(reinsurers_share_written) as total_reins,
        SUM(net_premiums_written) as total_net,
        ABS(SUM(net_premiums_written) - (SUM(gross_premiums_written) - SUM(reinsurers_share_written))) as diff
    FROM {fqn('silver_premiums_by_lob')}
""").first()
recon_diff = float(reconciliation["diff"])
lineage.log_data_quality(
    step_name="silver_premiums_by_lob",
    table_name=fqn("silver_premiums_by_lob"),
    check_name="net_equals_gross_minus_reinsurance_written",
    check_category="consistency",
    check_expression="ABS(SUM(net) - (SUM(gross) - SUM(reinsurance))) < 0.01",
    expected_value="< 0.01",
    actual_value=f"{recon_diff:,.4f}",
    passed=recon_diff < 0.01,
    severity="error",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage — premiums

# COMMAND ----------

# Count source rows
premiums_source_count = spark.table(fqn("bronze_premiums_transactions")).count()
policies_source_count = spark.table(fqn("bronze_policies")).count()

lineage.log_lineage(
    step_name="silver_premiums_by_lob",
    step_sequence=1,
    source_tables=[fqn("bronze_premiums_transactions"), fqn("bronze_policies")],
    target_table=fqn("silver_premiums_by_lob"),
    transformation_type="aggregation",
    transformation_desc="Aggregate bronze premiums by LoB and reporting period. Join with policies for LoB classification. Pivot transaction types (written/earned) into separate columns for gross, reinsurance, and net amounts.",
    row_count_in=premiums_source_count + policies_source_count,
    row_count_out=premiums_count,
    columns_out=list(premium_column_comments.keys()),
    parameters={"catalog": catalog, "schema": schema, "reporting_period": reporting_period},
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Claims by LoB

# COMMAND ----------

lineage.start_step("silver_claims_by_lob")

claims_sql = f"""
SELECT
  c.reporting_period,
  pol.lob_code,
  pol.line_of_business,
  SUM(CASE WHEN c.transaction_type = 'incurred' THEN c.gross_amount_eur ELSE 0 END) as gross_claims_incurred,
  SUM(CASE WHEN c.transaction_type = 'paid' THEN c.gross_amount_eur ELSE 0 END) as gross_claims_paid,
  SUM(CASE WHEN c.transaction_type = 'incurred' THEN c.reinsurance_recovery_eur ELSE 0 END) as reinsurers_share_incurred,
  SUM(CASE WHEN c.transaction_type = 'paid' THEN c.reinsurance_recovery_eur ELSE 0 END) as reinsurers_share_paid,
  SUM(CASE WHEN c.transaction_type = 'incurred' THEN c.net_amount_eur ELSE 0 END) as net_claims_incurred,
  SUM(CASE WHEN c.transaction_type = 'paid' THEN c.net_amount_eur ELSE 0 END) as net_claims_paid,
  SUM(CASE WHEN c.transaction_type = 'reserve_change' THEN c.gross_amount_eur ELSE 0 END) as gross_changes_in_provisions,
  SUM(CASE WHEN c.transaction_type = 'reserve_change' THEN c.net_amount_eur ELSE 0 END) as net_changes_in_provisions
FROM {schema}.bronze_claims_transactions c
JOIN {schema}.bronze_policies pol ON c.policy_id = pol.policy_id
GROUP BY c.reporting_period, pol.lob_code, pol.line_of_business
ORDER BY c.reporting_period, pol.lob_code
"""

df_claims = spark.sql(claims_sql)
df_claims.write.format("delta").mode("overwrite").saveAsTable(fqn("silver_claims_by_lob"))

claims_count = spark.table(fqn("silver_claims_by_lob")).count()
print(f"silver_claims_by_lob: {claims_count} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column comments for silver_claims_by_lob

# COMMAND ----------

claims_column_comments = {
    "reporting_period": "Reporting period in YYYY-QN format (e.g. 2025-Q4)",
    "lob_code": "Solvency II line of business code per Annex I",
    "line_of_business": "Solvency II line of business name",
    "gross_claims_incurred": "Total gross claims incurred in EUR",
    "gross_claims_paid": "Total gross claims paid in EUR",
    "reinsurers_share_incurred": "Reinsurers share of claims incurred in EUR",
    "reinsurers_share_paid": "Reinsurers share of claims paid in EUR",
    "net_claims_incurred": "Net claims incurred (gross minus reinsurers share) in EUR",
    "net_claims_paid": "Net claims paid (gross minus reinsurers share) in EUR",
    "gross_changes_in_provisions": "Gross changes in claims provisions (reserve movements) in EUR",
    "net_changes_in_provisions": "Net changes in claims provisions (reserve movements) in EUR",
}

for col_name, comment in claims_column_comments.items():
    spark.sql(f"ALTER TABLE {fqn('silver_claims_by_lob')} ALTER COLUMN {col_name} COMMENT '{comment}'")

print("Column comments applied to silver_claims_by_lob")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality checks — claims

# COMMAND ----------

# Check 1: Row count > 0
lineage.log_data_quality(
    step_name="silver_claims_by_lob",
    table_name=fqn("silver_claims_by_lob"),
    check_name="row_count_positive",
    check_category="completeness",
    check_expression="COUNT(*) > 0",
    expected_value="> 0",
    actual_value=str(claims_count),
    passed=claims_count > 0,
    severity="error",
)

# Check 2: Total gross claims incurred > 0
gci_total = spark.sql(f"SELECT SUM(gross_claims_incurred) as total FROM {fqn('silver_claims_by_lob')}").first()["total"]
lineage.log_data_quality(
    step_name="silver_claims_by_lob",
    table_name=fqn("silver_claims_by_lob"),
    check_name="total_gross_claims_incurred_positive",
    check_category="validity",
    check_expression="SUM(gross_claims_incurred) > 0",
    expected_value="> 0",
    actual_value=f"{gci_total:,.2f}",
    passed=gci_total is not None and gci_total > 0,
    severity="error",
)

# Check 3: All expected LoB codes present
actual_claims_lob_codes = sorted([
    row["lob_code"] for row in
    spark.sql(f"SELECT DISTINCT lob_code FROM {fqn('silver_claims_by_lob')}").collect()
])
missing_claims_lobs = set(expected_lob_codes) - set(actual_claims_lob_codes)
lineage.log_data_quality(
    step_name="silver_claims_by_lob",
    table_name=fqn("silver_claims_by_lob"),
    check_name="all_lob_codes_present",
    check_category="completeness",
    check_expression=f"DISTINCT lob_code contains {expected_lob_codes}",
    expected_value=str(expected_lob_codes),
    actual_value=str(actual_claims_lob_codes),
    passed=len(missing_claims_lobs) == 0,
    severity="error",
    details=f"Missing LoB codes: {sorted(missing_claims_lobs)}" if missing_claims_lobs else None,
)

# Check 4: Net = Gross - Reinsurance (spot check on incurred claims)
claims_recon = spark.sql(f"""
    SELECT
        ABS(SUM(net_claims_incurred) - (SUM(gross_claims_incurred) - SUM(reinsurers_share_incurred))) as diff
    FROM {fqn('silver_claims_by_lob')}
""").first()
claims_recon_diff = float(claims_recon["diff"])
lineage.log_data_quality(
    step_name="silver_claims_by_lob",
    table_name=fqn("silver_claims_by_lob"),
    check_name="net_equals_gross_minus_reinsurance_incurred",
    check_category="consistency",
    check_expression="ABS(SUM(net) - (SUM(gross) - SUM(reinsurance))) < 0.01",
    expected_value="< 0.01",
    actual_value=f"{claims_recon_diff:,.4f}",
    passed=claims_recon_diff < 0.01,
    severity="error",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage — claims

# COMMAND ----------

claims_source_count = spark.table(fqn("bronze_claims_transactions")).count()

lineage.log_lineage(
    step_name="silver_claims_by_lob",
    step_sequence=2,
    source_tables=[fqn("bronze_claims_transactions"), fqn("bronze_policies")],
    target_table=fqn("silver_claims_by_lob"),
    transformation_type="aggregation",
    transformation_desc="Aggregate bronze claims by LoB and reporting period. Join with policies for LoB classification. Pivot transaction types (incurred/paid/reserve_change) into separate columns for gross, reinsurance, and net amounts.",
    row_count_in=claims_source_count + policies_source_count,
    row_count_out=claims_count,
    columns_out=list(claims_column_comments.keys()),
    parameters={"catalog": catalog, "schema": schema, "reporting_period": reporting_period},
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Expenses by LoB

# COMMAND ----------

lineage.start_step("silver_expenses_by_lob")

expenses_sql = f"""
SELECT
  e.reporting_period,
  e.lob_code,
  e.line_of_business,
  SUM(CASE WHEN e.expense_category = 'acquisition' THEN e.gross_amount_eur ELSE 0 END) as acquisition_expenses,
  SUM(CASE WHEN e.expense_category = 'administrative' THEN e.gross_amount_eur ELSE 0 END) as administrative_expenses,
  SUM(CASE WHEN e.expense_category = 'claims_management' THEN e.gross_amount_eur ELSE 0 END) as claims_management_expenses,
  SUM(CASE WHEN e.expense_category = 'overhead' THEN e.gross_amount_eur ELSE 0 END) as overhead_expenses,
  SUM(CASE WHEN e.expense_category = 'investment_management' THEN e.gross_amount_eur ELSE 0 END) as investment_management_expenses,
  SUM(CASE WHEN e.expense_category = 'other' THEN e.gross_amount_eur ELSE 0 END) as other_expenses,
  SUM(e.gross_amount_eur) as total_expenses
FROM {schema}.bronze_expenses e
GROUP BY e.reporting_period, e.lob_code, e.line_of_business
ORDER BY e.reporting_period, e.lob_code
"""

df_expenses = spark.sql(expenses_sql)
df_expenses.write.format("delta").mode("overwrite").saveAsTable(fqn("silver_expenses_by_lob"))

expenses_count = spark.table(fqn("silver_expenses_by_lob")).count()
print(f"silver_expenses_by_lob: {expenses_count} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column comments for silver_expenses_by_lob

# COMMAND ----------

expenses_column_comments = {
    "reporting_period": "Reporting period in YYYY-QN format (e.g. 2025-Q4)",
    "lob_code": "Solvency II line of business code per Annex I",
    "line_of_business": "Solvency II line of business name",
    "acquisition_expenses": "Acquisition expenses (commissions, brokerage, marketing) in EUR",
    "administrative_expenses": "Administrative expenses (staff, IT, premises) in EUR",
    "claims_management_expenses": "Claims management and handling expenses in EUR",
    "overhead_expenses": "Overhead and general operating expenses in EUR",
    "investment_management_expenses": "Investment management expenses in EUR",
    "other_expenses": "Other miscellaneous expenses in EUR",
    "total_expenses": "Total of all expense categories in EUR",
}

for col_name, comment in expenses_column_comments.items():
    spark.sql(f"ALTER TABLE {fqn('silver_expenses_by_lob')} ALTER COLUMN {col_name} COMMENT '{comment}'")

print("Column comments applied to silver_expenses_by_lob")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality checks — expenses

# COMMAND ----------

# Check 1: Row count > 0
lineage.log_data_quality(
    step_name="silver_expenses_by_lob",
    table_name=fqn("silver_expenses_by_lob"),
    check_name="row_count_positive",
    check_category="completeness",
    check_expression="COUNT(*) > 0",
    expected_value="> 0",
    actual_value=str(expenses_count),
    passed=expenses_count > 0,
    severity="error",
)

# Check 2: All expected LoB codes present
actual_expenses_lob_codes = sorted([
    row["lob_code"] for row in
    spark.sql(f"SELECT DISTINCT lob_code FROM {fqn('silver_expenses_by_lob')}").collect()
])
missing_expenses_lobs = set(expected_lob_codes) - set(actual_expenses_lob_codes)
lineage.log_data_quality(
    step_name="silver_expenses_by_lob",
    table_name=fqn("silver_expenses_by_lob"),
    check_name="all_lob_codes_present",
    check_category="completeness",
    check_expression=f"DISTINCT lob_code contains {expected_lob_codes}",
    expected_value=str(expected_lob_codes),
    actual_value=str(actual_expenses_lob_codes),
    passed=len(missing_expenses_lobs) == 0,
    severity="error",
    details=f"Missing LoB codes: {sorted(missing_expenses_lobs)}" if missing_expenses_lobs else None,
)

# Check 3: Total expenses > 0
total_exp = spark.sql(f"SELECT SUM(total_expenses) as total FROM {fqn('silver_expenses_by_lob')}").first()["total"]
lineage.log_data_quality(
    step_name="silver_expenses_by_lob",
    table_name=fqn("silver_expenses_by_lob"),
    check_name="total_expenses_positive",
    check_category="validity",
    check_expression="SUM(total_expenses) > 0",
    expected_value="> 0",
    actual_value=f"{total_exp:,.2f}",
    passed=total_exp is not None and total_exp > 0,
    severity="error",
)

# Check 4: Category totals sum to total_expenses
expense_recon = spark.sql(f"""
    SELECT ABS(
        SUM(total_expenses) - (
            SUM(acquisition_expenses) + SUM(administrative_expenses) +
            SUM(claims_management_expenses) + SUM(overhead_expenses) +
            SUM(investment_management_expenses) + SUM(other_expenses)
        )
    ) as diff
    FROM {fqn('silver_expenses_by_lob')}
""").first()
expense_recon_diff = float(expense_recon["diff"])
lineage.log_data_quality(
    step_name="silver_expenses_by_lob",
    table_name=fqn("silver_expenses_by_lob"),
    check_name="category_totals_equal_total_expenses",
    check_category="consistency",
    check_expression="ABS(SUM(total) - SUM(categories)) < 0.01",
    expected_value="< 0.01",
    actual_value=f"{expense_recon_diff:,.4f}",
    passed=expense_recon_diff < 0.01,
    severity="error",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage — expenses

# COMMAND ----------

expenses_source_count = spark.table(fqn("bronze_expenses")).count()

lineage.log_lineage(
    step_name="silver_expenses_by_lob",
    step_sequence=3,
    source_tables=[fqn("bronze_expenses")],
    target_table=fqn("silver_expenses_by_lob"),
    transformation_type="aggregation",
    transformation_desc="Aggregate bronze expenses by LoB and reporting period. Pivot expense categories (acquisition/administrative/claims_management/overhead/investment_management/other) into separate columns with a total.",
    row_count_in=expenses_source_count,
    row_count_out=expenses_count,
    columns_out=list(expenses_column_comments.keys()),
    parameters={"catalog": catalog, "schema": schema, "reporting_period": reporting_period},
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 90)
print("SILVER PREMIUMS, CLAIMS & EXPENSES — SUMMARY")
print("=" * 90)

# GWP by LoB
print("\n--- Gross Written Premiums by LoB ---")
gwp_by_lob = spark.sql(f"""
    SELECT lob_code, line_of_business,
           ROUND(gross_premiums_written, 2) as gwp_eur,
           ROUND(net_premiums_written, 2) as nwp_eur
    FROM {fqn('silver_premiums_by_lob')}
    WHERE reporting_period = '{reporting_period}'
    ORDER BY lob_code
""").collect()

for row in gwp_by_lob:
    print(f"  LoB {row['lob_code']:>2} ({row['line_of_business']:<30}): GWP = {row['gwp_eur']:>15,.2f} EUR | NWP = {row['nwp_eur']:>15,.2f} EUR")

# Total claims
print("\n--- Gross Claims Incurred by LoB ---")
claims_by_lob = spark.sql(f"""
    SELECT lob_code, line_of_business,
           ROUND(gross_claims_incurred, 2) as gci_eur,
           ROUND(net_claims_incurred, 2) as nci_eur
    FROM {fqn('silver_claims_by_lob')}
    WHERE reporting_period = '{reporting_period}'
    ORDER BY lob_code
""").collect()

for row in claims_by_lob:
    print(f"  LoB {row['lob_code']:>2} ({row['line_of_business']:<30}): GCI = {row['gci_eur']:>15,.2f} EUR | NCI = {row['nci_eur']:>15,.2f} EUR")

# Combined ratio by LoB
print("\n--- Combined Ratio by LoB (Net Claims Incurred + Total Expenses) / Net Premiums Earned ---")
combined_ratio = spark.sql(f"""
    SELECT
        p.lob_code,
        p.line_of_business,
        p.net_premiums_earned,
        c.net_claims_incurred,
        e.total_expenses,
        CASE WHEN p.net_premiums_earned > 0
             THEN ROUND(((c.net_claims_incurred + e.total_expenses) / p.net_premiums_earned) * 100, 1)
             ELSE NULL
        END as combined_ratio_pct
    FROM {fqn('silver_premiums_by_lob')} p
    JOIN {fqn('silver_claims_by_lob')} c
        ON p.lob_code = c.lob_code AND p.reporting_period = c.reporting_period
    JOIN {fqn('silver_expenses_by_lob')} e
        ON p.lob_code = e.lob_code AND p.reporting_period = e.reporting_period
    WHERE p.reporting_period = '{reporting_period}'
    ORDER BY p.lob_code
""").collect()

for row in combined_ratio:
    ratio_str = f"{row['combined_ratio_pct']}%" if row['combined_ratio_pct'] is not None else "N/A"
    print(f"  LoB {row['lob_code']:>2} ({row['line_of_business']:<30}): Combined Ratio = {ratio_str:>8}")

# Grand totals
print("\n--- Grand Totals ---")
totals = spark.sql(f"""
    SELECT
        ROUND(SUM(p.gross_premiums_written), 2) as total_gwp,
        ROUND(SUM(p.net_premiums_earned), 2) as total_npe,
        ROUND(SUM(c.gross_claims_incurred), 2) as total_gci,
        ROUND(SUM(c.net_claims_incurred), 2) as total_nci,
        ROUND(SUM(e.total_expenses), 2) as total_expenses
    FROM {fqn('silver_premiums_by_lob')} p
    JOIN {fqn('silver_claims_by_lob')} c
        ON p.lob_code = c.lob_code AND p.reporting_period = c.reporting_period
    JOIN {fqn('silver_expenses_by_lob')} e
        ON p.lob_code = e.lob_code AND p.reporting_period = e.reporting_period
    WHERE p.reporting_period = '{reporting_period}'
""").first()

print(f"  Total GWP:              {totals['total_gwp']:>18,.2f} EUR")
print(f"  Total Net Premiums Earned: {totals['total_npe']:>15,.2f} EUR")
print(f"  Total Gross Claims:     {totals['total_gci']:>18,.2f} EUR")
print(f"  Total Net Claims:       {totals['total_nci']:>18,.2f} EUR")
print(f"  Total Expenses:         {totals['total_expenses']:>18,.2f} EUR")

if totals['total_npe'] and totals['total_npe'] > 0:
    overall_combined = ((totals['total_nci'] + totals['total_expenses']) / totals['total_npe']) * 100
    print(f"  Overall Combined Ratio: {overall_combined:>17.1f} %")

print("\n" + "=" * 90)
print(f"Tables written: {premiums_count} premium rows, {claims_count} claims rows, {expenses_count} expense rows")
print("=" * 90)
