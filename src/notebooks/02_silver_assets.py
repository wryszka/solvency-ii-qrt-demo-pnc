# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_assets — Cleanse and enrich asset data with CIC decomposition and SII valuation
# MAGIC
# MAGIC Reads `bronze_assets` and produces `silver_assets_enriched`: a cleansed, enriched version of the
# MAGIC investment register with CIC decomposition, Solvency II valuation, derived risk flags, and data
# MAGIC quality indicators.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC 1. CIC code decomposition (country + category)
# MAGIC 2. Asset class mapping from CIC category
# MAGIC 3. SII valuation (market value = SII value for this demo)
# MAGIC 4. Unit price calculations (price per unit, % of par)
# MAGIC 5. Risk classification flags (fixed income, equity type 1/2)
# MAGIC 6. Data quality flag per row
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage` and `audit_data_quality_log`.

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_classic_aws_us_catalog")
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_date", "2025-12-31")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")

# Derive reporting period (YYYY-QN) from reporting_date
from datetime import datetime
_rd = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period = f"{_rd.year}-Q{(_rd.month - 1) // 3 + 1}"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Reporting period: {reporting_period}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise Lineage Logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)

source_table = f"{catalog}.{schema}.bronze_assets"
target_table = f"{catalog}.{schema}.silver_assets_enriched"
step_name = "silver_assets_enrichment"

lineage.start_step(step_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

df_bronze = spark.table(source_table)
row_count_in = df_bronze.count()
columns_in = df_bronze.columns

print(f"bronze_assets: {row_count_in} rows, {len(columns_in)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Temporary View for SQL Transformations

# COMMAND ----------

df_bronze.createOrReplaceTempView("v_bronze_assets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichment Transformations (SQL)
# MAGIC
# MAGIC All transformations are expressed in SQL for readability by insurance SMEs:
# MAGIC - CIC decomposition
# MAGIC - Asset class mapping
# MAGIC - SII valuation
# MAGIC - Unit price calculations
# MAGIC - Risk classification flags
# MAGIC - Data quality flag

# COMMAND ----------

df_enriched = spark.sql("""
SELECT
    -- =====================================================================
    -- Original columns (all kept)
    -- =====================================================================
    ba.*,

    -- =====================================================================
    -- 1. CIC decomposition
    -- =====================================================================
    SUBSTRING(ba.cic_code, 1, 2) AS cic_country,
    SUBSTRING(ba.cic_code, 3, 2) AS cic_category,

    -- =====================================================================
    -- 2. Asset class mapping from CIC category
    -- =====================================================================
    CASE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 11 AND 13
            THEN 'Government bonds'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 21 AND 23
            THEN 'Corporate bonds'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 31 AND 33
            THEN 'Equity (listed/unlisted)'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 41 AND 46
            THEN 'Collective investment undertakings'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 51 AND 54
            THEN 'Structured notes'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) = 61
            THEN 'Collateralised securities'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 71 AND 79
            THEN 'Cash and deposits'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 81 AND 85
            THEN 'Mortgages and loans'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 91 AND 99
            THEN 'Property'
        ELSE 'Other / unclassified'
    END AS sii_asset_class,

    -- =====================================================================
    -- 3. SII valuation (market value = SII value for this demo)
    -- =====================================================================
    ba.market_value_eur AS sii_value,

    -- =====================================================================
    -- 4. Unit price calculations
    -- =====================================================================
    ba.market_value_eur / NULLIF(ba.par_value, 0) AS unit_sii_price,
    ba.market_value_eur / NULLIF(ba.par_value, 0) * 100 AS unit_percentage_par,

    -- =====================================================================
    -- 5. Derived risk classification flags
    -- =====================================================================
    CASE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 11 AND 13 THEN TRUE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 21 AND 23 THEN TRUE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 51 AND 54 THEN TRUE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) = 61             THEN TRUE
        ELSE FALSE
    END AS is_fixed_income,

    CASE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) = 31 THEN TRUE
        ELSE FALSE
    END AS is_equity_type1,

    CASE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) IN (32, 33)      THEN TRUE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 41 AND 46 THEN TRUE
        ELSE FALSE
    END AS is_equity_type2,

    -- =====================================================================
    -- 6. Data quality flag
    -- =====================================================================
    CASE
        WHEN ba.asset_id IS NULL
          OR ba.cic_code IS NULL
          OR ba.market_value_eur IS NULL
          OR ba.issuer_name IS NULL
          OR ba.currency IS NULL
          OR ba.par_value IS NULL
        THEN 'WARN'
        ELSE 'PASS'
    END AS dq_flag

FROM v_bronze_assets ba
""")

row_count_out = df_enriched.count()
columns_out = df_enriched.columns

print(f"silver_assets_enriched: {row_count_out} rows, {len(columns_out)} columns")
print(f"New columns added: {[c for c in columns_out if c not in columns_in]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Register enriched view for DQ checks
df_enriched.createOrReplaceTempView("v_silver_assets_enriched")

# --- Check 1: Null asset_id ---
null_asset_ids = spark.sql("SELECT COUNT(*) AS cnt FROM v_silver_assets_enriched WHERE asset_id IS NULL").first()["cnt"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="asset_id_not_null",
    check_category="completeness",
    check_expression="COUNT(*) WHERE asset_id IS NULL",
    expected_value="0",
    actual_value=str(null_asset_ids),
    passed=(null_asset_ids == 0),
    severity="error",
    details="Primary key asset_id must not be null"
)

# --- Check 2: Null cic_code ---
null_cic = spark.sql("SELECT COUNT(*) AS cnt FROM v_silver_assets_enriched WHERE cic_code IS NULL").first()["cnt"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="cic_code_not_null",
    check_category="completeness",
    check_expression="COUNT(*) WHERE cic_code IS NULL",
    expected_value="0",
    actual_value=str(null_cic),
    passed=(null_cic == 0),
    severity="error",
    details="CIC code is required for all assets per EIOPA reporting"
)

# --- Check 3: market_value_eur > 0 ---
non_positive_mv = spark.sql("SELECT COUNT(*) AS cnt FROM v_silver_assets_enriched WHERE market_value_eur <= 0 OR market_value_eur IS NULL").first()["cnt"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="market_value_positive",
    check_category="validity",
    check_expression="COUNT(*) WHERE market_value_eur <= 0 OR IS NULL",
    expected_value="0",
    actual_value=str(non_positive_mv),
    passed=(non_positive_mv == 0),
    severity="warning",
    details="All assets should have a positive market value"
)

# --- Check 4: Total market value reconciliation ---
total_sii_value = spark.sql("SELECT COALESCE(SUM(sii_value), 0) AS total FROM v_silver_assets_enriched").first()["total"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="total_sii_value_reconciliation",
    check_category="reconciliation",
    check_expression="SUM(sii_value)",
    expected_value="logged_for_audit",
    actual_value=f"{total_sii_value:,.2f}",
    passed=True,
    severity="info",
    details=f"Total SII value of all assets: EUR {total_sii_value:,.2f}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Table

# COMMAND ----------

(
    df_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Table {target_table} written successfully ({row_count_out} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Column Comments (Unity Catalog Governance)

# COMMAND ----------

COLUMN_COMMENTS = {
    # Original bronze columns
    "asset_id":             "Unique internal asset identifier (e.g. A000001). Primary key.",
    "asset_name":           "Descriptive name of the asset holding.",
    "issuer_name":          "Legal name of the issuer or counterparty.",
    "issuer_lei":           "LEI of the issuer (20-character ISO 17442 code).",
    "issuer_country":       "Country of domicile of the issuer — ISO 3166-1 alpha-2.",
    "issuer_sector":        "NACE Rev.2 sector code of the issuer.",
    "cic_code":             "EIOPA Complementary Identification Code (4 characters). First 2 = country, last 2 = asset category.",
    "currency":             "ISO 4217 currency code of the asset.",
    "acquisition_date":     "Date the asset was acquired/purchased.",
    "maturity_date":        "Contractual maturity date for fixed-income instruments. Null for equity, property, and perpetual holdings.",
    "par_value":            "Face value / nominal / par amount in EUR.",
    "acquisition_cost":     "Total acquisition cost in EUR.",
    "market_value_eur":     "Fair market value in EUR at the valuation date.",
    "accrued_interest":     "Accrued but not yet received interest in EUR.",
    "coupon_rate":          "Annual coupon rate as a decimal (e.g. 0.0125 for 1.25%).",
    "credit_rating":        "External credit rating on S&P scale.",
    "credit_quality_step":  "EIOPA Credit Quality Step (0-6) mapped from external rating.",
    "portfolio_type":       "Solvency II portfolio allocation: Life, Non-life, Ring-fenced, or Other.",
    "custodian_name":       "Name of the custodian bank holding the asset.",
    "valuation_date":       "Date at which the market value was determined.",
    "is_listed":            "Whether the asset is listed on a regulated exchange (true/false).",
    "infrastructure_flag":  "Whether the asset qualifies as infrastructure under SII (true/false).",
    "modified_duration":    "Macaulay modified duration in years. Null for equity/property.",
    "asset_class":          "Bronze-layer asset class category (original, from source system).",
    "reporting_period":     "Reporting period in YYYY-QN format.",
    # New derived columns
    "cic_country":          "CIC country code (first 2 characters of cic_code). XL = supranational.",
    "cic_category":         "CIC asset category (last 2 characters of cic_code). Maps to EIOPA asset classification.",
    "sii_asset_class":      "Descriptive Solvency II asset class derived from CIC category (e.g. Government bonds, Corporate bonds).",
    "sii_value":            "Solvency II valuation in EUR. For this demo, equals market_value_eur (mark-to-market).",
    "unit_sii_price":       "SII value per unit (sii_value / par_value). Null if par_value is zero.",
    "unit_percentage_par":  "Market value as a percentage of par value (market_value_eur / par_value * 100).",
    "is_fixed_income":      "True if the asset is fixed income (CIC categories 11-13, 21-23, 51-54, 61). Used for interest rate risk SCR.",
    "is_equity_type1":      "True if the asset is Type 1 equity (listed, CIC 31). Lower equity risk charge under standard formula.",
    "is_equity_type2":      "True if the asset is Type 2 equity (unlisted/CIU, CIC 32-33, 41-46). Higher equity risk charge.",
    "dq_flag":              "Data quality flag: PASS if all key fields are non-null, WARN if any key field is missing.",
}

for col_name, comment in COLUMN_COMMENTS.items():
    try:
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col_name} COMMENT '{safe_comment}'")
    except Exception as e:
        print(f"  Could not comment column '{col_name}': {str(e)[:80]}")

print(f"Column comments applied to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Lineage

# COMMAND ----------

lineage.log_lineage(
    step_name=step_name,
    step_sequence=2,
    source_tables=[source_table],
    target_table=target_table,
    transformation_type="enrichment",
    transformation_desc=(
        "CIC decomposition (country + category), asset class mapping, "
        "SII valuation (market value = SII value), unit price calculations, "
        "risk classification flags (fixed income, equity type 1/2), "
        "data quality flag per row"
    ),
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    columns_in=columns_in,
    columns_out=columns_out,
    parameters={"reporting_date": reporting_date, "reporting_period": reporting_period},
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  SILVER ASSETS ENRICHMENT — SUMMARY")
print("=" * 70)
print(f"  Source table:       {source_table}")
print(f"  Target table:       {target_table}")
print(f"  Reporting period:   {reporting_period}")
print(f"  Rows in:            {row_count_in:,}")
print(f"  Rows out:           {row_count_out:,}")
print(f"  Columns out:        {len(columns_out)}")
print(f"  Total SII value:    EUR {total_sii_value:,.2f}")
print("-" * 70)

# Asset class distribution
print("\n  Asset Class Distribution:")
print("-" * 70)

df_class_dist = spark.sql("""
    SELECT
        sii_asset_class,
        COUNT(*)          AS num_assets,
        SUM(sii_value)    AS total_sii_value,
        ROUND(SUM(sii_value) / (SELECT SUM(sii_value) FROM v_silver_assets_enriched) * 100, 2) AS pct_of_total
    FROM v_silver_assets_enriched
    GROUP BY sii_asset_class
    ORDER BY total_sii_value DESC
""")

df_class_dist.show(20, truncate=False)

# DQ flag distribution
print("  Data Quality Flag Distribution:")
dq_dist = spark.sql("""
    SELECT dq_flag, COUNT(*) AS cnt
    FROM v_silver_assets_enriched
    GROUP BY dq_flag
    ORDER BY dq_flag
""")
dq_dist.show(truncate=False)

print("=" * 70)
print("  02_silver_assets complete.")
print("=" * 70)
