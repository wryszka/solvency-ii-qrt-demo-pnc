# Databricks notebook source
# MAGIC %md
# MAGIC # 05_gold_s0602 — Format assets into EIOPA S.06.02 QRT template
# MAGIC
# MAGIC Reads `silver_assets_enriched` and produces `gold_qrt_s0602` — the EIOPA S.06.02
# MAGIC "List of Assets" QRT template. Each row represents one asset holding, and columns
# MAGIC are named after the EIOPA cell references (C0040–C0370).
# MAGIC
# MAGIC **Source:** `silver_assets_enriched`
# MAGIC **Target:** `gold_qrt_s0602`
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
dbutils.widgets.text("entity_lei", "5493001KJTIIGC8Y1R12")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")
entity_lei = dbutils.widgets.get("entity_lei")

# Derive reporting period (YYYY-QN) from reporting_date
from datetime import datetime
_rd = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period = f"{_rd.year}-Q{(_rd.month - 1) // 3 + 1}"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Entity LEI:       {entity_lei}")
print(f"Reporting period: {reporting_period}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise Lineage Logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)

source_table = f"{catalog}.{schema}.silver_assets_enriched"
target_table = f"{catalog}.{schema}.gold_qrt_s0602"
step_name = "gold_s0602_list_of_assets"

lineage.start_step(step_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

df_source = spark.table(source_table)
row_count_in = df_source.count()
columns_in = df_source.columns

print(f"silver_assets_enriched: {row_count_in} rows, {len(columns_in)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build S.06.02 QRT Table
# MAGIC
# MAGIC Single `CREATE OR REPLACE TABLE ... AS SELECT` mapping silver columns to EIOPA cell references.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {target_table} AS
SELECT
    -- =====================================================================
    -- Header fields
    -- =====================================================================
    DATE '{reporting_date}'                                       AS reporting_reference_date,
    '{entity_lei}'                                                AS reporting_entity_lei,

    -- =====================================================================
    -- C0040–C0110: Asset identification and custody
    -- =====================================================================
    asset_id                                                      AS c0040_asset_id,
    CASE
        WHEN asset_id LIKE 'A%' THEN '99'
        ELSE '1'
    END                                                           AS c0050_id_code_type,
    portfolio_type                                                AS c0060_portfolio,
    CAST(NULL AS STRING)                                          AS c0070_fund_number,
    CAST(NULL AS STRING)                                          AS c0080_matching_adj_portfolio,
    0                                                             AS c0090_asset_unit_linked,
    0                                                             AS c0100_asset_pledged,
    'DE'                                                          AS c0110_country_of_custody,

    -- =====================================================================
    -- C0120–C0180: Custodian, quantity, valuation
    -- =====================================================================
    custodian_name                                                AS c0120_custodian,
    par_value                                                     AS c0130_quantity,
    par_value                                                     AS c0140_par_amount,
    CASE
        WHEN is_listed THEN 1
        ELSE 2
    END                                                           AS c0150_valuation_method,
    acquisition_cost                                              AS c0160_acquisition_value,
    sii_value                                                     AS c0170_total_sii_amount,
    accrued_interest                                              AS c0180_accrued_interest,

    -- =====================================================================
    -- C0190–C0250: Item and issuer information
    -- =====================================================================
    asset_name                                                    AS c0190_item_title,
    issuer_name                                                   AS c0200_issuer_name,
    issuer_lei                                                    AS c0210_issuer_code,
    'LEI'                                                         AS c0220_issuer_code_type,
    issuer_sector                                                 AS c0230_issuer_sector,
    CAST(NULL AS STRING)                                          AS c0240_issuer_group_code,
    issuer_country                                                AS c0250_issuer_country,

    -- =====================================================================
    -- C0260–C0280: Currency, CIC, infrastructure
    -- =====================================================================
    currency                                                      AS c0260_currency,
    cic_code                                                      AS c0270_cic,
    CASE
        WHEN infrastructure_flag THEN 1
        ELSE 0
    END                                                           AS c0280_infrastructure,

    -- =====================================================================
    -- C0290–C0320: Credit assessment
    -- =====================================================================
    credit_rating                                                 AS c0290_external_rating,
    'Standard and Poors'                                          AS c0300_nominated_ecai,
    credit_quality_step                                           AS c0310_credit_quality_step,
    CAST(NULL AS STRING)                                          AS c0320_internal_rating,

    -- =====================================================================
    -- C0340–C0370: Duration, unit price, maturity
    -- =====================================================================
    modified_duration                                             AS c0340_duration,
    unit_sii_price                                                AS c0350_unit_sii_price,
    unit_percentage_par                                           AS c0360_unit_percentage_par,
    maturity_date                                                 AS c0370_maturity_date

FROM {source_table}
""")

row_count_out = spark.table(target_table).count()
columns_out = spark.table(target_table).columns

print(f"Table {target_table} created successfully ({row_count_out} rows, {len(columns_out)} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Column Comments (Unity Catalog Governance)

# COMMAND ----------

COLUMN_COMMENTS = {
    "reporting_reference_date":      "S.06.02 header — Reporting reference date (end of reporting period).",
    "reporting_entity_lei":          "S.06.02 header — LEI of the reporting undertaking.",
    "c0040_asset_id":                "S.06.02 C0040 — Asset ID code. Unique identifier for each asset.",
    "c0050_id_code_type":            "S.06.02 C0050 — Type of asset ID code. 1=ISIN, 99=internal code.",
    "c0060_portfolio":               "S.06.02 C0060 — Portfolio distinction (Life, Non-life, Ring-fenced, Other).",
    "c0070_fund_number":             "S.06.02 C0070 — Fund number for ring-fenced funds. NULL when not applicable.",
    "c0080_matching_adj_portfolio":  "S.06.02 C0080 — Matching adjustment portfolio number. NULL when not applicable.",
    "c0090_asset_unit_linked":       "S.06.02 C0090 — Identifies assets held for unit-linked / index-linked contracts. 0=not unit-linked.",
    "c0100_asset_pledged":           "S.06.02 C0100 — Identifies assets pledged as collateral. 0=not pledged.",
    "c0110_country_of_custody":      "S.06.02 C0110 — ISO 3166-1 alpha-2 country code of the custodian.",
    "c0120_custodian":               "S.06.02 C0120 — Name of the custodian holding the asset.",
    "c0130_quantity":                "S.06.02 C0130 — Number of units or par value of the asset.",
    "c0140_par_amount":              "S.06.02 C0140 — Par / face / nominal amount for fixed-income instruments in EUR.",
    "c0150_valuation_method":        "S.06.02 C0150 — Valuation method. 1=quoted market price (mark-to-market), 2=adjusted equity methods (mark-to-model).",
    "c0160_acquisition_value":       "S.06.02 C0160 — Total acquisition value of the asset in EUR.",
    "c0170_total_sii_amount":        "S.06.02 C0170 — Total Solvency II amount (SII value) in EUR.",
    "c0180_accrued_interest":        "S.06.02 C0180 — Accrued interest in EUR (fixed-income instruments).",
    "c0190_item_title":              "S.06.02 C0190 — Name / title of the asset item.",
    "c0200_issuer_name":             "S.06.02 C0200 — Legal name of the issuer.",
    "c0210_issuer_code":             "S.06.02 C0210 — Code identifying the issuer (LEI).",
    "c0220_issuer_code_type":        "S.06.02 C0220 — Type of issuer code. LEI = Legal Entity Identifier.",
    "c0230_issuer_sector":           "S.06.02 C0230 — Economic sector of the issuer (NACE code).",
    "c0240_issuer_group_code":       "S.06.02 C0240 — Group code of the issuer. NULL when simplified.",
    "c0250_issuer_country":          "S.06.02 C0250 — ISO 3166-1 alpha-2 country code of the issuer.",
    "c0260_currency":                "S.06.02 C0260 — ISO 4217 currency code of the asset.",
    "c0270_cic":                     "S.06.02 C0270 — EIOPA Complementary Identification Code (CIC) — 4 characters.",
    "c0280_infrastructure":          "S.06.02 C0280 — Infrastructure investment flag. 1=qualifying infrastructure, 0=other.",
    "c0290_external_rating":         "S.06.02 C0290 — External credit rating from nominated ECAI.",
    "c0300_nominated_ecai":          "S.06.02 C0300 — Nominated External Credit Assessment Institution (ECAI).",
    "c0310_credit_quality_step":     "S.06.02 C0310 — EIOPA Credit Quality Step (0-6) derived from external rating.",
    "c0320_internal_rating":         "S.06.02 C0320 — Internal credit rating. NULL when not used.",
    "c0340_duration":                "S.06.02 C0340 — Modified duration in years for fixed-income instruments.",
    "c0350_unit_sii_price":          "S.06.02 C0350 — Unit Solvency II price (SII value per unit).",
    "c0360_unit_percentage_par":     "S.06.02 C0360 — Unit price as a percentage of par value.",
    "c0370_maturity_date":           "S.06.02 C0370 — Contractual maturity date for fixed-income instruments.",
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
# MAGIC ## Data Quality Checks

# COMMAND ----------

# --- Check 1: Row count matches source ---
row_count_match = (row_count_out == row_count_in)
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="row_count_matches_source",
    check_category="completeness",
    check_expression="gold row_count == silver row_count",
    expected_value=str(row_count_in),
    actual_value=str(row_count_out),
    passed=row_count_match,
    severity="error",
    details="Gold S.06.02 must have one row per source asset — row counts must match."
)

# --- Check 2: Total C0170 (SII amount) > 0 ---
total_sii = spark.sql(f"SELECT COALESCE(SUM(c0170_total_sii_amount), 0) AS total FROM {target_table}").first()["total"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="total_c0170_positive",
    check_category="validity",
    check_expression="SUM(c0170_total_sii_amount) > 0",
    expected_value="> 0",
    actual_value=f"{total_sii:,.2f}",
    passed=(total_sii > 0),
    severity="error",
    details="Total Solvency II amount across all assets must be positive."
)

# --- Check 3: No null C0040 (asset_id) ---
null_c0040 = spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_table} WHERE c0040_asset_id IS NULL").first()["cnt"]
lineage.log_data_quality(
    step_name=step_name,
    table_name=target_table,
    check_name="c0040_asset_id_not_null",
    check_category="completeness",
    check_expression="COUNT(*) WHERE c0040_asset_id IS NULL",
    expected_value="0",
    actual_value=str(null_c0040),
    passed=(null_c0040 == 0),
    severity="error",
    details="S.06.02 C0040 (Asset ID) is mandatory and must not be null."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Lineage

# COMMAND ----------

lineage.log_lineage(
    step_name=step_name,
    step_sequence=5,
    source_tables=[source_table],
    target_table=target_table,
    transformation_type="qrt_formatting",
    transformation_desc=(
        "Maps silver_assets_enriched columns to EIOPA S.06.02 'List of Assets' QRT template. "
        "Columns C0040-C0370 follow EIOPA cell references. Header includes reporting date and entity LEI."
    ),
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    columns_in=columns_in,
    columns_out=columns_out,
    parameters={
        "reporting_date": reporting_date,
        "reporting_period": reporting_period,
        "entity_lei": entity_lei,
    },
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  GOLD S.06.02 LIST OF ASSETS — SUMMARY")
print("=" * 70)
print(f"  Source table:       {source_table}")
print(f"  Target table:       {target_table}")
print(f"  Reporting period:   {reporting_period}")
print(f"  Entity LEI:         {entity_lei}")
print(f"  Rows:               {row_count_out:,}")
print(f"  Columns:            {len(columns_out)}")
print(f"  Total SII amount:   EUR {total_sii:,.2f}")
print("-" * 70)

# Top 10 asset classes by SII value
print("\n  Top 10 Asset Classes by SII Value:")
print("-" * 70)

df_top_classes = spark.sql(f"""
    SELECT
        c0270_cic,
        COUNT(*)                            AS num_assets,
        SUM(c0170_total_sii_amount)         AS total_sii_value,
        ROUND(SUM(c0170_total_sii_amount) /
              (SELECT SUM(c0170_total_sii_amount) FROM {target_table}) * 100, 2)
                                            AS pct_of_total
    FROM {target_table}
    GROUP BY c0270_cic
    ORDER BY total_sii_value DESC
    LIMIT 10
""")

df_top_classes.show(10, truncate=False)

print("=" * 70)
print("  05_gold_s0602 complete.")
print("=" * 70)
