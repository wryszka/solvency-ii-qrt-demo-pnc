-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Enrich Investment Register
-- MAGIC
-- MAGIC Reads the raw **assets** table and produces **assets_enriched** with:
-- MAGIC - CIC code decomposition (country, category, subcategory)
-- MAGIC - Solvency II asset class mapping
-- MAGIC - SII valuation (mark-to-market / mark-to-model)
-- MAGIC - Credit quality step from external rating
-- MAGIC
-- MAGIC **Source:** `assets` (raw investment register)
-- MAGIC **Target:** `assets_enriched`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW assets_enriched(
  -- Data quality: every asset must have an ID and positive SII value
  CONSTRAINT asset_id_not_null       EXPECT (asset_id IS NOT NULL)           ON VIOLATION DROP ROW,
  CONSTRAINT sii_value_positive      EXPECT (sii_value > 0)                 ON VIOLATION FAIL UPDATE,
  CONSTRAINT cic_code_valid          EXPECT (LENGTH(cic_code) = 4)          ON VIOLATION DROP ROW,
  CONSTRAINT currency_not_null       EXPECT (currency IS NOT NULL)          ON VIOLATION DROP ROW
)
COMMENT 'Enriched investment register — CIC decomposition, SII valuation, credit quality mapping'
AS
SELECT
    -- === Identity ===
    asset_id,
    asset_name,
    asset_class,
    reporting_period,

    -- === CIC Decomposition ===
    cic_code,
    SUBSTRING(cic_code, 1, 2)           AS cic_country,
    SUBSTRING(cic_code, 3, 1)           AS cic_category,
    SUBSTRING(cic_code, 4, 1)           AS cic_subcategory,
    CASE SUBSTRING(cic_code, 3, 1)
        WHEN '1' THEN 'Government bonds'
        WHEN '2' THEN 'Corporate bonds'
        WHEN '3' THEN 'Equity'
        WHEN '4' THEN 'Collective investment undertakings'
        WHEN '5' THEN 'Structured notes'
        WHEN '6' THEN 'Collateralised securities'
        WHEN '7' THEN 'Cash and deposits'
        WHEN '8' THEN 'Mortgages and loans'
        WHEN '9' THEN 'Property'
        ELSE 'Other'
    END                                 AS cic_category_name,

    -- === Issuer ===
    issuer_name,
    issuer_lei,
    issuer_country,
    issuer_sector,

    -- === Valuation ===
    currency,
    par_value,
    acquisition_cost,
    market_value_eur,
    sii_value,
    accrued_interest,
    coupon_rate,
    CASE WHEN is_listed THEN 'Mark-to-market' ELSE 'Mark-to-model' END
                                        AS valuation_method,
    CASE WHEN is_listed THEN 1 ELSE 2 END
                                        AS valuation_method_code,

    -- === Risk characteristics ===
    credit_rating,
    credit_quality_step,
    modified_duration,
    maturity_date,
    infrastructure_flag,
    CASE WHEN infrastructure_flag THEN 'Qualifying infrastructure' ELSE 'Standard' END
                                        AS infrastructure_label,

    -- === Custody ===
    portfolio_type,
    custodian_name,
    is_listed

FROM LIVE.assets
WHERE reporting_period = (SELECT MAX(reporting_period) FROM LIVE.assets)
