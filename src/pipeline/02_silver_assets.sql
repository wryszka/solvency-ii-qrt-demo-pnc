-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Asset Enrichment
-- MAGIC CIC decomposition, Solvency II asset class mapping, unit pricing, and risk flags.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_assets_enriched(
  CONSTRAINT asset_id_not_null EXPECT (asset_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT cic_code_not_null EXPECT (cic_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT market_value_positive EXPECT (market_value_eur > 0)
)
COMMENT 'Enriched investment register with CIC decomposition, SII valuation, and risk flags'
AS
SELECT
    ba.*,

    -- CIC decomposition
    SUBSTRING(ba.cic_code, 1, 2) AS cic_country,
    SUBSTRING(ba.cic_code, 3, 2) AS cic_category,

    -- Solvency II asset class mapping from CIC category
    CASE
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 11 AND 13 THEN 'Government bonds'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 21 AND 23 THEN 'Corporate bonds'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 31 AND 35 THEN 'Equity'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 41 AND 45 THEN 'Collective Investment Undertakings'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 51 AND 56 THEN 'Structured notes'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 61 AND 62 THEN 'Collateralised securities'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 71 AND 79 THEN 'Cash and deposits'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 81 AND 84 THEN 'Mortgages and loans'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) = 91 THEN 'Property'
        WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) = 95 THEN 'Plant and equipment'
        ELSE 'Other investments'
    END AS sii_asset_class,

    -- SII valuation (market value = SII value for this demo)
    ba.market_value_eur AS sii_value,

    -- Unit pricing
    ba.market_value_eur / NULLIF(ba.par_value, 0) AS unit_sii_price,
    ba.market_value_eur / NULLIF(ba.par_value, 0) * 100 AS unit_percentage_par,

    -- Risk classification flags
    CASE WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 11 AND 23 THEN TRUE ELSE FALSE END AS is_fixed_income,
    CASE WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 31 AND 32 THEN TRUE ELSE FALSE END AS is_equity_type1,
    CASE WHEN CAST(SUBSTRING(ba.cic_code, 3, 2) AS INT) BETWEEN 33 AND 35 THEN TRUE ELSE FALSE END AS is_equity_type2

FROM LIVE.bronze_assets ba
