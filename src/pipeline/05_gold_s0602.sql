-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.06.02 — List of Assets (EIOPA QRT)
-- MAGIC Maps enriched assets to EIOPA cell references C0040–C0370.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_qrt_s0602(
  CONSTRAINT asset_id_present EXPECT (c0040_asset_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sii_value_positive EXPECT (c0170_total_sii_amount > 0)
)
COMMENT 'EIOPA S.06.02 List of Assets QRT — one row per asset holding'
AS
SELECT
    CURRENT_DATE() AS reporting_reference_date,

    -- C0040-C0110: Asset identification
    asset_id                    AS c0040_asset_id,
    CASE WHEN asset_id LIKE 'A%' THEN '99' ELSE '1' END AS c0050_id_code_type,
    portfolio_type              AS c0060_portfolio,
    CAST(NULL AS STRING)        AS c0070_fund_number,
    CAST(NULL AS STRING)        AS c0080_matching_adj_portfolio,
    0                           AS c0090_asset_unit_linked,
    0                           AS c0100_asset_pledged,
    'DE'                        AS c0110_country_of_custody,

    -- C0120-C0180: Custodian, valuation
    custodian_name              AS c0120_custodian,
    par_value                   AS c0130_quantity,
    par_value                   AS c0140_par_amount,
    CASE WHEN is_listed THEN 1 ELSE 2 END AS c0150_valuation_method,
    acquisition_cost            AS c0160_acquisition_value,
    sii_value                   AS c0170_total_sii_amount,
    accrued_interest            AS c0180_accrued_interest,

    -- C0190-C0250: Item/issuer info
    asset_name                  AS c0190_item_title,
    issuer_name                 AS c0200_issuer_name,
    issuer_lei                  AS c0210_issuer_code,
    'LEI'                       AS c0220_issuer_code_type,
    issuer_sector               AS c0230_issuer_sector,
    CAST(NULL AS STRING)        AS c0240_issuer_group_code,
    issuer_country              AS c0250_issuer_country,

    -- C0260-C0280: Currency, CIC, infrastructure
    currency                    AS c0260_currency,
    cic_code                    AS c0270_cic,
    CASE WHEN infrastructure_flag THEN 1 ELSE 0 END AS c0280_infrastructure,

    -- C0290-C0320: Credit assessment
    credit_rating               AS c0290_external_rating,
    'Standard and Poors'        AS c0300_nominated_ecai,
    credit_quality_step         AS c0310_credit_quality_step,
    CAST(NULL AS STRING)        AS c0320_internal_rating,

    -- C0340-C0370: Duration, unit price, maturity
    modified_duration           AS c0340_duration,
    unit_sii_price              AS c0350_unit_sii_price,
    unit_percentage_par         AS c0360_unit_percentage_par,
    maturity_date               AS c0370_maturity_date

FROM LIVE.silver_assets_enriched
