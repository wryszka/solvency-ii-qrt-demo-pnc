-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.06.02 — List of Assets
-- MAGIC
-- MAGIC Maps **assets_enriched** to the EIOPA S.06.02 QRT template.
-- MAGIC Each column corresponds to an EIOPA cell reference (C0040–C0370).
-- MAGIC
-- MAGIC This is a **column rename** — no business logic, just mapping to regulatory format.
-- MAGIC An actuary can verify each mapping against the EIOPA Log.
-- MAGIC
-- MAGIC **Source:** `assets_enriched`
-- MAGIC **Target:** `s0602_list_of_assets`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s0602_list_of_assets(
  -- S.06.02 completeness checks
  CONSTRAINT c0040_asset_id_present  EXPECT (C0040_Asset_ID IS NOT NULL)            ON VIOLATION DROP ROW,
  CONSTRAINT c0170_sii_positive      EXPECT (C0170_Total_Solvency_II_Amount > 0)    ON VIOLATION FAIL UPDATE,
  CONSTRAINT c0270_cic_present       EXPECT (C0270_CIC IS NOT NULL)                 ON VIOLATION DROP ROW
)
COMMENT 'EIOPA S.06.02 List of Assets — one row per asset, columns = EIOPA cell references'
AS
SELECT
    -- ─── Header ─────────────────────────────────────────────────────
    reporting_period,

    -- ─── C0040–C0110: Asset identification & custody ────────────────
    asset_id                            AS C0040_Asset_ID,
    CASE WHEN asset_id LIKE 'A%'
         THEN '99' ELSE '1'
    END                                 AS C0050_ID_Code_Type,
    portfolio_type                      AS C0060_Portfolio,
    CAST(NULL AS STRING)                AS C0070_Fund_Number,
    CAST(NULL AS STRING)                AS C0080_Matching_Adj_Portfolio,
    0                                   AS C0090_Unit_Linked,
    0                                   AS C0100_Pledged_As_Collateral,
    'DE'                                AS C0110_Country_of_Custody,

    -- ─── C0120–C0180: Custodian, quantity, valuation ────────────────
    custodian_name                      AS C0120_Custodian,
    par_value                           AS C0130_Quantity,
    par_value                           AS C0140_Par_Amount,
    valuation_method_code               AS C0150_Valuation_Method,
    acquisition_cost                    AS C0160_Acquisition_Value,
    sii_value                           AS C0170_Total_Solvency_II_Amount,
    accrued_interest                    AS C0180_Accrued_Interest,

    -- ─── C0190–C0250: Item & issuer information ─────────────────────
    asset_name                          AS C0190_Item_Title,
    issuer_name                         AS C0200_Issuer_Name,
    issuer_lei                          AS C0210_Issuer_Code,
    'LEI'                               AS C0220_Issuer_Code_Type,
    issuer_sector                       AS C0230_Issuer_Sector,
    CAST(NULL AS STRING)                AS C0240_Issuer_Group_Code,
    issuer_country                      AS C0250_Issuer_Country,

    -- ─── C0260–C0280: Currency, CIC, infrastructure ─────────────────
    currency                            AS C0260_Currency,
    cic_code                            AS C0270_CIC,
    CASE WHEN infrastructure_flag
         THEN 1 ELSE 0
    END                                 AS C0280_Infrastructure_Investment,

    -- ─── C0290–C0320: Credit assessment ─────────────────────────────
    credit_rating                       AS C0290_External_Rating,
    'Standard and Poors'                AS C0300_Nominated_ECAI,
    credit_quality_step                 AS C0310_Credit_Quality_Step,
    CAST(NULL AS STRING)                AS C0320_Internal_Rating,

    -- ─── C0340–C0370: Duration, unit price, maturity ────────────────
    modified_duration                   AS C0340_Duration,
    CASE WHEN par_value > 0
         THEN ROUND(sii_value / par_value, 6)
         ELSE NULL
    END                                 AS C0350_Unit_Solvency_II_Price,
    CASE WHEN par_value > 0
         THEN ROUND(sii_value / par_value * 100, 4)
         ELSE NULL
    END                                 AS C0360_Unit_Percentage_of_Par,
    maturity_date                       AS C0370_Maturity_Date

FROM LIVE.assets_enriched
