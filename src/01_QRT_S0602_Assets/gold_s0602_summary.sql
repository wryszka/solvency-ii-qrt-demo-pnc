-- Databricks notebook source
-- MAGIC %md
-- MAGIC # S.06.02 — Validation Summary
-- MAGIC
-- MAGIC Aggregated view of the S.06.02 output for **actuarial review**.
-- MAGIC Shows totals by CIC category with reconciliation to source data.
-- MAGIC
-- MAGIC This is what the actuary checks before approving the QRT:
-- MAGIC - Total SII amount reconciles to the balance sheet
-- MAGIC - Asset counts match the investment register
-- MAGIC - No unexpected CIC categories
-- MAGIC
-- MAGIC **Source:** `s0602_list_of_assets`
-- MAGIC **Target:** `s0602_summary` (validation view)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s0602_summary(
  CONSTRAINT total_sii_positive EXPECT (total_sii_amount > 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'S.06.02 validation summary — totals by CIC category for actuarial review and sign-off'
AS
SELECT
    reporting_period,

    -- CIC category breakdown
    SUBSTRING(C0270_CIC, 3, 1)         AS cic_category_code,
    CASE SUBSTRING(C0270_CIC, 3, 1)
        WHEN '1' THEN 'Government bonds'
        WHEN '2' THEN 'Corporate bonds'
        WHEN '3' THEN 'Equity'
        WHEN '4' THEN 'Collective investment undertakings'
        WHEN '9' THEN 'Property'
        ELSE 'Other'
    END                                 AS cic_category_name,

    -- Aggregates
    COUNT(*)                            AS asset_count,
    SUM(C0170_Total_Solvency_II_Amount) AS total_sii_amount,
    SUM(C0160_Acquisition_Value)        AS total_acquisition_value,
    SUM(C0180_Accrued_Interest)         AS total_accrued_interest,
    ROUND(SUM(C0170_Total_Solvency_II_Amount) * 100.0 /
          SUM(SUM(C0170_Total_Solvency_II_Amount)) OVER (PARTITION BY reporting_period), 2)
                                        AS pct_of_total_sii,

    -- Quality indicators
    COUNT(CASE WHEN C0310_Credit_Quality_Step <= 2 THEN 1 END)
                                        AS investment_grade_count,
    AVG(C0340_Duration)                 AS avg_duration

FROM LIVE.s0602_list_of_assets
GROUP BY
    reporting_period,
    SUBSTRING(C0270_CIC, 3, 1)
