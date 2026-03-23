-- Databricks notebook source
-- MAGIC %md
-- MAGIC # S.26.06 — Summary for Actuarial Review
-- MAGIC
-- MAGIC Pivoted view of the S.26.06 template for sign-off. Shows premium risk,
-- MAGIC reserve risk, catastrophe risk, diversification benefit, and total
-- MAGIC NL underwriting risk.
-- MAGIC
-- MAGIC **Source:** `s2606_nl_uw_risk`
-- MAGIC **Target:** `s2606_summary`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s2606_summary(
  CONSTRAINT total_nl_uw_positive EXPECT (total_nl_uw_scr > 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'S.26.06 summary — NL UW risk breakdown for actuarial review and sign-off'
AS
SELECT
    reporting_period,

    MAX(CASE WHEN template_row_id = 'R0010' THEN amount_eur END) AS premium_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0020' THEN amount_eur END) AS reserve_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0030' THEN amount_eur END) AS combined_prem_res_eur,
    MAX(CASE WHEN template_row_id = 'R0040' THEN amount_eur END) AS cat_risk_var_eur,
    MAX(CASE WHEN template_row_id = 'R0050' THEN amount_eur END) AS cat_risk_tvar_eur,
    MAX(CASE WHEN template_row_id = 'R0100' THEN amount_eur END) AS total_nl_uw_scr,
    MAX(CASE WHEN template_row_id = 'R0110' THEN amount_eur END) AS diversification_benefit,

    -- Cat risk as % of total
    ROUND(MAX(CASE WHEN template_row_id = 'R0040' THEN amount_eur END) * 100.0 /
          NULLIF(MAX(CASE WHEN template_row_id = 'R0100' THEN amount_eur END), 0), 1)
        AS cat_pct_of_total,

    -- Premium risk as % of total
    ROUND(MAX(CASE WHEN template_row_id = 'R0010' THEN amount_eur END) * 100.0 /
          NULLIF(MAX(CASE WHEN template_row_id = 'R0100' THEN amount_eur END), 0), 1)
        AS premium_pct_of_total

FROM LIVE.s2606_nl_uw_risk
GROUP BY reporting_period
