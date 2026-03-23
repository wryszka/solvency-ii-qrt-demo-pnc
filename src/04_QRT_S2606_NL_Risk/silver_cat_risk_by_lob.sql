-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Catastrophe Risk by Line of Business
-- MAGIC
-- MAGIC Aggregates Igloo stochastic output to catastrophe risk charges per LoB.
-- MAGIC Uses the **1-in-200 return period** (VaR 99.5%) as required by Solvency II.
-- MAGIC
-- MAGIC **Source:** `igloo_run_results` (stochastic engine output)
-- MAGIC **Target:** `cat_risk_by_lob`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW cat_risk_by_lob(
  -- Catastrophe risk must be positive (net of reinsurance)
  CONSTRAINT var_net_positive       EXPECT (var_net_eur > 0)           ON VIOLATION DROP ROW,
  -- TVaR should be >= VaR (tail is heavier than the point estimate)
  CONSTRAINT tvar_gte_var           EXPECT (tvar_net_eur >= var_net_eur) ON VIOLATION DROP ROW
)
COMMENT 'Catastrophe risk by LoB — 1-in-200 VaR from Igloo stochastic model, net of reinsurance'
AS
SELECT
    reporting_period,
    lob_code,
    lob_name,

    -- Aggregate across all perils for this LoB (sum of per-peril VaR at 1-in-200)
    SUM(CAST(var_gross_eur AS DOUBLE))  AS var_gross_eur,
    SUM(CAST(tvar_gross_eur AS DOUBLE)) AS tvar_gross_eur,
    SUM(CAST(var_ceded_eur AS DOUBLE))  AS var_ceded_eur,
    SUM(CAST(tvar_ceded_eur AS DOUBLE)) AS tvar_ceded_eur,
    SUM(CAST(var_net_eur AS DOUBLE))    AS var_net_eur,
    SUM(CAST(tvar_net_eur AS DOUBLE))   AS tvar_net_eur,

    -- Diversification info
    COUNT(DISTINCT peril)               AS perils_modelled,
    model_version

FROM LIVE.igloo_run_results
WHERE CAST(return_period AS INT) = 200   -- 1-in-200 = VaR 99.5% (regulatory standard)
GROUP BY reporting_period, lob_code, lob_name, model_version
