-- Databricks notebook source
-- MAGIC %md
-- MAGIC # S.25.01 — Solvency Summary
-- MAGIC
-- MAGIC Combines the SCR breakdown with **own funds** to produce the solvency ratio.
-- MAGIC This is what the actuary and board review before sign-off.
-- MAGIC
-- MAGIC Key metrics:
-- MAGIC - Eligible own funds (Tier 1 + Tier 2 + Tier 3, with tiering limits)
-- MAGIC - SCR and MCR
-- MAGIC - Solvency ratio = Eligible own funds / SCR
-- MAGIC - MCR ratio = Eligible own funds / MCR
-- MAGIC
-- MAGIC **Sources:** `s2501_scr_breakdown`, `own_funds`
-- MAGIC **Target:** `s2501_summary` (validation view)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s2501_summary(
  CONSTRAINT solvency_ratio_positive EXPECT (solvency_ratio_pct > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT scr_positive            EXPECT (scr_eur > 0)            ON VIOLATION FAIL UPDATE
)
COMMENT 'S.25.01 solvency summary — SCR, own funds, solvency ratio for board review and regulatory filing'
AS
WITH scr AS (
  SELECT
    reporting_period,
    model_version,
    calibration_year,
    MAX(CASE WHEN template_row_id = 'R0010' THEN amount_eur END) AS market_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0020' THEN amount_eur END) AS default_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0030' THEN amount_eur END) AS life_uw_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0040' THEN amount_eur END) AS health_uw_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0050' THEN amount_eur END) AS non_life_uw_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0100' THEN amount_eur END) AS bscr_eur,
    MAX(CASE WHEN template_row_id = 'R0130' THEN amount_eur END) AS op_risk_eur,
    MAX(CASE WHEN template_row_id = 'R0150' THEN amount_eur END) AS lac_dt_eur,
    MAX(CASE WHEN template_row_id = 'R0200' THEN amount_eur END) AS scr_eur
  FROM LIVE.s2501_scr_breakdown
  GROUP BY reporting_period, model_version, calibration_year
),
funds AS (
  SELECT
    reporting_period,
    SUM(CASE WHEN tier = 1 THEN amount_eur ELSE 0 END) AS tier1_eur,
    SUM(CASE WHEN tier = 2 THEN amount_eur ELSE 0 END) AS tier2_eur,
    SUM(CASE WHEN tier = 3 THEN amount_eur ELSE 0 END) AS tier3_eur,
    SUM(amount_eur)                                      AS total_own_funds_eur
  FROM LIVE.own_funds
  GROUP BY reporting_period
)
SELECT
    s.reporting_period,
    s.model_version,
    s.calibration_year,

    -- Risk modules
    s.market_risk_eur,
    s.default_risk_eur,
    s.life_uw_risk_eur,
    s.health_uw_risk_eur,
    s.non_life_uw_risk_eur,
    s.bscr_eur,
    s.op_risk_eur,
    s.lac_dt_eur,
    s.scr_eur,

    -- MCR = max(25% of SCR, absolute floor of EUR 3.7m for non-life)
    GREATEST(ROUND(s.scr_eur * 0.25, 2), 3700000) AS mcr_eur,

    -- Own funds
    f.tier1_eur,
    f.tier2_eur,
    f.tier3_eur,
    f.total_own_funds_eur,

    -- Eligible own funds (tiering limits: T2 <= 50% SCR, T3 <= 15% SCR)
    f.tier1_eur
      + LEAST(f.tier2_eur, ROUND(s.scr_eur * 0.50, 2))
      + LEAST(f.tier3_eur, ROUND(s.scr_eur * 0.15, 2))
    AS eligible_own_funds_eur,

    -- Solvency ratio
    ROUND(
      (f.tier1_eur
        + LEAST(f.tier2_eur, ROUND(s.scr_eur * 0.50, 2))
        + LEAST(f.tier3_eur, ROUND(s.scr_eur * 0.15, 2))
      ) * 100.0 / s.scr_eur, 1
    ) AS solvency_ratio_pct,

    -- MCR ratio
    ROUND(
      (f.tier1_eur
        + LEAST(f.tier2_eur, ROUND(s.scr_eur * 0.50, 2))
        + LEAST(f.tier3_eur, ROUND(s.scr_eur * 0.15, 2))
      ) * 100.0 / GREATEST(ROUND(s.scr_eur * 0.25, 2), 3700000), 1
    ) AS mcr_ratio_pct,

    -- Surplus
    f.tier1_eur
      + LEAST(f.tier2_eur, ROUND(s.scr_eur * 0.50, 2))
      + LEAST(f.tier3_eur, ROUND(s.scr_eur * 0.15, 2))
      - s.scr_eur
    AS surplus_eur

FROM scr s
JOIN funds f ON s.reporting_period = f.reporting_period
