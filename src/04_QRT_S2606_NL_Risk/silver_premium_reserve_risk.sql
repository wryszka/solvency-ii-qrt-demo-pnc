-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Premium & Reserve Risk by Line of Business
-- MAGIC
-- MAGIC Applies EIOPA Standard Formula factors to compute premium and reserve risk
-- MAGIC charges per LoB. Uses prescribed sigma factors and the volume measure formula.
-- MAGIC
-- MAGIC **Formula:** Risk charge = 3 * sigma * volume_measure
-- MAGIC (approximation of VaR 99.5% assuming lognormal distribution)
-- MAGIC
-- MAGIC **Source:** `volume_measures`
-- MAGIC **Target:** `premium_reserve_risk`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW premium_reserve_risk(
  CONSTRAINT volume_positive       EXPECT (volume_measure_eur > 0)    ON VIOLATION DROP ROW,
  CONSTRAINT premium_risk_positive EXPECT (premium_risk_eur >= 0)     ON VIOLATION DROP ROW,
  CONSTRAINT reserve_risk_positive EXPECT (reserve_risk_eur >= 0)     ON VIOLATION DROP ROW
)
COMMENT 'Premium & reserve risk by LoB — EIOPA Standard Formula sigma factors applied to volume measures'
AS
SELECT
    reporting_period,
    lob_code,
    lob_name,

    -- Volume measure = max(earned, written_next_year) + BE_claims
    earned_premium_net,
    written_premium_net_next_year,
    best_estimate_claims_provision,
    best_estimate_premium_provision,

    GREATEST(earned_premium_net, written_premium_net_next_year)
        + best_estimate_claims_provision
        AS volume_measure_eur,

    -- EIOPA prescribed sigma factors by LoB (Delegated Regulation Art. 117)
    CASE lob_code
        WHEN 1  THEN 0.065   -- Medical expense
        WHEN 2  THEN 0.085   -- Income protection
        WHEN 4  THEN 0.100   -- Motor vehicle liability
        WHEN 5  THEN 0.070   -- Other motor
        WHEN 7  THEN 0.080   -- Fire and property
        WHEN 8  THEN 0.140   -- General liability
        WHEN 12 THEN 0.130   -- Misc financial loss
        ELSE 0.100
    END AS sigma_premium,

    CASE lob_code
        WHEN 1  THEN 0.090   -- Medical expense
        WHEN 2  THEN 0.110   -- Income protection
        WHEN 4  THEN 0.095   -- Motor vehicle liability
        WHEN 5  THEN 0.100   -- Other motor
        WHEN 7  THEN 0.110   -- Fire and property
        WHEN 8  THEN 0.190   -- General liability
        WHEN 12 THEN 0.150   -- Misc financial loss
        ELSE 0.120
    END AS sigma_reserve,

    -- Risk charges: VaR(99.5%) approximation = 3 * sigma * volume
    ROUND(3.0 * CASE lob_code
        WHEN 1 THEN 0.065 WHEN 2 THEN 0.085 WHEN 4 THEN 0.100
        WHEN 5 THEN 0.070 WHEN 7 THEN 0.080 WHEN 8 THEN 0.140
        WHEN 12 THEN 0.130 ELSE 0.100
    END * GREATEST(earned_premium_net, written_premium_net_next_year), 2)
        AS premium_risk_eur,

    ROUND(3.0 * CASE lob_code
        WHEN 1 THEN 0.090 WHEN 2 THEN 0.110 WHEN 4 THEN 0.095
        WHEN 5 THEN 0.100 WHEN 7 THEN 0.110 WHEN 8 THEN 0.190
        WHEN 12 THEN 0.150 ELSE 0.120
    END * best_estimate_claims_provision, 2)
        AS reserve_risk_eur

FROM LIVE.volume_measures
WHERE reporting_period = (SELECT MAX(reporting_period) FROM LIVE.volume_measures)
