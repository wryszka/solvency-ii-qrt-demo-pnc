-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.25.01 — SCR Breakdown (Standard Formula)
-- MAGIC
-- MAGIC Maps the `scr_results` table into the EIOPA S.25.01 QRT template.
-- MAGIC Output is in **long format**: one row per (template_row, reporting_period).
-- MAGIC
-- MAGIC Each `template_row_id` (R0010, R0020, ...) matches the EIOPA S.25.01 row reference.
-- MAGIC The actuary can verify each mapping against the EIOPA S.25.01 log.
-- MAGIC
-- MAGIC **Source:** `scr_results` (from Standard Formula model run)
-- MAGIC **Target:** `s2501_scr_breakdown`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s2501_scr_breakdown(
  CONSTRAINT row_id_present   EXPECT (template_row_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT amount_not_null  EXPECT (amount_eur IS NOT NULL)       ON VIOLATION DROP ROW
)
COMMENT 'EIOPA S.25.01 SCR Standard Formula breakdown — long format (row per template_row × quarter)'
AS

-- R0010: Market risk
SELECT reporting_period, 'R0010' AS template_row_id,
       'Market risk' AS template_row_label,
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market'

UNION ALL
-- R0020: Counterparty default risk
SELECT reporting_period, 'R0020',
       'Counterparty default risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_default'

UNION ALL
-- R0030: Life underwriting risk
SELECT reporting_period, 'R0030',
       'Life underwriting risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_life'

UNION ALL
-- R0040: Health underwriting risk
SELECT reporting_period, 'R0040',
       'Health underwriting risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_health'

UNION ALL
-- R0050: Non-life underwriting risk
SELECT reporting_period, 'R0050',
       'Non-life underwriting risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_non_life'

UNION ALL
-- R0100: Basic SCR (diversified)
SELECT reporting_period, 'R0100',
       'Basic Solvency Capital Requirement',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'BSCR'

UNION ALL
-- R0130: Operational risk
SELECT reporting_period, 'R0130',
       'Operational risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'Op_risk'

UNION ALL
-- R0150: Loss-absorbing capacity of deferred taxes
SELECT reporting_period, 'R0150',
       'Loss-absorbing capacity of deferred taxes',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'LAC_DT'

UNION ALL
-- R0200: Solvency Capital Requirement
SELECT reporting_period, 'R0200',
       'Solvency Capital Requirement',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR'

-- === Market risk sub-modules ===

UNION ALL
-- R0010.01: Interest rate risk
SELECT reporting_period, 'R0010.01',
       'Interest rate risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_interest_rate'

UNION ALL
-- R0010.02: Equity risk
SELECT reporting_period, 'R0010.02',
       'Equity risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_equity'

UNION ALL
-- R0010.03: Property risk
SELECT reporting_period, 'R0010.03',
       'Property risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_property'

UNION ALL
-- R0010.04: Spread risk (bonds)
SELECT reporting_period, 'R0010.04',
       'Spread risk - bonds',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_spread_bonds'

UNION ALL
-- R0010.05: Spread risk (structured)
SELECT reporting_period, 'R0010.05',
       'Spread risk - structured products',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_spread_structured'

UNION ALL
-- R0010.06: Currency risk
SELECT reporting_period, 'R0010.06',
       'Currency risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_currency'

UNION ALL
-- R0010.07: Concentration risk
SELECT reporting_period, 'R0010.07',
       'Concentration risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_market_concentration'

-- === Non-life underwriting sub-modules ===

UNION ALL
-- R0050.01: Premium and reserve risk
SELECT reporting_period, 'R0050.01',
       'Non-life premium and reserve risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_nl_premium_reserve'

UNION ALL
-- R0050.02: Lapse risk
SELECT reporting_period, 'R0050.02',
       'Non-life lapse risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_nl_lapse'

UNION ALL
-- R0050.03: Catastrophe risk
SELECT reporting_period, 'R0050.03',
       'Non-life catastrophe risk',
       amount_eur, model_version, calibration_year
FROM LIVE.scr_results WHERE component = 'SCR_nl_catastrophe'
