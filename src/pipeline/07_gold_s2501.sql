-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.25.01 — Solvency Capital Requirement (EIOPA QRT)
-- MAGIC Maps the SCR waterfall from silver to EIOPA template rows R0010–R0220.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_qrt_s2501(
  CONSTRAINT template_row_id_present EXPECT (template_row_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT scr_positive EXPECT (template_row_id != 'R0200' OR c0100_net_scr > 0)
)
COMMENT 'EIOPA S.25.01 Solvency Capital Requirement - Standard Formula QRT'
AS

-- R0010: Market risk
SELECT CURRENT_DATE() AS reporting_reference_date,
       'R0010' AS template_row_id,
       'Market risk' AS template_row_label,
       gross_capital_charge AS c0110_gross_scr,
       'None' AS c0080_ust_simplifications,
       net_capital_charge AS c0100_net_scr
FROM LIVE.silver_scr_modules
WHERE level = 'module' AND module_name = 'Market risk'

-- R0020: Counterparty default risk
UNION ALL
SELECT CURRENT_DATE(), 'R0020', 'Counterparty default risk',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'module' AND module_name = 'Counterparty default risk'

-- R0030: Life underwriting risk (zero for P&C)
UNION ALL
SELECT CURRENT_DATE(), 'R0030', 'Life underwriting risk',
       CAST(0.0 AS DOUBLE), 'None', CAST(0.0 AS DOUBLE)

-- R0040: Health underwriting risk
UNION ALL
SELECT CURRENT_DATE(), 'R0040', 'Health underwriting risk',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'module' AND module_name = 'Health underwriting risk'

-- R0050: Non-life underwriting risk
UNION ALL
SELECT CURRENT_DATE(), 'R0050', 'Non-life underwriting risk',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'module' AND module_name = 'Non-life underwriting risk'

-- R0060: Diversification
UNION ALL
SELECT CURRENT_DATE(), 'R0060', 'Diversification',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'intermediate' AND module_name = 'Inter-module diversification'

-- R0070: Intangible asset module
UNION ALL
SELECT CURRENT_DATE(), 'R0070', 'Intangible asset module',
       COALESCE(gross_capital_charge, 0.0), 'None', COALESCE(net_capital_charge, 0.0)
FROM LIVE.silver_scr_modules
WHERE level = 'module' AND module_name = 'Intangible risk'

-- R0100: Basic Solvency Capital Requirement
UNION ALL
SELECT CURRENT_DATE(), 'R0100', 'Basic Solvency Capital Requirement',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'bscr'

-- R0130: Operational risk
UNION ALL
SELECT CURRENT_DATE(), 'R0130', 'Calculation of Solvency Capital Requirement - Operational risk',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'adjustment' AND module_name = 'Operational risk'

-- R0140: Loss-absorbing capacity of technical provisions
UNION ALL
SELECT CURRENT_DATE(), 'R0140', 'Adjustment - Loss-absorbing capacity of technical provisions',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'adjustment' AND module_name = 'LAC Technical Provisions'

-- R0150: Loss-absorbing capacity of deferred taxes
UNION ALL
SELECT CURRENT_DATE(), 'R0150', 'Adjustment - Loss-absorbing capacity of deferred taxes',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'adjustment' AND module_name = 'LAC Deferred Taxes'

-- R0200: Solvency Capital Requirement
UNION ALL
SELECT CURRENT_DATE(), 'R0200', 'Solvency Capital Requirement',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'scr'

-- R0210: Capital add-on already set (zero)
UNION ALL
SELECT CURRENT_DATE(), 'R0210', 'Capital add-on already set',
       CAST(0.0 AS DOUBLE), 'None', CAST(0.0 AS DOUBLE)

-- R0220: Solvency Capital Requirement including capital add-on
UNION ALL
SELECT CURRENT_DATE(), 'R0220', 'Solvency capital requirement including capital add-on',
       gross_capital_charge, 'None', net_capital_charge
FROM LIVE.silver_scr_modules
WHERE level = 'scr'
