-- Databricks notebook source
-- MAGIC %md
-- MAGIC # S.05.01 — Validation Summary
-- MAGIC
-- MAGIC Aggregated view of the S.05.01 output for **actuarial review**.
-- MAGIC Shows key P&L ratios by Line of Business for each reporting period.
-- MAGIC
-- MAGIC This is what the actuary checks before approving the QRT:
-- MAGIC - Loss ratio = Net claims incurred / Net earned premium
-- MAGIC - Expense ratio = Total expenses / Net earned premium
-- MAGIC - Combined ratio = Loss ratio + Expense ratio (should be < 100% for profit)
-- MAGIC - Net vs Gross reconciliation per LoB
-- MAGIC
-- MAGIC **Source:** `s0501_premiums_claims_expenses`
-- MAGIC **Target:** `s0501_summary` (validation view)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s0501_summary(
  CONSTRAINT combined_ratio_realistic EXPECT (combined_ratio_pct BETWEEN 50 AND 200) ON VIOLATION DROP ROW
)
COMMENT 'S.05.01 validation summary — key P&L ratios by LoB for actuarial review and sign-off'
AS
WITH pivoted AS (
  SELECT
    reporting_period,
    lob_code,
    lob_name,

    -- Premiums
    MAX(CASE WHEN template_row_id = 'R0110' THEN amount_eur END) AS gross_written_premium,
    MAX(CASE WHEN template_row_id = 'R0200' THEN amount_eur END) AS net_written_premium,
    MAX(CASE WHEN template_row_id = 'R0210' THEN amount_eur END) AS gross_earned_premium,
    MAX(CASE WHEN template_row_id = 'R0300' THEN amount_eur END) AS net_earned_premium,

    -- Claims
    MAX(CASE WHEN template_row_id = 'R0310' THEN amount_eur END) AS gross_incurred,
    MAX(CASE WHEN template_row_id = 'R0400' THEN amount_eur END) AS net_incurred,

    -- Expenses
    MAX(CASE WHEN template_row_id = 'R0550' THEN amount_eur END) AS total_expenses

  FROM LIVE.s0501_premiums_claims_expenses
  WHERE lob_code != 0  -- exclude Total rows for per-LoB analysis
  GROUP BY reporting_period, lob_code, lob_name
)
SELECT
    reporting_period,
    lob_code,
    lob_name,

    -- Absolute amounts
    gross_written_premium,
    net_written_premium,
    gross_earned_premium,
    net_earned_premium,
    gross_incurred,
    net_incurred,
    total_expenses,

    -- Key ratios
    ROUND(net_incurred * 100.0 / NULLIF(net_earned_premium, 0), 1)
                                            AS loss_ratio_pct,
    ROUND(total_expenses * 100.0 / NULLIF(net_earned_premium, 0), 1)
                                            AS expense_ratio_pct,
    ROUND((net_incurred + total_expenses) * 100.0 / NULLIF(net_earned_premium, 0), 1)
                                            AS combined_ratio_pct,

    -- Reinsurance cession rate
    ROUND((gross_written_premium - net_written_premium) * 100.0 / NULLIF(gross_written_premium, 0), 1)
                                            AS ri_cession_rate_pct

FROM pivoted
