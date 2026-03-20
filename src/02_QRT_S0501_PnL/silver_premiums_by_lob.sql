-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Premiums by Line of Business
-- MAGIC
-- MAGIC Aggregates raw **premiums** transactions to quarterly totals per LoB.
-- MAGIC Gross, reinsurers' share, and net — reconciled automatically.
-- MAGIC
-- MAGIC **Source:** `premiums`
-- MAGIC **Target:** `premiums_by_lob`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW premiums_by_lob(
  CONSTRAINT gross_written_positive    EXPECT (gross_written_premium > 0)          ON VIOLATION DROP ROW,
  CONSTRAINT net_equals_gross_minus_ri EXPECT (ABS(net_written_premium - (gross_written_premium - reinsurers_share_written)) < 1.0)
)
COMMENT 'Premium aggregation by LoB and quarter — gross, RI share, net. Reconciled: net = gross - RI.'
AS
SELECT
    reporting_period,
    lob_code,
    lob_name,
    SUM(gross_written_premium)      AS gross_written_premium,
    SUM(gross_earned_premium)       AS gross_earned_premium,
    SUM(reinsurers_share_written)   AS reinsurers_share_written,
    SUM(reinsurers_share_earned)    AS reinsurers_share_earned,
    SUM(net_written_premium)        AS net_written_premium,
    SUM(net_earned_premium)         AS net_earned_premium,
    COUNT(*)                        AS transaction_count
FROM LIVE.premiums
GROUP BY reporting_period, lob_code, lob_name
