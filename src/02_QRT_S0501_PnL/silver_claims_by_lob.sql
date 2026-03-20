-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Claims by Line of Business
-- MAGIC
-- MAGIC Aggregates raw **claims** transactions to quarterly totals per LoB.
-- MAGIC Gross incurred, paid, reserved — with reinsurers' share and net.
-- MAGIC
-- MAGIC **Source:** `claims`
-- MAGIC **Target:** `claims_by_lob`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_by_lob(
  CONSTRAINT gross_incurred_positive  EXPECT (gross_incurred > 0)           ON VIOLATION DROP ROW,
  CONSTRAINT net_leq_gross            EXPECT (net_incurred <= gross_incurred + 1.0)
)
COMMENT 'Claims aggregation by LoB and quarter — incurred, paid, reserved, gross/RI/net.'
AS
SELECT
    reporting_period,
    lob_code,
    lob_name,
    SUM(gross_incurred)             AS gross_incurred,
    SUM(gross_paid)                 AS gross_paid,
    SUM(gross_reserved)             AS gross_reserved,
    SUM(reinsurers_share_incurred)  AS reinsurers_share_incurred,
    SUM(reinsurers_share_paid)      AS reinsurers_share_paid,
    SUM(net_incurred)               AS net_incurred,
    SUM(net_paid)                   AS net_paid,
    COUNT(*)                        AS claim_count,
    COUNT(CASE WHEN status = 'open' THEN 1 END) AS open_claims
FROM LIVE.claims
GROUP BY reporting_period, lob_code, lob_name
