-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.05.01 — Non-Life Premiums, Claims and Expenses (EIOPA QRT)
-- MAGIC Maps silver aggregations to EIOPA template rows R0110–R1300 in long format.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_qrt_s0501(
  CONSTRAINT template_row_id_present EXPECT (template_row_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT amount_not_null EXPECT (amount_eur IS NOT NULL)
)
COMMENT 'EIOPA S.05.01 Non-Life Premiums, Claims and Expenses QRT in long format'
AS

-- R0110: Premiums written - Gross - Direct business
SELECT CURRENT_DATE() AS reporting_reference_date,
       'R0110' AS template_row_id,
       'Premiums written - Gross - Direct business' AS template_row_label,
       lob_code, line_of_business AS lob_label,
       gross_premiums_written AS amount_eur
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0110', 'Premiums written - Gross - Direct business',
       0, 'Total', SUM(gross_premiums_written)
FROM LIVE.silver_premiums_by_lob

-- R0120: Premiums written - Gross - Proportional reinsurance accepted (zero for direct insurer)
UNION ALL
SELECT CURRENT_DATE(), 'R0120', 'Premiums written - Gross - Proportional reinsurance accepted',
       lob_code, line_of_business, 0
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0120', 'Premiums written - Gross - Proportional reinsurance accepted',
       0, 'Total', 0

-- R0140: Premiums written - Reinsurers share
UNION ALL
SELECT CURRENT_DATE(), 'R0140', 'Premiums written - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_written
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0140', 'Premiums written - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_written)
FROM LIVE.silver_premiums_by_lob

-- R0200: Premiums written - Net
UNION ALL
SELECT CURRENT_DATE(), 'R0200', 'Premiums written - Net',
       lob_code, line_of_business, net_premiums_written
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0200', 'Premiums written - Net',
       0, 'Total', SUM(net_premiums_written)
FROM LIVE.silver_premiums_by_lob

-- R0210: Premiums earned - Gross - Direct business
UNION ALL
SELECT CURRENT_DATE(), 'R0210', 'Premiums earned - Gross - Direct business',
       lob_code, line_of_business, gross_premiums_earned
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0210', 'Premiums earned - Gross - Direct business',
       0, 'Total', SUM(gross_premiums_earned)
FROM LIVE.silver_premiums_by_lob

-- R0300: Premiums earned - Net
UNION ALL
SELECT CURRENT_DATE(), 'R0300', 'Premiums earned - Net',
       lob_code, line_of_business, net_premiums_earned
FROM LIVE.silver_premiums_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0300', 'Premiums earned - Net',
       0, 'Total', SUM(net_premiums_earned)
FROM LIVE.silver_premiums_by_lob

-- R0310: Claims incurred - Gross - Direct business
UNION ALL
SELECT CURRENT_DATE(), 'R0310', 'Claims incurred - Gross - Direct business',
       lob_code, line_of_business, gross_claims_incurred
FROM LIVE.silver_claims_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0310', 'Claims incurred - Gross - Direct business',
       0, 'Total', SUM(gross_claims_incurred)
FROM LIVE.silver_claims_by_lob

-- R0370: Claims incurred - Reinsurers share
UNION ALL
SELECT CURRENT_DATE(), 'R0370', 'Claims incurred - Reinsurers share',
       lob_code, line_of_business, reinsurers_share_incurred
FROM LIVE.silver_claims_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0370', 'Claims incurred - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_incurred)
FROM LIVE.silver_claims_by_lob

-- R0400: Claims incurred - Net
UNION ALL
SELECT CURRENT_DATE(), 'R0400', 'Claims incurred - Net',
       lob_code, line_of_business, net_claims_incurred
FROM LIVE.silver_claims_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0400', 'Claims incurred - Net',
       0, 'Total', SUM(net_claims_incurred)
FROM LIVE.silver_claims_by_lob

-- R0550: Expenses incurred
UNION ALL
SELECT CURRENT_DATE(), 'R0550', 'Expenses incurred',
       lob_code, line_of_business, total_expenses
FROM LIVE.silver_expenses_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R0550', 'Expenses incurred',
       0, 'Total', SUM(total_expenses)
FROM LIVE.silver_expenses_by_lob

-- R1200: Other expenses
UNION ALL
SELECT CURRENT_DATE(), 'R1200', 'Other expenses',
       lob_code, line_of_business, other_expenses
FROM LIVE.silver_expenses_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R1200', 'Other expenses',
       0, 'Total', SUM(other_expenses)
FROM LIVE.silver_expenses_by_lob

-- R1300: Total expenses
UNION ALL
SELECT CURRENT_DATE(), 'R1300', 'Total expenses',
       lob_code, line_of_business, total_expenses
FROM LIVE.silver_expenses_by_lob

UNION ALL
SELECT CURRENT_DATE(), 'R1300', 'Total expenses',
       0, 'Total', SUM(total_expenses)
FROM LIVE.silver_expenses_by_lob
