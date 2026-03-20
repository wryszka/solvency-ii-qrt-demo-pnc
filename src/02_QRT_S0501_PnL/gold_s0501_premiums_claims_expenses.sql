-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: S.05.01 — Premiums, Claims and Expenses by LoB
-- MAGIC
-- MAGIC Maps the three silver tables into the EIOPA S.05.01 QRT template.
-- MAGIC Output is in **long format**: one row per (template_row, line_of_business).
-- MAGIC
-- MAGIC Each `template_row_id` (R0110, R0140, ...) matches the EIOPA template row reference.
-- MAGIC An actuary can verify each mapping against the EIOPA S.05.01 log.
-- MAGIC
-- MAGIC **Sources:** `premiums_by_lob`, `claims_by_lob`, `expenses_by_lob`
-- MAGIC **Target:** `s0501_premiums_claims_expenses`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s0501_premiums_claims_expenses(
  CONSTRAINT row_id_present     EXPECT (template_row_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT amount_not_null    EXPECT (amount_eur IS NOT NULL)         ON VIOLATION DROP ROW
)
COMMENT 'EIOPA S.05.01 Non-Life Premiums, Claims & Expenses — long format (row per template_row × LoB)'
AS

-- === PREMIUMS ===

-- R0110: Premiums written — Gross — Direct Business
SELECT reporting_period, 'R0110' AS template_row_id,
       'Premiums written - Gross - Direct business' AS template_row_label,
       lob_code, lob_name, gross_written_premium AS amount_eur
FROM LIVE.premiums_by_lob

UNION ALL
-- R0110 Total
SELECT reporting_period, 'R0110', 'Premiums written - Gross - Direct business',
       0, 'Total', SUM(gross_written_premium)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

UNION ALL
-- R0140: Premiums written — Reinsurers' share
SELECT reporting_period, 'R0140',
       'Premiums written - Reinsurers share',
       lob_code, lob_name, reinsurers_share_written
FROM LIVE.premiums_by_lob

UNION ALL
SELECT reporting_period, 'R0140', 'Premiums written - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_written)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

UNION ALL
-- R0200: Premiums written — Net
SELECT reporting_period, 'R0200',
       'Premiums written - Net',
       lob_code, lob_name, net_written_premium
FROM LIVE.premiums_by_lob

UNION ALL
SELECT reporting_period, 'R0200', 'Premiums written - Net',
       0, 'Total', SUM(net_written_premium)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

UNION ALL
-- R0210: Premiums earned — Gross
SELECT reporting_period, 'R0210',
       'Premiums earned - Gross - Direct business',
       lob_code, lob_name, gross_earned_premium
FROM LIVE.premiums_by_lob

UNION ALL
SELECT reporting_period, 'R0210', 'Premiums earned - Gross - Direct business',
       0, 'Total', SUM(gross_earned_premium)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

UNION ALL
-- R0240: Premiums earned — Reinsurers' share
SELECT reporting_period, 'R0240',
       'Premiums earned - Reinsurers share',
       lob_code, lob_name, reinsurers_share_earned
FROM LIVE.premiums_by_lob

UNION ALL
SELECT reporting_period, 'R0240', 'Premiums earned - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_earned)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

UNION ALL
-- R0300: Premiums earned — Net
SELECT reporting_period, 'R0300',
       'Premiums earned - Net',
       lob_code, lob_name, net_earned_premium
FROM LIVE.premiums_by_lob

UNION ALL
SELECT reporting_period, 'R0300', 'Premiums earned - Net',
       0, 'Total', SUM(net_earned_premium)
FROM LIVE.premiums_by_lob GROUP BY reporting_period

-- === CLAIMS ===

UNION ALL
-- R0310: Claims incurred — Gross
SELECT reporting_period, 'R0310',
       'Claims incurred - Gross - Direct business',
       lob_code, lob_name, gross_incurred
FROM LIVE.claims_by_lob

UNION ALL
SELECT reporting_period, 'R0310', 'Claims incurred - Gross - Direct business',
       0, 'Total', SUM(gross_incurred)
FROM LIVE.claims_by_lob GROUP BY reporting_period

UNION ALL
-- R0340: Claims incurred — Reinsurers' share
SELECT reporting_period, 'R0340',
       'Claims incurred - Reinsurers share',
       lob_code, lob_name, reinsurers_share_incurred
FROM LIVE.claims_by_lob

UNION ALL
SELECT reporting_period, 'R0340', 'Claims incurred - Reinsurers share',
       0, 'Total', SUM(reinsurers_share_incurred)
FROM LIVE.claims_by_lob GROUP BY reporting_period

UNION ALL
-- R0400: Claims incurred — Net
SELECT reporting_period, 'R0400',
       'Claims incurred - Net',
       lob_code, lob_name, net_incurred
FROM LIVE.claims_by_lob

UNION ALL
SELECT reporting_period, 'R0400', 'Claims incurred - Net',
       0, 'Total', SUM(net_incurred)
FROM LIVE.claims_by_lob GROUP BY reporting_period

UNION ALL
-- R0410: Claims paid — Gross
SELECT reporting_period, 'R0410',
       'Claims paid - Gross - Direct business',
       lob_code, lob_name, gross_paid
FROM LIVE.claims_by_lob

UNION ALL
SELECT reporting_period, 'R0410', 'Claims paid - Gross - Direct business',
       0, 'Total', SUM(gross_paid)
FROM LIVE.claims_by_lob GROUP BY reporting_period

UNION ALL
-- R0500: Claims paid — Net
SELECT reporting_period, 'R0500',
       'Claims paid - Net',
       lob_code, lob_name, net_paid
FROM LIVE.claims_by_lob

UNION ALL
SELECT reporting_period, 'R0500', 'Claims paid - Net',
       0, 'Total', SUM(net_paid)
FROM LIVE.claims_by_lob GROUP BY reporting_period

-- === EXPENSES ===

UNION ALL
-- R0550: Expenses incurred (total)
SELECT reporting_period, 'R0550',
       'Expenses incurred',
       lob_code, lob_name, total_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0550', 'Expenses incurred',
       0, 'Total', SUM(total_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R0610: Administrative expenses
SELECT reporting_period, 'R0610',
       'Administrative expenses',
       lob_code, lob_name, administrative_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0610', 'Administrative expenses',
       0, 'Total', SUM(administrative_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R0620: Investment management expenses
SELECT reporting_period, 'R0620',
       'Investment management expenses',
       lob_code, lob_name, investment_management_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0620', 'Investment management expenses',
       0, 'Total', SUM(investment_management_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R0630: Claims management expenses
SELECT reporting_period, 'R0630',
       'Claims management expenses',
       lob_code, lob_name, claims_management_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0630', 'Claims management expenses',
       0, 'Total', SUM(claims_management_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R0640: Acquisition expenses
SELECT reporting_period, 'R0640',
       'Acquisition expenses',
       lob_code, lob_name, acquisition_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0640', 'Acquisition expenses',
       0, 'Total', SUM(acquisition_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R0680: Overhead expenses
SELECT reporting_period, 'R0680',
       'Overhead expenses',
       lob_code, lob_name, overhead_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R0680', 'Overhead expenses',
       0, 'Total', SUM(overhead_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period

UNION ALL
-- R1200: Other expenses
SELECT reporting_period, 'R1200',
       'Other expenses',
       lob_code, lob_name, other_expenses
FROM LIVE.expenses_by_lob

UNION ALL
SELECT reporting_period, 'R1200', 'Other expenses',
       0, 'Total', SUM(other_expenses)
FROM LIVE.expenses_by_lob GROUP BY reporting_period
