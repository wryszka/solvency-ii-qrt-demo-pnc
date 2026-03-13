-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Premiums, Claims and Expenses by Line of Business
-- MAGIC Aggregates bronze transaction tables into LoB-level summaries for S.05.01.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Premiums by LoB

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_premiums_by_lob(
  CONSTRAINT has_rows EXPECT (gross_premiums_written IS NOT NULL)
)
COMMENT 'Gross/net/reinsurer premiums written and earned by line of business'
AS
SELECT
    p.reporting_period,
    pol.lob_code,
    pol.line_of_business,
    SUM(CASE WHEN p.transaction_type = 'written' THEN p.gross_amount_eur ELSE 0 END) AS gross_premiums_written,
    SUM(CASE WHEN p.transaction_type = 'earned'  THEN p.gross_amount_eur ELSE 0 END) AS gross_premiums_earned,
    SUM(CASE WHEN p.transaction_type = 'written' THEN p.reinsurance_amount_eur ELSE 0 END) AS reinsurers_share_written,
    SUM(CASE WHEN p.transaction_type = 'earned'  THEN p.reinsurance_amount_eur ELSE 0 END) AS reinsurers_share_earned,
    SUM(CASE WHEN p.transaction_type = 'written' THEN p.gross_amount_eur - p.reinsurance_amount_eur ELSE 0 END) AS net_premiums_written,
    SUM(CASE WHEN p.transaction_type = 'earned'  THEN p.gross_amount_eur - p.reinsurance_amount_eur ELSE 0 END) AS net_premiums_earned
FROM LIVE.bronze_premiums_transactions p
JOIN LIVE.bronze_policies pol ON p.policy_id = pol.policy_id
WHERE p.transaction_type IN ('written', 'earned')
GROUP BY p.reporting_period, pol.lob_code, pol.line_of_business

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Claims by LoB

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_claims_by_lob(
  CONSTRAINT has_rows EXPECT (gross_claims_incurred IS NOT NULL)
)
COMMENT 'Gross/net/reinsurer claims incurred, paid, and reserve changes by line of business'
AS
SELECT
    c.reporting_period,
    pol.lob_code,
    pol.line_of_business,
    SUM(CASE WHEN c.transaction_type = 'incurred'       THEN c.gross_amount_eur ELSE 0 END) AS gross_claims_incurred,
    SUM(CASE WHEN c.transaction_type = 'paid'            THEN c.gross_amount_eur ELSE 0 END) AS gross_claims_paid,
    SUM(CASE WHEN c.transaction_type = 'reserve_change'  THEN c.gross_amount_eur ELSE 0 END) AS gross_reserve_changes,
    SUM(CASE WHEN c.transaction_type = 'incurred'       THEN c.reinsurance_recovery_eur ELSE 0 END) AS reinsurers_share_incurred,
    SUM(CASE WHEN c.transaction_type = 'paid'            THEN c.reinsurance_recovery_eur ELSE 0 END) AS reinsurers_share_paid,
    SUM(CASE WHEN c.transaction_type = 'reserve_change'  THEN c.reinsurance_recovery_eur ELSE 0 END) AS reinsurers_share_reserve_changes,
    SUM(CASE WHEN c.transaction_type = 'incurred'       THEN c.gross_amount_eur - c.reinsurance_recovery_eur ELSE 0 END) AS net_claims_incurred,
    SUM(CASE WHEN c.transaction_type = 'paid'            THEN c.gross_amount_eur - c.reinsurance_recovery_eur ELSE 0 END) AS net_claims_paid,
    SUM(CASE WHEN c.transaction_type = 'reserve_change'  THEN c.gross_amount_eur - c.reinsurance_recovery_eur ELSE 0 END) AS net_reserve_changes
FROM LIVE.bronze_claims_transactions c
JOIN LIVE.bronze_policies pol ON c.policy_id = pol.policy_id
GROUP BY c.reporting_period, pol.lob_code, pol.line_of_business

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Expenses by LoB

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_expenses_by_lob(
  CONSTRAINT has_rows EXPECT (total_expenses IS NOT NULL)
)
COMMENT 'Expense breakdown by category and line of business'
AS
SELECT
    e.reporting_period,
    e.lob_code,
    e.line_of_business,
    SUM(CASE WHEN e.expense_category = 'acquisition'        THEN e.gross_amount_eur ELSE 0 END) AS acquisition_expenses,
    SUM(CASE WHEN e.expense_category = 'administrative'     THEN e.gross_amount_eur ELSE 0 END) AS administrative_expenses,
    SUM(CASE WHEN e.expense_category = 'claims_management'  THEN e.gross_amount_eur ELSE 0 END) AS claims_management_expenses,
    SUM(CASE WHEN e.expense_category = 'overhead'           THEN e.gross_amount_eur ELSE 0 END) AS overhead_expenses,
    SUM(CASE WHEN e.expense_category = 'investment'         THEN e.gross_amount_eur ELSE 0 END) AS investment_management_expenses,
    SUM(CASE WHEN e.expense_category = 'other'              THEN e.gross_amount_eur ELSE 0 END) AS other_expenses,
    SUM(e.gross_amount_eur) AS total_expenses
FROM LIVE.bronze_expenses e
GROUP BY e.reporting_period, e.lob_code, e.line_of_business
