-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Expenses by Line of Business
-- MAGIC
-- MAGIC Reads the **expenses** allocation table. Already at LoB level,
-- MAGIC but this view adds validation and ensures consistency with the premium base.
-- MAGIC
-- MAGIC **Source:** `expenses`
-- MAGIC **Target:** `expenses_by_lob`

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW expenses_by_lob(
  CONSTRAINT total_expenses_positive  EXPECT (total_expenses > 0)           ON VIOLATION DROP ROW,
  CONSTRAINT components_sum_to_total  EXPECT (
    ABS(total_expenses - (acquisition_expenses + administrative_expenses + claims_management_expenses + overhead_expenses + investment_management_expenses + other_expenses)) < 1.0
  )
)
COMMENT 'Expense allocation by LoB and quarter — acquisition, admin, claims mgmt, overhead, investment mgmt.'
AS
SELECT
    reporting_period,
    lob_code,
    lob_name,
    acquisition_expenses,
    administrative_expenses,
    claims_management_expenses,
    overhead_expenses,
    investment_management_expenses,
    other_expenses,
    total_expenses
FROM LIVE.expenses
