# Databricks notebook source
# MAGIC %md
# MAGIC # Teardown — Remove All Demo Data
# MAGIC
# MAGIC Drops the entire schema and all tables. Use to clean up after a demo.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "solvency2_qrt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print(f"WARNING: This will drop {catalog}.{schema} and ALL tables/volumes within it.")

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
print(f"Schema {catalog}.{schema} dropped successfully.")
