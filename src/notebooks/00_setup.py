# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Schemas and Volumes
# MAGIC Creates the medallion layer schemas and export volume for the Solvency II QRT demo.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
catalog = dbutils.widgets.get("catalog_name")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# Bronze - raw synthetic source data
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_bronze")

# Silver - cleansed and aggregated
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_silver")

# Gold - EIOPA QRT-aligned output tables
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_gold")

# COMMAND ----------

# Export volume for Tagetik-ready CSV files
spark.sql("CREATE VOLUME IF NOT EXISTS qrt_demo_gold.exports")

# COMMAND ----------

print(f"Setup complete for catalog: {catalog}")
print("Schemas: qrt_demo_bronze, qrt_demo_silver, qrt_demo_gold")
print("Volume: qrt_demo_gold.exports")
