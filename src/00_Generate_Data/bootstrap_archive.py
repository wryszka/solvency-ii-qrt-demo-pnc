# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap Archive Data (Q1–Q3)
# MAGIC
# MAGIC Generates synthetic data for Q1, Q2, Q3 of the reporting year.
# MAGIC These quarters form the "previously completed" QRT archive that exists
# MAGIC before the live demo begins.
# MAGIC
# MAGIC **Run once during initial deployment.** Q4 is left for the live demo.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_year", "2025")
dbutils.widgets.text("entity_name", "Bricksurance SE")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_year = dbutils.widgets.get("reporting_year")
entity_name = dbutils.widgets.get("entity_name")

print(f"Bootstrapping archive for {reporting_year} Q1–Q3")
print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data for Q1, Q2, Q3

# COMMAND ----------

for quarter in [1, 2, 3]:
    rp = f"{reporting_year}-Q{quarter}"
    print(f"\n{'='*70}")
    print(f"  Generating data for {rp}")
    print(f"{'='*70}\n")

    dbutils.notebook.run(
        "./generate_data",
        timeout_seconds=600,
        arguments={
            "catalog_name": catalog,
            "schema_name": schema,
            "reporting_period": rp,
            "mode": "full_reset" if quarter == 1 else "append",
            "entity_name": entity_name,
        }
    )
    print(f"  {rp} complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

tables = [
    "counterparties", "assets", "policies", "premiums", "claims", "expenses",
    "reinsurance", "claims_triangles", "risk_factors", "scr_parameters",
    "volume_measures", "exposures", "igloo_results", "own_funds", "balance_sheet",
]

print("=" * 70)
print("  ARCHIVE DATA SUMMARY")
print("=" * 70)

for t in tables:
    try:
        total = spark.table(t).count()
        # Count distinct reporting periods for tables that have the column
        try:
            periods = [r["reporting_period"] for r in spark.sql(f"SELECT DISTINCT reporting_period FROM {t} ORDER BY reporting_period").collect()]
            print(f"  {t:30s} {total:>10,} rows  periods: {', '.join(periods)}")
        except Exception:
            print(f"  {t:30s} {total:>10,} rows  (master table)")
    except Exception:
        print(f"  {t:30s} NOT FOUND")

print("=" * 70)
print("  Archive bootstrap complete. Q4 is ready for live demo.")
print("=" * 70)
