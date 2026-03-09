# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Generate Synthetic Claims Triangle Data
# MAGIC Generates realistic claims development triangle data for Solvency II Non-Life
# MAGIC underwriting risk reporting (S.26.06 / S.19.01).
# MAGIC
# MAGIC **Tables produced:**
# MAGIC - `bronze_claims_triangles` — incremental and cumulative paid/incurred triangles by LoB and accident year
# MAGIC - `bronze_volume_measures` — premium and reserve volume measures for S.26.06 premium & reserve risk
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog")
dbutils.widgets.text("schema_name", "solvency2demo", "Schema")
dbutils.widgets.text("reporting_date", "2025-12-31", "Reporting Date")
dbutils.widgets.text("entity_name", "Europa Re Insurance SE", "Entity Name")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")
entity_name = dbutils.widgets.get("entity_name")

reporting_year = int(reporting_date[:4])

print(f"Catalog:        {catalog}")
print(f"Schema:         {schema}")
print(f"Reporting date: {reporting_date}")
print(f"Entity:         {entity_name}")
print(f"Reporting year: {reporting_year}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration: Lines of Business and Development Patterns

# COMMAND ----------

import random
import numpy as np
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

SEED = 42
random.seed(SEED)
rng = np.random.RandomState(SEED)

# Lines of Business — codes match the existing pipeline
LOB_CONFIG = {
    1: {
        "name": "Motor vehicle liability",
        "ultimate_base": 100_000_000,  # EUR ~80-120M ultimate per AY
        "tail": "long",
    },
    2: {
        "name": "Motor other",
        "ultimate_base": 45_000_000,
        "tail": "short",
    },
    3: {
        "name": "Marine aviation transport",
        "ultimate_base": 30_000_000,
        "tail": "medium",
    },
    4: {
        "name": "Fire and other property",
        "ultimate_base": 55_000_000,
        "tail": "medium",
    },
    5: {
        "name": "General liability",
        "ultimate_base": 70_000_000,
        "tail": "long",
    },
    6: {
        "name": "Credit and suretyship",
        "ultimate_base": 20_000_000,
        "tail": "medium",
    },
    7: {
        "name": "Legal expenses",
        "ultimate_base": 12_000_000,
        "tail": "short",
    },
    8: {
        "name": "Assistance",
        "ultimate_base": 7_000_000,
        "tail": "short",
    },
}

# Development patterns (cumulative % of ultimate paid at each dev period)
# These are baseline patterns; randomness is added per AY.
DEV_PATTERNS = {
    "long": [0.15, 0.35, 0.52, 0.65, 0.75, 0.83, 0.89, 0.93, 0.96, 0.98],
    "medium": [0.30, 0.55, 0.72, 0.83, 0.90, 0.94, 0.97, 0.985, 0.995, 1.00],
    "short": [0.50, 0.78, 0.90, 0.95, 0.975, 0.99, 0.995, 0.998, 1.00, 1.00],
}

# IBNR buffer: incurred leads paid by this multiplicative factor (decreases over dev periods)
IBNR_FACTORS = {
    "long":   [1.60, 1.45, 1.30, 1.20, 1.12, 1.08, 1.05, 1.03, 1.01, 1.00],
    "medium": [1.40, 1.28, 1.18, 1.10, 1.06, 1.03, 1.02, 1.01, 1.005, 1.00],
    "short":  [1.25, 1.12, 1.06, 1.03, 1.015, 1.005, 1.002, 1.001, 1.00, 1.00],
}

ANNUAL_GROWTH_RATE = 0.03  # 3% annual volume growth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Claims Triangle Data

# COMMAND ----------

rows = []

accident_years = list(range(reporting_year - 10 + 1, reporting_year))  # 10 AYs: 2015-2024 for RY=2025

for lob_code, cfg in LOB_CONFIG.items():
    tail_type = cfg["tail"]
    base_pattern = DEV_PATTERNS[tail_type]
    ibnr_pattern = IBNR_FACTORS[tail_type]
    lob_name = cfg["name"]

    for ay in accident_years:
        # Scale ultimate by growth from base year and add randomness (+/- 15%)
        years_from_start = ay - accident_years[0]
        growth_factor = (1 + ANNUAL_GROWTH_RATE) ** years_from_start
        noise = 1.0 + rng.uniform(-0.15, 0.15)
        ultimate_paid = cfg["ultimate_base"] * growth_factor * noise

        # Maximum development periods available for this AY
        max_dev = min(10, reporting_year - ay)

        cum_paid = 0.0
        cum_incurred = 0.0

        for dev in range(1, max_dev + 1):
            # Cumulative paid fraction with small per-cell noise
            base_cum_frac = base_pattern[dev - 1]
            cell_noise = 1.0 + rng.uniform(-0.03, 0.03)
            target_cum_paid = ultimate_paid * base_cum_frac * cell_noise

            # Ensure monotonically increasing
            if target_cum_paid < cum_paid:
                target_cum_paid = cum_paid + abs(rng.normal(0, ultimate_paid * 0.005))

            inc_paid = round(target_cum_paid - cum_paid, 2)
            cum_paid = round(cum_paid + inc_paid, 2)

            # Incurred: paid * IBNR factor (incurred >= paid always)
            ibnr_noise = 1.0 + rng.uniform(-0.02, 0.02)
            ibnr_mult = ibnr_pattern[dev - 1] * ibnr_noise
            if ibnr_mult < 1.0:
                ibnr_mult = 1.0
            target_cum_incurred = cum_paid * ibnr_mult

            # Ensure incurred is monotonically increasing and >= paid
            if target_cum_incurred < cum_incurred:
                target_cum_incurred = cum_incurred + abs(rng.normal(0, ultimate_paid * 0.002))
            if target_cum_incurred < cum_paid:
                target_cum_incurred = cum_paid

            inc_incurred = round(target_cum_incurred - cum_incurred, 2)
            cum_incurred = round(cum_incurred + inc_incurred, 2)

            rows.append(Row(
                accident_year=int(ay),
                development_period=int(dev),
                lob_code=int(lob_code),
                lob_name=lob_name,
                incremental_paid=round(inc_paid, 2),
                incremental_incurred=round(inc_incurred, 2),
                cumulative_paid=round(cum_paid, 2),
                cumulative_incurred=round(cum_incurred, 2),
            ))

print(f"Generated {len(rows)} triangle cells across {len(LOB_CONFIG)} LoBs and {len(accident_years)} accident years")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write `bronze_claims_triangles`

# COMMAND ----------

triangle_schema = StructType([
    StructField("accident_year", IntegerType(), False),
    StructField("development_period", IntegerType(), False),
    StructField("lob_code", IntegerType(), False),
    StructField("lob_name", StringType(), False),
    StructField("incremental_paid", DoubleType(), False),
    StructField("incremental_incurred", DoubleType(), False),
    StructField("cumulative_paid", DoubleType(), False),
    StructField("cumulative_incurred", DoubleType(), False),
])

df_triangles = spark.createDataFrame(rows, schema=triangle_schema)

(
    df_triangles
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_claims_triangles")
)

print(f"Wrote {df_triangles.count()} rows to {catalog}.{schema}.bronze_claims_triangles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Volume Measures for S.26.06

# COMMAND ----------

volume_rows = []

for lob_code, cfg in LOB_CONFIG.items():
    tail_type = cfg["tail"]
    ultimate = cfg["ultimate_base"] * (1 + ANNUAL_GROWTH_RATE) ** 9  # latest AY level

    # Earned premium: profitable insurer => premium > expected claims (combined ratio ~92-98%)
    combined_ratio = rng.uniform(0.92, 0.98)
    earned_premium_net = round(ultimate / combined_ratio, 2)

    # Written premium next year: slight growth over current earned
    written_premium_net_next_year = round(earned_premium_net * (1 + ANNUAL_GROWTH_RATE) * (1 + rng.uniform(-0.02, 0.05)), 2)

    # Best estimate claims provision: outstanding claims reserve
    # For long-tail, larger reserve relative to premium; for short-tail, smaller
    reserve_factors = {"long": 1.80, "medium": 1.10, "short": 0.55}
    be_claims_provision = round(earned_premium_net * reserve_factors[tail_type] * (1 + rng.uniform(-0.10, 0.10)), 2)

    # Best estimate premium provision: unearned premium reserve portion
    premium_provision_factors = {"long": 0.25, "medium": 0.18, "short": 0.10}
    be_premium_provision = round(earned_premium_net * premium_provision_factors[tail_type] * (1 + rng.uniform(-0.05, 0.05)), 2)

    volume_rows.append(Row(
        lob_code=int(lob_code),
        lob_name=cfg["name"],
        earned_premium_net=earned_premium_net,
        written_premium_net_next_year=written_premium_net_next_year,
        best_estimate_claims_provision=be_claims_provision,
        best_estimate_premium_provision=be_premium_provision,
    ))

volume_schema = StructType([
    StructField("lob_code", IntegerType(), False),
    StructField("lob_name", StringType(), False),
    StructField("earned_premium_net", DoubleType(), False),
    StructField("written_premium_net_next_year", DoubleType(), False),
    StructField("best_estimate_claims_provision", DoubleType(), False),
    StructField("best_estimate_premium_provision", DoubleType(), False),
])

df_volume = spark.createDataFrame(volume_rows, schema=volume_schema)

(
    df_volume
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_volume_measures")
)

print(f"Wrote {df_volume.count()} rows to {catalog}.{schema}.bronze_volume_measures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Lineage

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.{schema}.audit_pipeline_lineage VALUES (
    'sf_pipeline',
    '20_generate_triangle_data',
    1,
    'reporting_date={reporting_date}',
    'bronze_claims_triangles, bronze_volume_measures',
    current_timestamp(),
    '{entity_name}'
)
""")

print("Audit lineage row inserted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

triangle_count = spark.table(f"{catalog}.{schema}.bronze_claims_triangles").count()
volume_count = spark.table(f"{catalog}.{schema}.bronze_volume_measures").count()

print(f"bronze_claims_triangles: {triangle_count} rows")
print(f"bronze_volume_measures:  {volume_count} rows")
print()

# Show triangle sample: cumulative paid for Motor vehicle liability
print("=== Sample: Cumulative Paid — Motor vehicle liability (LoB 1) ===")
display(
    spark.sql(f"""
        SELECT accident_year, development_period,
               cumulative_paid, cumulative_incurred
        FROM {catalog}.{schema}.bronze_claims_triangles
        WHERE lob_code = 1
        ORDER BY accident_year, development_period
    """)
)

# Show volume measures
print("\n=== Volume Measures (all LoBs) ===")
display(
    spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.bronze_volume_measures
        ORDER BY lob_code
    """)
)
