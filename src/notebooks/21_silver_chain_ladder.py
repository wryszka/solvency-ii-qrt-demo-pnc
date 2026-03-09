# Databricks notebook source
# MAGIC %md
# MAGIC # 21_silver_chain_ladder — Chain-ladder actuarial reserving on claims triangles
# MAGIC
# MAGIC Reads `bronze_claims_triangles` and performs the chain-ladder method to produce:
# MAGIC 1. **silver_development_factors** — age-to-age (link) ratios by LoB
# MAGIC 2. **silver_ultimate_claims** — ultimate claims, IBNR, and case reserves by accident year and LoB
# MAGIC 3. **silver_best_estimate** — best estimate of technical provisions by LoB (claims provision,
# MAGIC    premium provision, risk margin)
# MAGIC
# MAGIC **Method:** Volume-weighted chain-ladder with exponential-decay tail extrapolation.
# MAGIC
# MAGIC **Lineage:** Logged to `audit_pipeline_lineage`.

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog")
dbutils.widgets.text("schema_name", "solvency2demo", "Schema")
dbutils.widgets.text("reporting_date", "2025-12-31", "Reporting Date")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")

from datetime import datetime
_rd = datetime.strptime(reporting_date, "%Y-%m-%d")
reporting_period = f"{_rd.year}-Q{(_rd.month - 1) // 3 + 1}"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Reporting period: {reporting_period}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise Lineage Logger

# COMMAND ----------

lineage = LineageLogger(spark, catalog, schema, reporting_period)
step_name = "silver_chain_ladder"
lineage.start_step(step_name)

source_table = f"{catalog}.{schema}.bronze_claims_triangles"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

df_bronze = spark.table(source_table)
row_count_in = df_bronze.count()
columns_in = df_bronze.columns

print(f"bronze_claims_triangles: {row_count_in} rows, {len(columns_in)} columns")
df_bronze.createOrReplaceTempView("v_claims_triangles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Compute Development Factors (Age-to-Age Link Ratios)
# MAGIC
# MAGIC Volume-weighted average link ratios: for each LoB and transition from_period -> to_period,
# MAGIC the factor is SUM(cumulative[to]) / SUM(cumulative[from]) across all accident years that
# MAGIC have observations at both development periods.

# COMMAND ----------

import math
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Collect triangle data for Python-side chain-ladder calculations
triangle_rows = df_bronze.collect()

# Organise data: {(lob_code, lob_name, accident_year, dev_period): {paid, incurred}}
triangle_data = {}
lob_info = {}  # lob_code -> lob_name

for r in triangle_rows:
    key = (r["lob_code"], r["accident_year"], r["development_period"])
    triangle_data[key] = {
        "cum_paid": r["cumulative_paid"],
        "cum_incurred": r["cumulative_incurred"],
    }
    lob_info[r["lob_code"]] = r["lob_name"]

lob_codes = sorted(lob_info.keys())

# Determine the range of accident years and dev periods
all_accident_years = sorted(set(k[1] for k in triangle_data.keys()))
max_dev_period = max(k[2] for k in triangle_data.keys())

print(f"Lines of Business:  {len(lob_codes)}")
print(f"Accident years:     {min(all_accident_years)} - {max(all_accident_years)}")
print(f"Max dev period:     {max_dev_period}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute volume-weighted link ratios

# COMMAND ----------

# Compute link ratios per LoB
# link_ratio[lob_code][(from_period, to_period)] = {ratio_paid, ratio_incurred, num_ay}
link_ratios = {}

for lob in lob_codes:
    link_ratios[lob] = {}
    for from_p in range(1, max_dev_period):
        to_p = from_p + 1
        sum_from_paid = 0.0
        sum_to_paid = 0.0
        sum_from_incurred = 0.0
        sum_to_incurred = 0.0
        n_ay = 0

        for ay in all_accident_years:
            key_from = (lob, ay, from_p)
            key_to = (lob, ay, to_p)
            if key_from in triangle_data and key_to in triangle_data:
                sum_from_paid += triangle_data[key_from]["cum_paid"]
                sum_to_paid += triangle_data[key_to]["cum_paid"]
                sum_from_incurred += triangle_data[key_from]["cum_incurred"]
                sum_to_incurred += triangle_data[key_to]["cum_incurred"]
                n_ay += 1

        if n_ay > 0 and sum_from_paid > 0 and sum_from_incurred > 0:
            link_ratios[lob][(from_p, to_p)] = {
                "ratio_paid": sum_to_paid / sum_from_paid,
                "ratio_incurred": sum_to_incurred / sum_from_incurred,
                "num_ay": n_ay,
            }

print("Link ratios computed for all LoB / dev period transitions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tail factor extrapolation (exponential decay beyond observed triangle)
# MAGIC
# MAGIC Fit an exponential decay to the last 3 observed factors and extrapolate
# MAGIC from the last observed period to an ultimate period (period 15).

# COMMAND ----------

ULTIMATE_PERIOD = 15  # extrapolate to this period

tail_factors = {}  # lob_code -> {tail_paid, tail_incurred}

for lob in lob_codes:
    for claim_type in ["paid", "incurred"]:
        ratio_key = f"ratio_{claim_type}"

        # Get the last 3 observed factors (sorted by from_period descending)
        observed = []
        for from_p in range(max_dev_period - 1, 0, -1):
            key = (from_p, from_p + 1)
            if key in link_ratios[lob]:
                observed.append((from_p, link_ratios[lob][key][ratio_key]))
            if len(observed) == 3:
                break

        observed.reverse()  # chronological order

        # Exponential decay: factor_excess(t) = (factor(t) - 1) decays exponentially
        # f(t) - 1 = a * exp(-b * t)
        # Use log-linear regression on the last 3 excess factors
        tail_product = 1.0
        if len(observed) >= 2:
            excesses = [(p, max(f - 1.0, 1e-10)) for p, f in observed if f > 1.0]

            if len(excesses) >= 2:
                # Log-linear fit: ln(excess) = ln(a) - b * period
                n = len(excesses)
                sum_x = sum(p for p, _ in excesses)
                sum_y = sum(math.log(e) for _, e in excesses)
                sum_xy = sum(p * math.log(e) for p, e in excesses)
                sum_x2 = sum(p * p for p, _ in excesses)

                denom = n * sum_x2 - sum_x * sum_x
                if abs(denom) > 1e-12:
                    b = -(n * sum_xy - sum_x * sum_y) / denom  # decay rate
                    ln_a = (sum_y + b * sum_x) / n
                    a = math.exp(ln_a)

                    # Ensure decay (b > 0); if not, use simple average
                    if b > 0:
                        for ext_p in range(max_dev_period, ULTIMATE_PERIOD):
                            extrapolated_excess = a * math.exp(-b * ext_p)
                            extrapolated_factor = 1.0 + max(extrapolated_excess, 0.0)
                            tail_product *= extrapolated_factor
                    else:
                        # Flat extrapolation using last observed factor
                        last_factor = observed[-1][1]
                        remaining = ULTIMATE_PERIOD - max_dev_period
                        # Dampen: use sqrt of excess to avoid over-projection
                        dampened = 1.0 + max((last_factor - 1.0) * 0.5, 0.0)
                        tail_product = dampened ** remaining
                else:
                    tail_product = 1.0
            else:
                # Not enough excess factors > 1; tail = 1.0
                tail_product = 1.0
        else:
            tail_product = 1.0

        # Floor at 1.0 (no negative development)
        tail_product = max(tail_product, 1.0)

        if lob not in tail_factors:
            tail_factors[lob] = {}
        tail_factors[lob][f"tail_{claim_type}"] = tail_product

print("Tail factors by LoB:")
for lob in lob_codes:
    print(f"  {lob} ({lob_info[lob]}): paid={tail_factors[lob]['tail_paid']:.6f}, "
          f"incurred={tail_factors[lob]['tail_incurred']:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write silver_development_factors

# COMMAND ----------

dev_factor_rows = []
for lob in lob_codes:
    for from_p in range(1, max_dev_period):
        to_p = from_p + 1
        key = (from_p, to_p)
        if key in link_ratios[lob]:
            lr = link_ratios[lob][key]
            dev_factor_rows.append(Row(
                lob_code=int(lob),
                lob_name=lob_info[lob],
                from_period=from_p,
                to_period=to_p,
                link_ratio_paid=float(lr["ratio_paid"]),
                link_ratio_incurred=float(lr["ratio_incurred"]),
                num_accident_years=lr["num_ay"],
            ))

dev_factors_schema = StructType([
    StructField("lob_code", IntegerType(), False),
    StructField("lob_name", StringType(), False),
    StructField("from_period", IntegerType(), False),
    StructField("to_period", IntegerType(), False),
    StructField("link_ratio_paid", DoubleType(), False),
    StructField("link_ratio_incurred", DoubleType(), False),
    StructField("num_accident_years", IntegerType(), False),
])

df_dev_factors = spark.createDataFrame(dev_factor_rows, schema=dev_factors_schema)

target_dev_factors = f"{catalog}.{schema}.silver_development_factors"
(
    df_dev_factors.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_dev_factors)
)

dev_factors_count = spark.table(target_dev_factors).count()
print(f"silver_development_factors written: {dev_factors_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ultimate Claims Projection
# MAGIC
# MAGIC For each accident year and LoB:
# MAGIC - Find the latest observed development period
# MAGIC - Compute the cumulative-to-ultimate factor (product of remaining link ratios including tail)
# MAGIC - Project ultimate paid and ultimate incurred
# MAGIC - Derive IBNR and case reserves

# COMMAND ----------

ultimate_rows = []

for lob in lob_codes:
    for ay in all_accident_years:
        # Find the latest observed dev period for this AY/LoB
        latest_dev = 0
        for dp in range(1, max_dev_period + 1):
            if (lob, ay, dp) in triangle_data:
                latest_dev = dp

        if latest_dev == 0:
            continue

        latest_paid = triangle_data[(lob, ay, latest_dev)]["cum_paid"]
        latest_incurred = triangle_data[(lob, ay, latest_dev)]["cum_incurred"]

        # Cumulative-to-ultimate factor = product of link ratios from latest_dev to max_dev
        # then multiply by tail factor
        cum_to_ult_paid = 1.0
        cum_to_ult_incurred = 1.0

        for from_p in range(latest_dev, max_dev_period):
            to_p = from_p + 1
            key = (from_p, to_p)
            if key in link_ratios[lob]:
                cum_to_ult_paid *= link_ratios[lob][key]["ratio_paid"]
                cum_to_ult_incurred *= link_ratios[lob][key]["ratio_incurred"]
            # If no link ratio available (shouldn't happen for valid triangle), factor stays 1.0

        # Apply tail factor beyond observed triangle
        cum_to_ult_paid *= tail_factors[lob]["tail_paid"]
        cum_to_ult_incurred *= tail_factors[lob]["tail_incurred"]

        ultimate_paid = latest_paid * cum_to_ult_paid
        ultimate_incurred = latest_incurred * cum_to_ult_incurred
        ibnr = ultimate_paid - latest_paid
        case_reserves = latest_incurred - latest_paid

        ultimate_rows.append(Row(
            accident_year=int(ay),
            lob_code=int(lob),
            lob_name=lob_info[lob],
            latest_dev_period=latest_dev,
            latest_cumulative_paid=float(latest_paid),
            latest_cumulative_incurred=float(latest_incurred),
            cum_to_ult_factor_paid=float(cum_to_ult_paid),
            cum_to_ult_factor_incurred=float(cum_to_ult_incurred),
            ultimate_paid=float(ultimate_paid),
            ultimate_incurred=float(ultimate_incurred),
            ibnr=float(ibnr),
            case_reserves=float(case_reserves),
        ))

ultimate_schema = StructType([
    StructField("accident_year", IntegerType(), False),
    StructField("lob_code", IntegerType(), False),
    StructField("lob_name", StringType(), False),
    StructField("latest_dev_period", IntegerType(), False),
    StructField("latest_cumulative_paid", DoubleType(), False),
    StructField("latest_cumulative_incurred", DoubleType(), False),
    StructField("cum_to_ult_factor_paid", DoubleType(), False),
    StructField("cum_to_ult_factor_incurred", DoubleType(), False),
    StructField("ultimate_paid", DoubleType(), False),
    StructField("ultimate_incurred", DoubleType(), False),
    StructField("ibnr", DoubleType(), False),
    StructField("case_reserves", DoubleType(), False),
])

df_ultimate = spark.createDataFrame(ultimate_rows, schema=ultimate_schema)

target_ultimate = f"{catalog}.{schema}.silver_ultimate_claims"
(
    df_ultimate.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_ultimate)
)

ultimate_count = spark.table(target_ultimate).count()
print(f"silver_ultimate_claims written: {ultimate_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Best Estimate of Technical Provisions
# MAGIC
# MAGIC For each LoB:
# MAGIC - **Claims provision** = SUM(IBNR) across all accident years
# MAGIC - **Premium provision** = 5% of latest accident year earned premium (simplified proxy)
# MAGIC - **Risk margin** = 6% x 8% x claims_provision x avg_remaining_duration (simplified CoC approach)
# MAGIC - **Total technical provisions** = claims_provision + premium_provision + risk_margin

# COMMAND ----------

df_ultimate.createOrReplaceTempView("v_ultimate_claims")

# Aggregate IBNR by LoB
lob_ibnr = {}
lob_latest_ay_paid = {}
lob_remaining_duration = {}

for lob in lob_codes:
    total_ibnr = 0.0
    weighted_remaining = 0.0
    total_weight = 0.0
    latest_ay_paid = 0.0

    for row in ultimate_rows:
        if row.lob_code == lob:
            total_ibnr += row.ibnr
            # Remaining dev periods = max_dev_period - latest_dev_period + tail periods
            remaining = (max_dev_period - row.latest_dev_period) + (ULTIMATE_PERIOD - max_dev_period)
            weighted_remaining += remaining * abs(row.ibnr)
            total_weight += abs(row.ibnr)

    lob_ibnr[lob] = total_ibnr

    # Average remaining settlement duration (IBNR-weighted)
    if total_weight > 0:
        lob_remaining_duration[lob] = weighted_remaining / total_weight
    else:
        lob_remaining_duration[lob] = 1.0

    # Latest accident year earned premium proxy: use the latest AY's ultimate paid
    # (as a simplified stand-in for earned premium)
    latest_ay = max(all_accident_years)
    key = (lob, latest_ay, 1)  # dev period 1 of latest AY
    if key in triangle_data:
        latest_ay_paid = triangle_data[key]["cum_paid"]
    lob_latest_ay_paid[lob] = latest_ay_paid

# Build best estimate rows
best_estimate_rows = []

for lob in lob_codes:
    claims_prov = lob_ibnr[lob]
    # Premium provision: 5% of latest AY earned premium (simplified proxy)
    premium_prov = 0.05 * lob_latest_ay_paid[lob]

    # Risk margin: CoC approach (simplified)
    # risk_margin = 6% * SCR_proxy * avg_remaining_duration
    # SCR_proxy = 8% of best_estimate_claims_prov
    scr_proxy = 0.08 * abs(claims_prov)
    avg_duration = lob_remaining_duration[lob]
    risk_margin = 0.06 * scr_proxy * avg_duration

    total_tp = claims_prov + premium_prov + risk_margin

    best_estimate_rows.append(Row(
        lob_code=int(lob),
        lob_name=lob_info[lob],
        best_estimate_claims_prov=float(claims_prov),
        best_estimate_premium_prov=float(premium_prov),
        risk_margin=float(risk_margin),
        total_technical_provisions=float(total_tp),
    ))

best_estimate_schema = StructType([
    StructField("lob_code", IntegerType(), False),
    StructField("lob_name", StringType(), False),
    StructField("best_estimate_claims_prov", DoubleType(), False),
    StructField("best_estimate_premium_prov", DoubleType(), False),
    StructField("risk_margin", DoubleType(), False),
    StructField("total_technical_provisions", DoubleType(), False),
])

df_best_estimate = spark.createDataFrame(best_estimate_rows, schema=best_estimate_schema)

target_best_estimate = f"{catalog}.{schema}.silver_best_estimate"
(
    df_best_estimate.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_best_estimate)
)

best_estimate_count = spark.table(target_best_estimate).count()
print(f"silver_best_estimate written: {best_estimate_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Lineage

# COMMAND ----------

target_tables = [target_dev_factors, target_ultimate, target_best_estimate]
total_rows_out = dev_factors_count + ultimate_count + best_estimate_count

lineage.log_lineage(
    step_name=step_name,
    step_sequence=21,
    source_tables=[source_table],
    target_table=", ".join(target_tables),
    transformation_type="actuarial_calculation",
    transformation_desc=(
        "Chain-ladder reserving method on claims triangles. "
        "Volume-weighted age-to-age link ratios with exponential-decay tail extrapolation to ultimate (period 15). "
        "Projects ultimate paid/incurred, derives IBNR and case reserves per accident year and LoB. "
        "Aggregates to best estimate technical provisions (claims provision, premium provision, risk margin) per LoB."
    ),
    row_count_in=row_count_in,
    row_count_out=total_rows_out,
    columns_in=columns_in,
    columns_out=[
        "lob_code", "lob_name", "from_period", "to_period", "link_ratio_paid",
        "link_ratio_incurred", "num_accident_years", "accident_year",
        "latest_dev_period", "latest_cumulative_paid", "latest_cumulative_incurred",
        "cum_to_ult_factor_paid", "cum_to_ult_factor_incurred", "ultimate_paid",
        "ultimate_incurred", "ibnr", "case_reserves", "best_estimate_claims_prov",
        "best_estimate_premium_prov", "risk_margin", "total_technical_provisions",
    ],
    parameters={
        "reporting_date": reporting_date,
        "reporting_period": reporting_period,
        "ultimate_period": ULTIMATE_PERIOD,
        "tail_method": "exponential_decay",
    },
    status="success",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# --- Development Factors Summary ---
print("=" * 80)
print("  CHAIN-LADDER RESERVING — SUMMARY")
print("=" * 80)

print("\n  DEVELOPMENT FACTORS (Paid) — sample:")
print("-" * 80)
df_dev_factors.createOrReplaceTempView("v_dev_factors")
spark.sql("""
    SELECT lob_name, from_period, to_period,
           ROUND(link_ratio_paid, 6) AS link_ratio_paid,
           ROUND(link_ratio_incurred, 6) AS link_ratio_incurred,
           num_accident_years
    FROM v_dev_factors
    ORDER BY lob_code, from_period
""").show(50, truncate=False)

# --- Tail Factors ---
print("\n  TAIL FACTORS:")
print("-" * 80)
for lob in lob_codes:
    print(f"  {lob_info[lob]:<40} paid={tail_factors[lob]['tail_paid']:.6f}  "
          f"incurred={tail_factors[lob]['tail_incurred']:.6f}")

# --- Ultimate Claims Summary ---
print("\n  ULTIMATE CLAIMS BY LoB:")
print("-" * 80)
spark.sql("""
    SELECT lob_name,
           COUNT(*) AS num_ay,
           ROUND(SUM(latest_cumulative_paid), 0) AS total_paid_to_date,
           ROUND(SUM(ultimate_paid), 0) AS total_ultimate_paid,
           ROUND(SUM(ibnr), 0) AS total_ibnr,
           ROUND(SUM(case_reserves), 0) AS total_case_reserves
    FROM v_ultimate_claims
    GROUP BY lob_code, lob_name
    ORDER BY lob_code
""").show(20, truncate=False)

# --- Best Estimate Summary ---
print("\n  BEST ESTIMATE OF TECHNICAL PROVISIONS BY LoB:")
print("-" * 80)
df_best_estimate.createOrReplaceTempView("v_best_estimate")
spark.sql("""
    SELECT lob_name,
           ROUND(best_estimate_claims_prov, 0) AS claims_prov,
           ROUND(best_estimate_premium_prov, 0) AS premium_prov,
           ROUND(risk_margin, 0) AS risk_margin,
           ROUND(total_technical_provisions, 0) AS total_tp
    FROM v_best_estimate
    ORDER BY lob_code
""").show(20, truncate=False)

# --- Grand Totals ---
grand_total_ibnr = sum(r.ibnr for r in ultimate_rows)
grand_total_tp = sum(r.total_technical_provisions for r in best_estimate_rows)
print(f"  Grand Total IBNR:                  EUR {grand_total_ibnr:>15,.0f}")
print(f"  Grand Total Technical Provisions:   EUR {grand_total_tp:>15,.0f}")

print("\n  Output tables:")
print(f"    {target_dev_factors} ({dev_factors_count} rows)")
print(f"    {target_ultimate} ({ultimate_count} rows)")
print(f"    {target_best_estimate} ({best_estimate_count} rows)")
print("=" * 80)
print("  21_silver_chain_ladder complete.")
print("=" * 80)
