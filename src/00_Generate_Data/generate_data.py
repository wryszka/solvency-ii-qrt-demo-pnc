# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Insurance Data
# MAGIC
# MAGIC Generates realistic P&C insurance data for **Bricksurance SE**, a mid-size European insurer.
# MAGIC
# MAGIC **Run once per quarter** — each run appends one quarter's data. Run for Q1–Q4 to build
# MAGIC a full year of history for QRT comparison.
# MAGIC
# MAGIC ## Tables produced
# MAGIC
# MAGIC | Table | Description | Feeds |
# MAGIC |---|---|---|
# MAGIC | `counterparties` | Master counterparty register (~500) | All QRTs |
# MAGIC | `assets` | Investment portfolio (~5,000) | S.06.02 |
# MAGIC | `policies` | Policy register (~20,000) | S.05.01 |
# MAGIC | `premiums` | Premium transactions (~20K/quarter) | S.05.01 |
# MAGIC | `claims` | Claims transactions (~15K/quarter) | S.05.01, S.19.01 |
# MAGIC | `expenses` | Expense allocations by LoB (~7/quarter) | S.05.01 |
# MAGIC | `reinsurance` | Reinsurance programme (~50) | All QRTs |
# MAGIC | `claims_triangles` | Development triangles (10yr x 8 LoB) | S.19.01, S.26.06 |
# MAGIC | `risk_factors` | SCR sub-module charges (~30) | S.25.01 |
# MAGIC | `scr_parameters` | EIOPA correlation matrix + factors | S.25.01 |
# MAGIC | `volume_measures` | Premium & reserve volumes by LoB | S.26.06 |
# MAGIC | `exposures` | Exposure sets by peril & LoB (~500) | Igloo input |
# MAGIC | `igloo_results` | Simulated stochastic output — VaR/TVaR | S.25.01 (IM) |
# MAGIC | `own_funds` | Own funds components (~10) | Solvency ratio |
# MAGIC | `balance_sheet` | SII balance sheet items (~20) | Overview |
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog_name` — Unity Catalog
# MAGIC - `schema_name` — Schema (default: `solvency2demo`)
# MAGIC - `reporting_period` — e.g. `2025-Q1`, `2025-Q2`, etc.
# MAGIC - `mode` — `append` (add quarter) or `full_reset` (drop everything, regenerate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main")
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_period", "2025-Q4")
dbutils.widgets.text("mode", "append")  # append | full_reset
dbutils.widgets.text("entity_name", "Bricksurance SE")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_period = dbutils.widgets.get("reporting_period")
mode = dbutils.widgets.get("mode")
entity_name = dbutils.widgets.get("entity_name")

# Parse reporting period
rp_year = int(reporting_period.split("-")[0])
rp_quarter = int(reporting_period.split("-Q")[1])
reporting_date = f"{rp_year}-{rp_quarter * 3:02d}-{[31,30,30,31][rp_quarter-1]:02d}"

# Deterministic seed: varies per quarter so data differs but is reproducible
base_seed = 42
quarter_seed = base_seed + rp_year * 10 + rp_quarter

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting period: {reporting_period}")
print(f"Reporting date:   {reporting_date}")
print(f"Mode:             {mode}")
print(f"Entity:           {entity_name}")
print(f"Seed:             {quarter_seed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import hashlib

rng = np.random.RandomState(quarter_seed)
rpt_date = datetime.strptime(reporting_date, "%Y-%m-%d").date()

spark.sql(f"USE CATALOG {catalog}")

if mode == "full_reset":
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print(f"Schema {schema} dropped (full_reset mode)")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

# Create volume for regulatory exports (used later by the app)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.regulatory_exports")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.igloo_exchange")

print(f"Schema {catalog}.{schema} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data & Helpers

# COMMAND ----------

# ── Lines of business (Solvency II Annex I) ─────────────────────────
LOB_CONFIG = [
    {"code": 1,  "name": "Medical expense insurance",         "gwp_share": 0.08},
    {"code": 2,  "name": "Income protection insurance",       "gwp_share": 0.06},
    {"code": 4,  "name": "Motor vehicle liability insurance", "gwp_share": 0.25},
    {"code": 5,  "name": "Other motor insurance",             "gwp_share": 0.15},
    {"code": 7,  "name": "Fire and other property insurance", "gwp_share": 0.25},
    {"code": 8,  "name": "General liability insurance",       "gwp_share": 0.13},
    {"code": 12, "name": "Miscellaneous financial loss",      "gwp_share": 0.08},
]
LOB_CODES = [l["code"] for l in LOB_CONFIG]
LOB_NAMES = {l["code"]: l["name"] for l in LOB_CONFIG}
GWP_SHARES = {l["code"]: l["gwp_share"] for l in LOB_CONFIG}

# ── EUR targets (annual, millions) ──────────────────────────────────
TOTAL_ASSETS_M = 6500.0
TOTAL_GWP_M = 2000.0
TARGET_COMBINED_RATIO = 0.96
TARGET_SCR_M = 1150.0
TARGET_OWN_FUNDS_M = 2000.0

# Quarter-over-quarter growth & seasonal factors
QUARTERLY_GROWTH = 0.008   # ~3.2% annual growth
SEASONAL_FACTORS = {1: 0.95, 2: 0.98, 3: 1.02, 4: 1.05}  # Q4 heaviest

# ── Countries ────────────────────────────────────────────────────────
SOVEREIGN_COUNTRIES = ["DE", "FR", "NL", "IT", "ES", "BE", "AT"]
SOVEREIGN_NAMES = {
    "DE": "Federal Republic of Germany", "FR": "Republic of France",
    "NL": "Kingdom of the Netherlands", "IT": "Republic of Italy",
    "ES": "Kingdom of Spain", "BE": "Kingdom of Belgium",
    "AT": "Republic of Austria",
}
SOVEREIGN_WEIGHTS = [0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.07]

CORPORATE_SECTORS = {
    "K64": "Financial services", "K65": "Insurance", "C20": "Chemicals",
    "D35": "Energy", "H49": "Transport", "J61": "Telecoms",
    "C29": "Automotive", "F41": "Construction", "G47": "Retail",
}

CUSTODIANS = [
    "Euroclear Bank SA/NV", "Clearstream Banking AG",
    "BNP Paribas Securities Services", "Deutsche Bank AG – Custody",
    "State Street Bank GmbH",
]

SP_RATINGS = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
              "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-",
              "B+", "B", "B-", "CCC+", "CCC", "NR"]
RATING_TO_CQS = {
    "AAA": 0, "AA+": 0, "AA": 0, "AA-": 0,
    "A+": 1, "A": 1, "A-": 1,
    "BBB+": 2, "BBB": 2, "BBB-": 2,
    "BB+": 3, "BB": 3, "BB-": 3,
    "B+": 4, "B": 4, "B-": 4,
    "CCC+": 5, "CCC": 5, "NR": 6,
}

REINSURER_NAMES = [
    "Munich Re AG", "Swiss Re Ltd", "Hannover Rueck SE",
    "SCOR SE", "General Reinsurance AG", "PartnerRe Ltd",
    "Everest Re Group", "TransRe", "RenaissanceRe Holdings",
]

LOB_CESSION = {1: 0.15, 2: 0.15, 4: 0.25, 5: 0.20, 7: 0.30, 8: 0.25, 12: 0.20}

CLAIM_CAUSES = {
    1:  ["illness", "hospitalisation", "outpatient", "chronic"],
    2:  ["disability", "long_term_illness", "accident", "mental_health"],
    4:  ["collision", "pedestrian", "multi_vehicle", "single_vehicle"],
    5:  ["theft", "vandalism", "hail", "windscreen", "fire"],
    7:  ["fire", "water_damage", "storm", "burglary", "subsidence"],
    8:  ["product_liability", "professional_indemnity", "public_liability"],
    12: ["fraud", "business_interruption", "cyber", "credit_default"],
}

LOB_SEVERITY_MU = {1: 8.5, 2: 8.8, 4: 9.2, 5: 8.0, 7: 9.0, 8: 9.5, 12: 9.0}
LOB_SEVERITY_SIGMA = {1: 1.2, 2: 1.3, 4: 1.4, 5: 1.1, 7: 1.5, 8: 1.6, 12: 1.4}

# ── Corp name pools ──────────────────────────────────────────────────
_corp_first = ["Alpha", "Beta", "Gamma", "Delta", "Euro", "Nord", "Atlas",
               "Hansa", "Rhein", "Baltic", "Iberian", "Nordic", "Helvetia",
               "Continental", "Maritime", "Alpen", "Titan", "Orion", "Polaris",
               "Apex", "Nexus", "Vertex", "Zenith", "Prima", "Optima", "Nova"]
_corp_suffix = ["AG", "SE", "GmbH", "NV", "SA", "SAS", "BV", "SpA", "Ltd", "Plc"]
_corp_mid = ["Capital", "Finance", "Holdings", "Industries", "Group", "Invest",
             "Partners", "Solutions", "Services", "Technologies", "Energy",
             "Logistics", "Trading", "Insurance", "Securities", "Asset Management"]

# ── Helpers ──────────────────────────────────────────────────────────

def make_lei(seed_str):
    h = hashlib.sha256(f"{base_seed}_{seed_str}".encode()).hexdigest().upper()
    return h[:20]

def make_isin(country, idx):
    h = hashlib.md5(f"{base_seed}_{country}_{idx}".encode()).hexdigest().upper()
    return f"{country}{h[:9]}0"

def random_date(start, end, n=1):
    delta = (end - start).days
    if delta <= 0:
        return [start] * n
    days = rng.randint(0, delta, size=n)
    return [start + timedelta(int(d)) for d in days]

def to_eur(x):
    return round(float(x), 2)

def gen_market_values(n, total_target, sigma=0.8):
    raw = rng.lognormal(mean=0.0, sigma=sigma, size=n)
    return raw / raw.sum() * total_target

def table_exists(table_name):
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table_name}")

def write_table(df_pandas, table_name, description, mode="overwrite"):
    """Write a pandas DataFrame to Delta. mode='overwrite' or 'append'."""
    full_name = f"{catalog}.{schema}.{table_name}"
    sdf = spark.createDataFrame(df_pandas)
    sdf.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full_name)
    cnt = spark.table(full_name).count()
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{description}'")
    print(f"  {table_name}: {cnt} rows")
    return cnt

def write_quarterly_table(df_pandas, table_name, description):
    """Write per-quarter data: deletes existing quarter rows then appends."""
    full_name = f"{catalog}.{schema}.{table_name}"
    if table_exists(table_name):
        spark.sql(f"DELETE FROM {full_name} WHERE reporting_period = '{reporting_period}'")
        write_table(df_pandas, table_name, description, mode="append")
    else:
        write_table(df_pandas, table_name, description, mode="overwrite")

# Seasonal + growth multiplier for this quarter
seasonal = SEASONAL_FACTORS[rp_quarter]
quarters_from_base = (rp_year - 2025) * 4 + rp_quarter
growth = (1 + QUARTERLY_GROWTH) ** quarters_from_base

print(f"Seasonal factor: {seasonal}, Growth factor: {growth:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Counterparties
# MAGIC
# MAGIC Master register — written once, not per-quarter.

# COMMAND ----------

# Only generate if table doesn't exist or full_reset
_cp_exists = spark.catalog.tableExists(f"{catalog}.{schema}.counterparties")

if not _cp_exists or mode == "full_reset":
    countries_pool = SOVEREIGN_COUNTRIES + ["LU", "IE", "FI", "SE", "DK", "PT", "CH"]
    nace_codes = list(CORPORATE_SECTORS.keys())
    cp_types = ["issuer", "issuer", "issuer", "issuer", "reinsurer", "bank"]
    rating_weights = np.array(
        [0.05] + [0.07]*3 + [0.10]*3 + [0.09]*3 + [0.04]*3 + [0.02]*3 + [0.01]*2 + [0.01]
    )
    rating_weights /= rating_weights.sum()

    counterparties = []
    for i in range(500):
        first = _corp_first[rng.randint(len(_corp_first))]
        mid = _corp_mid[rng.randint(len(_corp_mid))]
        suffix = _corp_suffix[rng.randint(len(_corp_suffix))]
        country = countries_pool[rng.randint(len(countries_pool))]
        rating = rng.choice(SP_RATINGS, p=rating_weights)

        counterparties.append({
            "counterparty_id": f"CP{i+1:05d}",
            "counterparty_name": f"{first} {mid} {suffix}",
            "lei": make_lei(f"cp_{i}"),
            "country": country,
            "sector_nace": nace_codes[rng.randint(len(nace_codes))],
            "credit_rating": rating,
            "credit_quality_step": int(RATING_TO_CQS[rating]),
            "counterparty_type": cp_types[rng.randint(len(cp_types))],
            "is_regulated": bool(rng.random() < 0.7),
        })

    write_table(pd.DataFrame(counterparties), "counterparties",
                "Master counterparty register — issuers, reinsurers, banks")
else:
    print("  counterparties: already exists, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Assets (~5,000)
# MAGIC
# MAGIC Investment portfolio snapshot at quarter-end. Overwrites each quarter —
# MAGIC asset valuations change quarter to quarter.

# COMMAND ----------

N_ASSETS = 5000
TOTAL_MV = TOTAL_ASSETS_M * 1e6 * (1 + rng.uniform(-0.03, 0.03))  # slight variation

alloc = {"government_bonds": 0.60, "corporate_bonds": 0.20, "equity": 0.10, "ciu": 0.05, "property": 0.05}
n_gov = int(N_ASSETS * 0.60)
n_corp = int(N_ASSETS * 0.20)
n_eq = int(N_ASSETS * 0.10)
n_ciu = int(N_ASSETS * 0.05)
n_oth = N_ASSETS - n_gov - n_corp - n_eq - n_ciu

# Load counterparties for issuer linkage
df_cp = spark.table(f"{catalog}.{schema}.counterparties").toPandas()
issuers = df_cp[df_cp["counterparty_type"] == "issuer"]

assets_rows = []
idx = 0

# -- Government bonds --
gov_mv = gen_market_values(n_gov, TOTAL_MV * alloc["government_bonds"], 0.9)
for i in range(n_gov):
    country = rng.choice(SOVEREIGN_COUNTRIES, p=SOVEREIGN_WEIGHTS)
    cic = f"{country}11"
    acq_date = random_date(date(2015,1,1), date(2025,6,30))[0]
    mat_years = rng.uniform(3, 15)
    mat_date = acq_date + timedelta(days=int(mat_years * 365.25))
    coupon = round(rng.uniform(0.005, 0.035), 4)
    par = to_eur(gov_mv[i] * rng.uniform(0.92, 1.08))
    mod_dur = round(rng.uniform(2.5, 12.0), 2)
    rating = rng.choice(["AAA","AA+","AA","AA-","A+","A"], p=[0.25,0.20,0.20,0.15,0.10,0.10])

    assets_rows.append({
        "asset_id": f"A{idx+1:06d}",
        "asset_name": f"{SOVEREIGN_NAMES[country]} {coupon*100:.2f}% {mat_date.year}",
        "issuer_name": SOVEREIGN_NAMES[country],
        "issuer_lei": make_lei(f"sov_{country}"),
        "issuer_country": country,
        "issuer_sector": "O84",
        "cic_code": cic,
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": mat_date,
        "par_value": par,
        "acquisition_cost": to_eur(gov_mv[i] * rng.uniform(0.95, 1.02)),
        "market_value_eur": to_eur(gov_mv[i]),
        "sii_value": to_eur(gov_mv[i] * rng.uniform(0.98, 1.02)),
        "accrued_interest": to_eur(par * coupon * rng.uniform(0.0, 0.5)),
        "coupon_rate": coupon,
        "credit_rating": rating,
        "credit_quality_step": int(RATING_TO_CQS[rating]),
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": mod_dur,
        "asset_class": "government_bonds",
        "reporting_period": reporting_period,
    })
    idx += 1

# -- Corporate bonds --
corp_mv = gen_market_values(n_corp, TOTAL_MV * alloc["corporate_bonds"], 0.85)
for i in range(n_corp):
    cp = issuers.iloc[rng.randint(len(issuers))]
    cic_suffix = rng.choice(["21", "22"], p=[0.85, 0.15])
    acq_date = random_date(date(2016,1,1), date(2025,6,30))[0]
    mat_years = rng.uniform(2, 10)
    mat_date = acq_date + timedelta(days=int(mat_years * 365.25))
    coupon = round(rng.uniform(0.015, 0.06), 4)
    par = to_eur(corp_mv[i] * rng.uniform(0.90, 1.10))
    mod_dur = round(rng.uniform(1.5, 8.0), 2)
    rating_w = np.array([0.02,0.05,0.08,0.10,0.12,0.15,0.12,0.10,0.08,0.06,0.04,0.03,0.02,0.01,0.01,0.005,0.002,0.002,0.001])
    rating_w /= rating_w.sum()
    rating = rng.choice(SP_RATINGS, p=rating_w)

    assets_rows.append({
        "asset_id": f"A{idx+1:06d}",
        "asset_name": f"{cp['counterparty_name']} {coupon*100:.2f}% {mat_date.year}",
        "issuer_name": cp["counterparty_name"],
        "issuer_lei": cp["lei"],
        "issuer_country": cp["country"],
        "issuer_sector": cp["sector_nace"],
        "cic_code": f"XL{cic_suffix}",
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": mat_date,
        "par_value": par,
        "acquisition_cost": to_eur(corp_mv[i] * rng.uniform(0.93, 1.05)),
        "market_value_eur": to_eur(corp_mv[i]),
        "sii_value": to_eur(corp_mv[i] * rng.uniform(0.97, 1.03)),
        "accrued_interest": to_eur(par * coupon * rng.uniform(0.0, 0.5)),
        "coupon_rate": coupon,
        "credit_rating": rating,
        "credit_quality_step": int(RATING_TO_CQS[rating]),
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "is_listed": True,
        "infrastructure_flag": bool(rng.random() < 0.05),
        "modified_duration": mod_dur,
        "asset_class": "corporate_bonds",
        "reporting_period": reporting_period,
    })
    idx += 1

# -- Equity --
_equity_names = [
    "Allianz SE", "AXA SA", "Zurich Insurance", "Generali SpA",
    "SAP SE", "Siemens AG", "ASML Holding NV", "TotalEnergies SE",
    "LVMH SE", "Unilever NV", "Nestlé SA", "Roche Holding AG",
    "Novartis AG", "Sanofi SA", "BNP Paribas SA", "Deutsche Bank AG",
    "ING Group NV", "Banco Santander SA", "Iberdrola SA", "Enel SpA",
]
eq_mv = gen_market_values(n_eq, TOTAL_MV * alloc["equity"], 0.7)
for i in range(n_eq):
    eq_name = _equity_names[i % len(_equity_names)]
    acq_date = random_date(date(2015,1,1), date(2025,6,30))[0]
    assets_rows.append({
        "asset_id": f"A{idx+1:06d}",
        "asset_name": eq_name,
        "issuer_name": eq_name,
        "issuer_lei": make_lei(f"eq_{i}"),
        "issuer_country": rng.choice(SOVEREIGN_COUNTRIES),
        "issuer_sector": rng.choice(["K64","C29","J61","D35","G47"]),
        "cic_code": "XL31",
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": None,
        "par_value": None,
        "acquisition_cost": to_eur(eq_mv[i] * rng.uniform(0.60, 1.10)),
        "market_value_eur": to_eur(eq_mv[i]),
        "sii_value": to_eur(eq_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": None,
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": "equity",
        "reporting_period": reporting_period,
    })
    idx += 1

# -- CIUs --
ciu_mv = gen_market_values(n_ciu, TOTAL_MV * alloc["ciu"], 0.65)
_ciu_types = [("41","Equity Fund"),("42","Debt Fund"),("43","Money Market Fund"),
              ("44","Asset Allocation Fund"),("45","Real Estate Fund")]
for i in range(n_ciu):
    suffix, fund_type = _ciu_types[i % len(_ciu_types)]
    fund_name = f"{_corp_first[rng.randint(len(_corp_first))]} {fund_type}"
    assets_rows.append({
        "asset_id": f"A{idx+1:06d}",
        "asset_name": fund_name,
        "issuer_name": fund_name,
        "issuer_lei": make_lei(f"ciu_{i}"),
        "issuer_country": rng.choice(["LU","IE","DE","FR","NL"]),
        "issuer_sector": "K64",
        "cic_code": f"XL{suffix}",
        "currency": "EUR",
        "acquisition_date": random_date(date(2017,1,1), date(2025,6,30))[0],
        "maturity_date": None,
        "par_value": None,
        "acquisition_cost": to_eur(ciu_mv[i] * rng.uniform(0.85, 1.05)),
        "market_value_eur": to_eur(ciu_mv[i]),
        "sii_value": to_eur(ciu_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": None,
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": "ciu",
        "reporting_period": reporting_period,
    })
    idx += 1

# -- Property / Other --
oth_mv = gen_market_values(n_oth, TOTAL_MV * alloc["property"], 0.6)
for i in range(n_oth):
    assets_rows.append({
        "asset_id": f"A{idx+1:06d}",
        "asset_name": f"Property Investment {i+1}",
        "issuer_name": f"Bricksurance Real Estate {i+1}",
        "issuer_lei": make_lei(f"prop_{i}"),
        "issuer_country": rng.choice(SOVEREIGN_COUNTRIES),
        "issuer_sector": "L68",
        "cic_code": "XL91",
        "currency": "EUR",
        "acquisition_date": random_date(date(2015,1,1), date(2024,6,30))[0],
        "maturity_date": None,
        "par_value": None,
        "acquisition_cost": to_eur(oth_mv[i] * rng.uniform(0.70, 1.00)),
        "market_value_eur": to_eur(oth_mv[i]),
        "sii_value": to_eur(oth_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": None,
        "portfolio_type": "Non-life",
        "custodian_name": "Direct holding",
        "is_listed": False,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": "property",
        "reporting_period": reporting_period,
    })
    idx += 1

df_assets = pd.DataFrame(assets_rows)
write_quarterly_table(df_assets, "assets", "Investment portfolio — quarter-end snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Policies (~20,000)
# MAGIC
# MAGIC Written once (master register). Policies span multiple quarters.

# COMMAND ----------

_pol_exists = spark.catalog.tableExists(f"{catalog}.{schema}.policies")

if not _pol_exists or mode == "full_reset":
    N_POLICIES = 20000
    policies = []
    for i in range(N_POLICIES):
        lob = LOB_CONFIG[rng.randint(len(LOB_CONFIG))]
        inception = random_date(date(2023,1,1), date(2025,9,30))[0]
        expiry = inception + timedelta(days=365)
        gwp = to_eur(rng.lognormal(mean=9.5, sigma=1.2) * GWP_SHARES[lob["code"]])

        policies.append({
            "policy_id": f"POL{i+1:06d}",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "inception_date": inception,
            "expiry_date": expiry,
            "gross_written_premium": gwp,
            "currency": "EUR",
            "country": rng.choice(SOVEREIGN_COUNTRIES),
            "status": rng.choice(["active","active","active","lapsed","cancelled"], p=[0.70,0.15,0.05,0.05,0.05]),
        })

    write_table(pd.DataFrame(policies), "policies", "Policy register — all active and historical policies")
else:
    print("  policies: already exists, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Premiums (per quarter, ~20K transactions)
# MAGIC
# MAGIC Appended each quarter. Represents earned/written premiums by LoB.

# COMMAND ----------

quarterly_gwp = TOTAL_GWP_M * 1e6 / 4 * seasonal * growth
premiums = []

for lob in LOB_CONFIG:
    lob_gwp = quarterly_gwp * lob["gwp_share"]
    n_txn = int(2500 * lob["gwp_share"] / 0.08)  # roughly proportional
    txn_amounts = gen_market_values(n_txn, lob_gwp, sigma=0.6)

    cession_rate = LOB_CESSION[lob["code"]]

    for j in range(n_txn):
        gross = to_eur(txn_amounts[j])
        ri_share = to_eur(gross * cession_rate * rng.uniform(0.8, 1.2))
        net = to_eur(gross - ri_share)

        premiums.append({
            "transaction_id": f"PR-{reporting_period}-{lob['code']}-{j+1:05d}",
            "policy_id": f"POL{rng.randint(1, 20001):06d}",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "reporting_period": reporting_period,
            "gross_written_premium": gross,
            "gross_earned_premium": to_eur(gross * rng.uniform(0.90, 1.00)),
            "reinsurers_share_written": ri_share,
            "reinsurers_share_earned": to_eur(ri_share * rng.uniform(0.90, 1.00)),
            "net_written_premium": net,
            "net_earned_premium": to_eur(net * rng.uniform(0.90, 1.00)),
            "currency": "EUR",
        })

write_quarterly_table(pd.DataFrame(premiums), "premiums",
            "Premium transactions by LoB — one quarter per run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Claims (per quarter, ~15K transactions)
# MAGIC
# MAGIC Appended each quarter. Detailed claim events.

# COMMAND ----------

claims = []
for lob in LOB_CONFIG:
    n_claims = int(15000 * lob["gwp_share"]) + rng.randint(-50, 50)
    for j in range(n_claims):
        severity = to_eur(rng.lognormal(LOB_SEVERITY_MU[lob["code"]], LOB_SEVERITY_SIGMA[lob["code"]]))
        paid_pct = rng.uniform(0.3, 1.0)
        gross_paid = to_eur(severity * paid_pct)
        gross_incurred = to_eur(severity)
        cession = LOB_CESSION[lob["code"]]
        ri_paid = to_eur(gross_paid * cession * rng.uniform(0.8, 1.2))
        ri_incurred = to_eur(gross_incurred * cession * rng.uniform(0.8, 1.2))

        loss_date = random_date(
            date(rp_year, (rp_quarter-1)*3+1, 1),
            rpt_date
        )[0]

        claims.append({
            "claim_id": f"CLM-{reporting_period}-{lob['code']}-{j+1:06d}",
            "policy_id": f"POL{rng.randint(1, 20001):06d}",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "reporting_period": reporting_period,
            "loss_date": loss_date,
            "notification_date": loss_date + timedelta(days=int(rng.exponential(15))),
            "cause": rng.choice(CLAIM_CAUSES[lob["code"]]),
            "gross_paid": gross_paid,
            "gross_incurred": gross_incurred,
            "gross_reserved": to_eur(gross_incurred - gross_paid),
            "reinsurers_share_paid": ri_paid,
            "reinsurers_share_incurred": ri_incurred,
            "net_paid": to_eur(gross_paid - ri_paid),
            "net_incurred": to_eur(gross_incurred - ri_incurred),
            "status": rng.choice(["open","open","settled","reopened"], p=[0.4,0.3,0.25,0.05]),
            "currency": "EUR",
        })

write_quarterly_table(pd.DataFrame(claims), "claims",
            "Claims transactions — loss events with paid/incurred/reserved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Expenses (per quarter, by LoB)

# COMMAND ----------

expenses = []
for lob in LOB_CONFIG:
    lob_gwp_q = TOTAL_GWP_M * 1e6 / 4 * lob["gwp_share"] * seasonal * growth
    acquisition = to_eur(lob_gwp_q * rng.uniform(0.12, 0.18))
    administrative = to_eur(lob_gwp_q * rng.uniform(0.05, 0.09))
    claims_mgmt = to_eur(lob_gwp_q * rng.uniform(0.03, 0.06))
    overhead = to_eur(lob_gwp_q * rng.uniform(0.02, 0.04))
    investment_mgmt = to_eur(lob_gwp_q * rng.uniform(0.005, 0.015))
    other = to_eur(lob_gwp_q * rng.uniform(0.005, 0.01))

    expenses.append({
        "lob_code": lob["code"],
        "lob_name": lob["name"],
        "reporting_period": reporting_period,
        "acquisition_expenses": acquisition,
        "administrative_expenses": administrative,
        "claims_management_expenses": claims_mgmt,
        "overhead_expenses": overhead,
        "investment_management_expenses": investment_mgmt,
        "other_expenses": other,
        "total_expenses": to_eur(acquisition + administrative + claims_mgmt + overhead + investment_mgmt + other),
        "currency": "EUR",
    })

write_quarterly_table(pd.DataFrame(expenses), "expenses",
            "Expense allocation by LoB — quarterly breakdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reinsurance Programme
# MAGIC
# MAGIC Written once — treaty structure doesn't change per quarter.

# COMMAND ----------

_ri_exists = spark.catalog.tableExists(f"{catalog}.{schema}.reinsurance")

if not _ri_exists or mode == "full_reset":
    ri_rows = []
    treaty_idx = 0
    for lob in LOB_CONFIG:
        # Quota share
        ri_rows.append({
            "treaty_id": f"RI{treaty_idx+1:04d}",
            "treaty_name": f"QS {lob['name'][:20]}",
            "treaty_type": "quota_share",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "reinsurer": rng.choice(REINSURER_NAMES),
            "cession_rate": round(LOB_CESSION[lob["code"]], 3),
            "retention": round(1 - LOB_CESSION[lob["code"]], 3),
            "limit_eur": None,
            "deductible_eur": None,
            "inception_date": date(rp_year, 1, 1),
            "expiry_date": date(rp_year, 12, 31),
            "currency": "EUR",
        })
        treaty_idx += 1

        # Excess of loss
        lob_gwp = TOTAL_GWP_M * 1e6 * lob["gwp_share"]
        ri_rows.append({
            "treaty_id": f"RI{treaty_idx+1:04d}",
            "treaty_name": f"XL {lob['name'][:20]}",
            "treaty_type": "excess_of_loss",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "reinsurer": rng.choice(REINSURER_NAMES),
            "cession_rate": None,
            "retention": None,
            "limit_eur": to_eur(lob_gwp * rng.uniform(0.15, 0.30)),
            "deductible_eur": to_eur(lob_gwp * rng.uniform(0.01, 0.05)),
            "inception_date": date(rp_year, 1, 1),
            "expiry_date": date(rp_year, 12, 31),
            "currency": "EUR",
        })
        treaty_idx += 1

    write_table(pd.DataFrame(ri_rows), "reinsurance",
                "Reinsurance programme — QS and XL treaties by LoB")
else:
    print("  reinsurance: already exists, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Claims Triangles (10 accident years x 8 LoBs)
# MAGIC
# MAGIC Development triangles for reserving. Regenerated per quarter as new development is observed.

# COMMAND ----------

# Triangle config — same LoBs, different tail patterns
TRIANGLE_LOB = {
    1: {"name": "Medical expense",  "ultimate_base": 50_000_000,  "tail": "short"},
    2: {"name": "Income protection","ultimate_base": 35_000_000,  "tail": "medium"},
    4: {"name": "Motor liability",  "ultimate_base": 100_000_000, "tail": "long"},
    5: {"name": "Other motor",      "ultimate_base": 45_000_000,  "tail": "short"},
    7: {"name": "Property",         "ultimate_base": 55_000_000,  "tail": "medium"},
    8: {"name": "General liability", "ultimate_base": 70_000_000,  "tail": "long"},
    12:{"name": "Misc financial",   "ultimate_base": 20_000_000,  "tail": "medium"},
}

DEV_PATTERNS = {
    "long":   [0.15, 0.35, 0.52, 0.65, 0.75, 0.83, 0.89, 0.93, 0.96, 0.98],
    "medium": [0.30, 0.55, 0.72, 0.83, 0.90, 0.94, 0.97, 0.985, 0.995, 1.00],
    "short":  [0.50, 0.78, 0.90, 0.95, 0.975, 0.99, 0.995, 0.998, 1.00, 1.00],
}
IBNR_FACTORS = {
    "long":   [1.60, 1.45, 1.30, 1.20, 1.12, 1.08, 1.05, 1.03, 1.01, 1.00],
    "medium": [1.40, 1.28, 1.18, 1.10, 1.06, 1.03, 1.02, 1.01, 1.005, 1.00],
    "short":  [1.25, 1.12, 1.06, 1.03, 1.015, 1.005, 1.002, 1.001, 1.00, 1.00],
}

accident_years = list(range(rp_year - 10 + 1, rp_year))
tri_rows = []

for lob_code, cfg in TRIANGLE_LOB.items():
    tail = cfg["tail"]
    pattern = DEV_PATTERNS[tail]
    ibnr_pat = IBNR_FACTORS[tail]

    for ay in accident_years:
        years_from_start = ay - accident_years[0]
        growth_f = (1.03) ** years_from_start
        noise = 1.0 + rng.uniform(-0.15, 0.15)
        ultimate = cfg["ultimate_base"] * growth_f * noise
        max_dev = min(10, rp_year - ay)

        cum_paid = 0.0
        cum_inc = 0.0

        for dev in range(1, max_dev + 1):
            target_cum_paid = ultimate * pattern[dev-1] * (1 + rng.uniform(-0.03, 0.03))
            if target_cum_paid < cum_paid:
                target_cum_paid = cum_paid + abs(rng.normal(0, ultimate * 0.005))
            inc_paid = round(target_cum_paid - cum_paid, 2)
            cum_paid = round(cum_paid + inc_paid, 2)

            ibnr_mult = max(1.0, ibnr_pat[dev-1] * (1 + rng.uniform(-0.02, 0.02)))
            target_cum_inc = cum_paid * ibnr_mult
            if target_cum_inc < cum_inc:
                target_cum_inc = cum_inc + abs(rng.normal(0, ultimate * 0.002))
            if target_cum_inc < cum_paid:
                target_cum_inc = cum_paid
            inc_inc = round(target_cum_inc - cum_inc, 2)
            cum_inc = round(cum_inc + inc_inc, 2)

            tri_rows.append({
                "accident_year": int(ay),
                "development_period": int(dev),
                "lob_code": int(lob_code),
                "lob_name": cfg["name"],
                "incremental_paid": round(inc_paid, 2),
                "incremental_incurred": round(inc_inc, 2),
                "cumulative_paid": round(cum_paid, 2),
                "cumulative_incurred": round(cum_inc, 2),
                "reporting_period": reporting_period,
            })

write_quarterly_table(pd.DataFrame(tri_rows), "claims_triangles",
            "Claims development triangles — paid & incurred by AY, dev period, LoB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. SCR Parameters & Risk Factors
# MAGIC
# MAGIC EIOPA Standard Formula parameters — correlation matrix and sub-module charges.
# MAGIC Written once (these are regulatory constants + calibrated inputs).

# COMMAND ----------

_scr_exists = spark.catalog.tableExists(f"{catalog}.{schema}.scr_parameters")

if not _scr_exists or mode == "full_reset":
    # EIOPA correlation matrix for BSCR aggregation
    modules = ["market", "default", "life", "health", "non_life"]
    corr_matrix = [
        [1.00, 0.25, 0.25, 0.25, 0.25],
        [0.25, 1.00, 0.25, 0.25, 0.50],
        [0.25, 0.25, 1.00, 0.25, 0.00],
        [0.25, 0.25, 0.25, 1.00, 0.00],
        [0.25, 0.50, 0.00, 0.00, 1.00],
    ]

    scr_params = []
    for i, mod_i in enumerate(modules):
        for j, mod_j in enumerate(modules):
            scr_params.append({
                "parameter_type": "bscr_correlation",
                "module_i": mod_i,
                "module_j": mod_j,
                "value": corr_matrix[i][j],
                "description": f"BSCR correlation: {mod_i} vs {mod_j}",
            })

    # Market risk sub-module correlations
    market_subs = ["interest_rate", "equity", "property", "spread", "currency", "concentration"]
    mkt_corr = [
        [1.00, 0.00, 0.00, 0.00, 0.25, 0.00],
        [0.00, 1.00, 0.75, 0.75, 0.25, 0.00],
        [0.00, 0.75, 1.00, 0.50, 0.25, 0.00],
        [0.00, 0.75, 0.50, 1.00, 0.25, 0.00],
        [0.25, 0.25, 0.25, 0.25, 1.00, 0.00],
        [0.00, 0.00, 0.00, 0.00, 0.00, 1.00],
    ]
    for i, sub_i in enumerate(market_subs):
        for j, sub_j in enumerate(market_subs):
            scr_params.append({
                "parameter_type": "market_correlation",
                "module_i": sub_i,
                "module_j": sub_j,
                "value": mkt_corr[i][j],
                "description": f"Market risk correlation: {sub_i} vs {sub_j}",
            })

    # Op risk factor
    scr_params.append({
        "parameter_type": "op_risk_factor",
        "module_i": "operational",
        "module_j": "operational",
        "value": 0.03,
        "description": "Operational risk as % of earned premiums",
    })

    write_table(pd.DataFrame(scr_params), "scr_parameters",
                "EIOPA Standard Formula parameters — correlation matrices and calibration factors")
else:
    print("  scr_parameters: already exists, skipping")

# COMMAND ----------

# Risk factors — SCR sub-module charges. These vary per quarter (market conditions).
risk_factors = []

# Market risk sub-modules
mkt_charges = {
    "interest_rate_up": to_eur(rng.uniform(180, 220) * 1e6 * growth),
    "interest_rate_down": to_eur(rng.uniform(150, 190) * 1e6 * growth),
    "equity_type1": to_eur(rng.uniform(200, 280) * 1e6 * growth),
    "equity_type2": to_eur(rng.uniform(30, 50) * 1e6 * growth),
    "property": to_eur(rng.uniform(60, 90) * 1e6 * growth),
    "spread_bonds": to_eur(rng.uniform(120, 160) * 1e6 * growth),
    "spread_structured": to_eur(rng.uniform(10, 25) * 1e6 * growth),
    "currency": to_eur(rng.uniform(80, 120) * 1e6 * growth),
    "concentration": to_eur(rng.uniform(20, 40) * 1e6 * growth),
}
for name, charge in mkt_charges.items():
    risk_factors.append({
        "risk_module": "market",
        "risk_sub_module": name,
        "charge_eur": charge,
        "reporting_period": reporting_period,
        "description": f"Market risk: {name.replace('_', ' ')}",
    })

# Default risk
risk_factors.append({
    "risk_module": "default",
    "risk_sub_module": "type1_financial",
    "charge_eur": to_eur(rng.uniform(60, 100) * 1e6 * growth),
    "reporting_period": reporting_period,
    "description": "Counterparty default: financial institutions",
})
risk_factors.append({
    "risk_module": "default",
    "risk_sub_module": "type2_receivables",
    "charge_eur": to_eur(rng.uniform(15, 30) * 1e6 * growth),
    "reporting_period": reporting_period,
    "description": "Counterparty default: receivables",
})

# Non-life underwriting risk
nl_charges = {
    "premium_reserve": to_eur(rng.uniform(250, 350) * 1e6 * growth),
    "lapse": to_eur(rng.uniform(20, 40) * 1e6 * growth),
    "catastrophe": to_eur(rng.uniform(100, 160) * 1e6 * growth),
}
for name, charge in nl_charges.items():
    risk_factors.append({
        "risk_module": "non_life",
        "risk_sub_module": name,
        "charge_eur": charge,
        "reporting_period": reporting_period,
        "description": f"Non-life UW risk: {name.replace('_', ' ')}",
    })

# Health underwriting
risk_factors.append({
    "risk_module": "health",
    "risk_sub_module": "health_similar_nl",
    "charge_eur": to_eur(rng.uniform(40, 70) * 1e6 * growth),
    "reporting_period": reporting_period,
    "description": "Health underwriting: similar to non-life techniques",
})

# Life (minimal for P&C insurer)
risk_factors.append({
    "risk_module": "life",
    "risk_sub_module": "life_expense",
    "charge_eur": to_eur(rng.uniform(5, 15) * 1e6 * growth),
    "reporting_period": reporting_period,
    "description": "Life underwriting: expense risk (minor for P&C)",
})

write_quarterly_table(pd.DataFrame(risk_factors), "risk_factors",
            "SCR sub-module charges by risk module — recalculated each quarter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Volume Measures (for S.26.06)

# COMMAND ----------

volume_rows = []
for lob_code, cfg in TRIANGLE_LOB.items():
    ultimate = cfg["ultimate_base"] * (1.03)**9
    combined_ratio = rng.uniform(0.92, 0.98)
    earned_premium_net = round(ultimate / combined_ratio, 2)
    written_next = round(earned_premium_net * 1.03 * (1 + rng.uniform(-0.02, 0.05)), 2)
    reserve_factors = {"long": 1.80, "medium": 1.10, "short": 0.55}
    be_claims = round(earned_premium_net * reserve_factors[cfg["tail"]] * (1 + rng.uniform(-0.10, 0.10)), 2)
    premium_prov_factors = {"long": 0.25, "medium": 0.18, "short": 0.10}
    be_premium = round(earned_premium_net * premium_prov_factors[cfg["tail"]] * (1 + rng.uniform(-0.05, 0.05)), 2)

    volume_rows.append({
        "lob_code": int(lob_code),
        "lob_name": cfg["name"],
        "earned_premium_net": earned_premium_net,
        "written_premium_net_next_year": written_next,
        "best_estimate_claims_provision": be_claims,
        "best_estimate_premium_provision": be_premium,
        "reporting_period": reporting_period,
    })

write_quarterly_table(pd.DataFrame(volume_rows), "volume_measures",
            "Premium & reserve volume measures by LoB — feeds S.26.06")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Exposures (Igloo input — by peril & LoB)
# MAGIC
# MAGIC Simulated exposure sets that would be sent to a stochastic engine like Igloo.

# COMMAND ----------

PERILS = ["windstorm", "flood", "earthquake", "hail", "subsidence", "freeze", "wildfire"]
exposure_rows = []

for lob in LOB_CONFIG:
    for peril in PERILS:
        # Not all LoBs are exposed to all perils
        if lob["code"] in [1, 2] and peril not in ["flood", "earthquake"]:
            continue  # health/income not exposed to most nat cat
        if lob["code"] == 12 and peril not in ["flood", "earthquake"]:
            continue

        n_risks = int(rng.uniform(20, 200))
        tsi = to_eur(rng.lognormal(18, 1.5))  # total sum insured
        agg_deductible = to_eur(tsi * rng.uniform(0.001, 0.01))
        agg_limit = to_eur(tsi * rng.uniform(0.5, 1.0))

        exposure_rows.append({
            "exposure_id": f"EXP-{lob['code']}-{peril[:4].upper()}-{reporting_period}",
            "lob_code": lob["code"],
            "lob_name": lob["name"],
            "peril": peril,
            "number_of_risks": n_risks,
            "total_sum_insured_eur": tsi,
            "aggregate_deductible_eur": agg_deductible,
            "aggregate_limit_eur": agg_limit,
            "currency": "EUR",
            "reporting_period": reporting_period,
        })

write_quarterly_table(pd.DataFrame(exposure_rows), "exposures",
            "Exposure sets by peril & LoB — input for stochastic engine (Igloo)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Igloo Results (simulated stochastic output)
# MAGIC
# MAGIC Simulates what a stochastic engine (Igloo / RAFM / ReMetrica) would return:
# MAGIC VaR & TVaR at various return periods, by peril and LoB, gross/net/ceded.

# COMMAND ----------

RETURN_PERIODS = [10, 25, 50, 100, 200, 500]
igloo_rows = []

for lob in LOB_CONFIG:
    lob_gwp = TOTAL_GWP_M * 1e6 * lob["gwp_share"]

    for peril in PERILS:
        # Skip non-exposed combinations
        if lob["code"] in [1, 2] and peril not in ["flood", "earthquake"]:
            continue
        if lob["code"] == 12 and peril not in ["flood", "earthquake"]:
            continue

        # Base AAL (average annual loss) as fraction of GWP
        aal_pct = rng.uniform(0.005, 0.04)
        aal_gross = lob_gwp * aal_pct

        for rp in RETURN_PERIODS:
            # VaR scales roughly with log of return period
            scale = np.log(rp) / np.log(200)
            var_gross = to_eur(aal_gross * rp * scale * rng.uniform(0.8, 1.2))
            tvar_gross = to_eur(var_gross * rng.uniform(1.10, 1.35))

            cession = LOB_CESSION[lob["code"]]
            var_ceded = to_eur(var_gross * cession * rng.uniform(0.7, 1.0))
            tvar_ceded = to_eur(tvar_gross * cession * rng.uniform(0.7, 1.0))

            igloo_rows.append({
                "lob_code": lob["code"],
                "lob_name": lob["name"],
                "peril": peril,
                "return_period": rp,
                "var_gross_eur": var_gross,
                "tvar_gross_eur": tvar_gross,
                "var_ceded_eur": var_ceded,
                "tvar_ceded_eur": tvar_ceded,
                "var_net_eur": to_eur(var_gross - var_ceded),
                "tvar_net_eur": to_eur(tvar_gross - tvar_ceded),
                "num_simulations": 10000,
                "model_version": "Igloo 5.2.1",
                "run_timestamp": datetime.now().isoformat(),
                "reporting_period": reporting_period,
            })

write_quarterly_table(pd.DataFrame(igloo_rows), "igloo_results",
            "Simulated stochastic engine output — VaR/TVaR by peril, LoB, return period")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Own Funds & Balance Sheet

# COMMAND ----------

# Own funds components
own_funds_rows = [
    {"component": "ordinary_share_capital", "tier": 1, "amount_eur": to_eur(500e6 * growth), "reporting_period": reporting_period},
    {"component": "share_premium", "tier": 1, "amount_eur": to_eur(200e6 * growth), "reporting_period": reporting_period},
    {"component": "reconciliation_reserve", "tier": 1, "amount_eur": to_eur(rng.uniform(600, 800) * 1e6 * growth), "reporting_period": reporting_period},
    {"component": "subordinated_liabilities_t1", "tier": 1, "amount_eur": to_eur(150e6 * growth), "reporting_period": reporting_period},
    {"component": "subordinated_liabilities_t2", "tier": 2, "amount_eur": to_eur(rng.uniform(200, 300) * 1e6 * growth), "reporting_period": reporting_period},
    {"component": "ancillary_own_funds", "tier": 3, "amount_eur": to_eur(rng.uniform(30, 60) * 1e6 * growth), "reporting_period": reporting_period},
]

write_quarterly_table(pd.DataFrame(own_funds_rows), "own_funds",
            "Own funds components by tier — feeds solvency ratio")

# Balance sheet items
total_assets_val = to_eur(TOTAL_ASSETS_M * 1e6 * (1 + rng.uniform(-0.02, 0.02)) * growth)
tp_val = to_eur(total_assets_val * rng.uniform(0.55, 0.65))
other_liabilities = to_eur(total_assets_val * rng.uniform(0.05, 0.10))
excess = to_eur(total_assets_val - tp_val - other_liabilities)

bs_rows = [
    {"item": "total_assets", "category": "assets", "amount_eur": total_assets_val, "reporting_period": reporting_period},
    {"item": "investments", "category": "assets", "amount_eur": to_eur(total_assets_val * 0.92), "reporting_period": reporting_period},
    {"item": "reinsurance_recoverables", "category": "assets", "amount_eur": to_eur(total_assets_val * 0.05), "reporting_period": reporting_period},
    {"item": "cash_and_equivalents", "category": "assets", "amount_eur": to_eur(total_assets_val * 0.03), "reporting_period": reporting_period},
    {"item": "technical_provisions_gross", "category": "liabilities", "amount_eur": tp_val, "reporting_period": reporting_period},
    {"item": "reinsurance_payables", "category": "liabilities", "amount_eur": to_eur(total_assets_val * 0.03), "reporting_period": reporting_period},
    {"item": "other_liabilities", "category": "liabilities", "amount_eur": other_liabilities, "reporting_period": reporting_period},
    {"item": "excess_of_assets_over_liabilities", "category": "equity", "amount_eur": excess, "reporting_period": reporting_period},
]

write_quarterly_table(pd.DataFrame(bs_rows), "balance_sheet",
            "Solvency II balance sheet — assets, liabilities, excess")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Pipeline SLA Status (for Control Tower)
# MAGIC
# MAGIC Tracks when each data feed arrived relative to SLA deadlines.
# MAGIC Simulates realistic arrival patterns — most on time, some late.

# COMMAND ----------

sla_deadline_day = 15  # 15th of month after quarter-end
sla_month = (rp_quarter * 3) % 12 + 1
sla_year = rp_year if sla_month > 1 else rp_year + 1
sla_deadline = datetime(sla_year, sla_month, sla_deadline_day, 18, 0, 0)

FEED_CONFIG = [
    {"feed": "assets", "source": "Investment Platform (Simcorp)", "typical_days_early": 5},
    {"feed": "premiums", "source": "Policy Admin System (Guidewire)", "typical_days_early": 3},
    {"feed": "claims", "source": "Claims Management System", "typical_days_early": 4},
    {"feed": "expenses", "source": "Finance / ERP (SAP)", "typical_days_early": 1},
    {"feed": "risk_factors", "source": "Risk Engine (Igloo/RAFM)", "typical_days_early": 2},
    {"feed": "reinsurance", "source": "RI Admin (Solvara)", "typical_days_early": 10},
]

sla_rows = []
for fc in FEED_CONFIG:
    days_early = fc["typical_days_early"] + int(rng.uniform(-3, 3))
    arrival = sla_deadline - timedelta(days=days_early)

    # Make expenses late in Q4 for demo narrative
    if fc["feed"] == "expenses" and rp_quarter == 4:
        arrival = sla_deadline + timedelta(days=2, hours=int(rng.uniform(1, 8)))

    status = "on_time" if arrival <= sla_deadline else "late"

    # Count rows from the actual table
    try:
        feed_count = spark.table(f"{catalog}.{schema}.{fc['feed']}").filter(
            f"reporting_period = '{reporting_period}'"
        ).count()
    except Exception:
        feed_count = int(rng.uniform(1000, 50000))

    dq_pass = round(rng.uniform(0.985, 1.0), 4)
    if fc["feed"] == "expenses" and rp_quarter == 4:
        dq_pass = round(rng.uniform(0.965, 0.985), 4)  # slightly worse for late data

    sla_rows.append({
        "reporting_period": reporting_period,
        "feed_name": fc["feed"],
        "source_system": fc["source"],
        "sla_deadline": sla_deadline,
        "actual_arrival": arrival,
        "row_count": feed_count,
        "status": status,
        "dq_pass_rate": dq_pass,
        "notes": f"Arrived {abs(days_early)} days {'early' if arrival <= sla_deadline else 'late'}",
    })

write_quarterly_table(pd.DataFrame(sla_rows), "pipeline_sla_status",
            "Pipeline SLA tracking — feed arrival times vs deadlines for Control Tower")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. DQ Expectation Results (for DQ Dashboard)
# MAGIC
# MAGIC Synthetic DLT expectation results mirroring what the pipeline produces.

# COMMAND ----------

DQ_EXPECTATIONS = [
    # S.06.02 pipeline
    {"pipeline": "S.06.02 List of Assets", "table": "assets_enriched",
     "expectation": "asset_id_not_null", "action": "DROP ROW", "base_total": 5000},
    {"pipeline": "S.06.02 List of Assets", "table": "assets_enriched",
     "expectation": "sii_value_positive", "action": "FAIL UPDATE", "base_total": 5000},
    {"pipeline": "S.06.02 List of Assets", "table": "assets_enriched",
     "expectation": "cic_code_valid", "action": "DROP ROW", "base_total": 5000},
    {"pipeline": "S.06.02 List of Assets", "table": "assets_enriched",
     "expectation": "currency_not_null", "action": "DROP ROW", "base_total": 5000},
    {"pipeline": "S.06.02 List of Assets", "table": "s0602_list_of_assets",
     "expectation": "c0040_asset_id_present", "action": "DROP ROW", "base_total": 5000},
    {"pipeline": "S.06.02 List of Assets", "table": "s0602_list_of_assets",
     "expectation": "c0170_sii_positive", "action": "FAIL UPDATE", "base_total": 5000},
    # S.05.01 pipeline
    {"pipeline": "S.05.01 Premiums Claims Expenses", "table": "premiums_by_lob",
     "expectation": "gross_written_positive", "action": "DROP ROW", "base_total": 7},
    {"pipeline": "S.05.01 Premiums Claims Expenses", "table": "premiums_by_lob",
     "expectation": "net_equals_gross_minus_ri", "action": "WARN", "base_total": 7},
    {"pipeline": "S.05.01 Premiums Claims Expenses", "table": "claims_by_lob",
     "expectation": "gross_incurred_positive", "action": "DROP ROW", "base_total": 7},
    {"pipeline": "S.05.01 Premiums Claims Expenses", "table": "s0501_summary",
     "expectation": "combined_ratio_realistic", "action": "DROP ROW", "base_total": 7},
    # S.25.01 pipeline
    {"pipeline": "S.25.01 SCR Template", "table": "s2501_scr_breakdown",
     "expectation": "row_id_present", "action": "DROP ROW", "base_total": 17},
    {"pipeline": "S.25.01 SCR Template", "table": "s2501_scr_breakdown",
     "expectation": "amount_not_null", "action": "DROP ROW", "base_total": 17},
    {"pipeline": "S.25.01 SCR Template", "table": "s2501_summary",
     "expectation": "solvency_ratio_positive", "action": "FAIL UPDATE", "base_total": 1},
    {"pipeline": "S.25.01 SCR Template", "table": "s2501_summary",
     "expectation": "scr_positive", "action": "FAIL UPDATE", "base_total": 1},
]

dq_rows = []
for exp in DQ_EXPECTATIONS:
    total = exp["base_total"]
    # Most expectations pass perfectly; a few have small failure counts
    if rng.random() < 0.3:  # 30% of checks have some failures
        failing = int(rng.uniform(1, max(2, total * 0.005)))
    else:
        failing = 0
    # Quality improves over time
    if rp_quarter > 1 and failing > 0:
        failing = max(0, failing - rp_quarter + 1)

    passing = total - failing
    dq_rows.append({
        "reporting_period": reporting_period,
        "pipeline_name": exp["pipeline"],
        "table_name": exp["table"],
        "expectation_name": exp["expectation"],
        "total_records": total,
        "passing_records": passing,
        "failing_records": failing,
        "pass_rate": round(passing / total, 4) if total > 0 else 1.0,
        "action": exp["action"],
        "evaluated_at": sla_deadline - timedelta(hours=int(rng.uniform(1, 48))),
    })

write_quarterly_table(pd.DataFrame(dq_rows), "dq_expectation_results",
            "DQ expectation results — pass/fail rates from DLT pipeline expectations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Cross-QRT Reconciliation (for Reconciliation Tab)

# COMMAND ----------

recon_rows = []

# Check 1: Total SII assets (S.06.02) vs balance sheet
try:
    s0602_total = float(spark.sql(f"""
        SELECT SUM(CAST(C0170_Total_Solvency_II_Amount AS DOUBLE))
        FROM {catalog}.{schema}.s0602_list_of_assets
        WHERE reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    s0602_total = TOTAL_ASSETS_M * 1e6

try:
    bs_total = float(spark.sql(f"""
        SELECT CAST(amount_eur AS DOUBLE)
        FROM {catalog}.{schema}.balance_sheet
        WHERE item = 'total_assets' AND reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    bs_total = s0602_total * rng.uniform(0.98, 1.02)

diff = abs(s0602_total - bs_total)
recon_rows.append({
    "reporting_period": reporting_period,
    "check_name": "total_assets_s0602_vs_balance_sheet",
    "check_description": "Total SII assets from S.06.02 should match balance sheet total assets",
    "source_qrt": "S.06.02",
    "target_qrt": "Balance Sheet",
    "source_value": round(s0602_total, 2),
    "target_value": round(bs_total, 2),
    "difference": round(diff, 2),
    "tolerance": round(bs_total * 0.02, 2),
    "status": "MATCH" if diff < bs_total * 0.02 else "MISMATCH",
})

# Check 2: GWP in S.05.01 vs sum of premium transactions
try:
    s0501_gwp = float(spark.sql(f"""
        SELECT SUM(CAST(amount_eur AS DOUBLE))
        FROM {catalog}.{schema}.s0501_premiums_claims_expenses
        WHERE template_row_id = 'R0110' AND lob_code = 0 AND reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    s0501_gwp = quarterly_gwp

try:
    prem_gwp = float(spark.sql(f"""
        SELECT SUM(gross_written_premium)
        FROM {catalog}.{schema}.premiums
        WHERE reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    prem_gwp = s0501_gwp * rng.uniform(0.99, 1.01)

diff2 = abs(s0501_gwp - prem_gwp)
recon_rows.append({
    "reporting_period": reporting_period,
    "check_name": "gwp_s0501_vs_premiums",
    "check_description": "Gross written premium in S.05.01 Total should match sum of premium transactions",
    "source_qrt": "S.05.01",
    "target_qrt": "Premiums (Bronze)",
    "source_value": round(s0501_gwp, 2),
    "target_value": round(prem_gwp, 2),
    "difference": round(diff2, 2),
    "tolerance": round(prem_gwp * 0.01, 2),
    "status": "MATCH" if diff2 < prem_gwp * 0.01 else "MISMATCH",
})

# Check 3: SCR < Eligible Own Funds (solvency OK)
try:
    solv = spark.sql(f"""
        SELECT scr_eur, eligible_own_funds_eur
        FROM {catalog}.{schema}.s2501_summary
        WHERE reporting_period = '{reporting_period}'
    """).first()
    scr_val = float(solv[0] or 0)
    eof_val = float(solv[1] or 0)
except Exception:
    scr_val = TARGET_SCR_M * 1e6
    eof_val = TARGET_OWN_FUNDS_M * 1e6

recon_rows.append({
    "reporting_period": reporting_period,
    "check_name": "scr_vs_own_funds",
    "check_description": "Eligible own funds must exceed SCR for solvency compliance",
    "source_qrt": "S.25.01",
    "target_qrt": "Own Funds",
    "source_value": round(scr_val, 2),
    "target_value": round(eof_val, 2),
    "difference": round(eof_val - scr_val, 2),
    "tolerance": 0,
    "status": "MATCH" if eof_val > scr_val else "MISMATCH",
})

# Check 4: Number of assets in S.06.02 vs raw assets table
try:
    qrt_count = int(spark.sql(f"""
        SELECT COUNT(*) FROM {catalog}.{schema}.s0602_list_of_assets
        WHERE reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    qrt_count = N_ASSETS

recon_rows.append({
    "reporting_period": reporting_period,
    "check_name": "asset_count_s0602_vs_raw",
    "check_description": "Asset count in S.06.02 should match raw assets (minus DQ drops)",
    "source_qrt": "S.06.02",
    "target_qrt": "Assets (Bronze)",
    "source_value": float(qrt_count),
    "target_value": float(N_ASSETS),
    "difference": float(abs(qrt_count - N_ASSETS)),
    "tolerance": 10.0,
    "status": "MATCH" if abs(qrt_count - N_ASSETS) <= 10 else "WITHIN_TOLERANCE",
})

write_quarterly_table(pd.DataFrame(recon_rows), "cross_qrt_reconciliation",
            "Cross-QRT reconciliation checks — consistency validation between QRTs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Model Registry Log (for Model Governance Tab)

# COMMAND ----------

model_rows = []

# Champion (v1, 2025 calibration) — used for all quarters
try:
    champ_scr = float(spark.sql(f"""
        SELECT amount_eur FROM {catalog}.{schema}.scr_results
        WHERE component = 'SCR' AND reporting_period = '{reporting_period}'
    """).first()[0] or 0)
except Exception:
    champ_scr = TARGET_SCR_M * 1e6 * growth

model_rows.append({
    "reporting_period": reporting_period,
    "model_name": "standard_formula",
    "model_version": 1,
    "alias": "Champion",
    "calibration_year": 2025,
    "scr_result_eur": round(champ_scr, 2),
    "run_timestamp": sla_deadline - timedelta(days=3),
    "registered_by": "laurence.ryszka@databricks.com",
    "description": "EIOPA 2025 Standard Formula — production calibration",
})

# Challenger (v2, 2026 calibration) — shows what-if
challenger_scr = champ_scr * rng.uniform(1.02, 1.06)  # slightly higher due to tighter correlations
model_rows.append({
    "reporting_period": reporting_period,
    "model_name": "standard_formula",
    "model_version": 2,
    "alias": "Challenger",
    "calibration_year": 2026,
    "scr_result_eur": round(challenger_scr, 2),
    "run_timestamp": sla_deadline - timedelta(days=2),
    "registered_by": "laurence.ryszka@databricks.com",
    "description": "EIOPA 2026 Updated Calibration — tighter market/NL correlation, higher op risk",
})

write_quarterly_table(pd.DataFrame(model_rows), "model_registry_log",
            "Model version usage log — Champion vs Challenger SCR results per quarter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print(f"  DATA GENERATION COMPLETE — {reporting_period}")
print("=" * 70)
print(f"  Catalog: {catalog}")
print(f"  Schema:  {schema}")
print(f"  Mode:    {mode}")
print()

tables = [
    "counterparties", "assets", "policies", "premiums", "claims", "expenses",
    "reinsurance", "claims_triangles", "risk_factors", "scr_parameters",
    "volume_measures", "exposures", "igloo_results", "own_funds", "balance_sheet",
    "pipeline_sla_status", "dq_expectation_results", "cross_qrt_reconciliation",
    "model_registry_log",
]

for t in tables:
    try:
        cnt = spark.table(f"{catalog}.{schema}.{t}").count()
        print(f"  {t:30s} {cnt:>10,} rows")
    except Exception:
        print(f"  {t:30s} NOT FOUND")

print()
print("=" * 70)
