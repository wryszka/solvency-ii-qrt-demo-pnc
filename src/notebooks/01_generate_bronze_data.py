# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Bronze Data
# MAGIC Generates synthetic insurance data for a mid-size European P&C insurer.
# MAGIC
# MAGIC **Tables produced** (all in `qrt_demo_bronze`):
# MAGIC - `counterparties` — master counterparty register
# MAGIC - `assets` — investment register (S.06.02 source)
# MAGIC - `policies` — policy register
# MAGIC - `premiums_transactions` — premium accounting (S.05.01 source)
# MAGIC - `claims_transactions` — claims accounting (S.19.01 source)
# MAGIC - `claims_triangles` — pre-aggregated development triangles
# MAGIC - `expenses` — expense allocation (S.05.01 source)
# MAGIC - `reinsurance_contracts` — reinsurance programme (S.30/S.31 source)
# MAGIC - `technical_provisions` — TP components (S.17.01 source)
# MAGIC - `own_funds_components` — own-funds breakdown (S.23.01 source)
# MAGIC - `risk_factors` — SCR sub-module charges (S.25.01 source)
# MAGIC - `scr_parameters` — SCR calibration parameters
# MAGIC - `balance_sheet_items` — Solvency II balance sheet (S.02.01 source)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_classic_aws_us_catalog")  # <-- Change to your catalog name
dbutils.widgets.text("schema_name", "solvency2demo")
dbutils.widgets.text("reporting_date", "2025-12-31")
dbutils.widgets.text("entity_name", "Bricksurance SE")
dbutils.widgets.text("random_seed", "42")
dbutils.widgets.text("scale_factor", "1.0")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
reporting_date = dbutils.widgets.get("reporting_date")
entity_name = dbutils.widgets.get("entity_name")
random_seed = int(dbutils.widgets.get("random_seed"))
scale_factor = float(dbutils.widgets.get("scale_factor"))

reporting_year = int(reporting_date[:4])
reporting_period = f"{reporting_year}-Q4"

print(f"Catalog:          {catalog}")
print(f"Schema:           {schema}")
print(f"Reporting date:   {reporting_date}")
print(f"Entity:           {entity_name}")
print(f"Random seed:      {random_seed}")
print(f"Scale factor:     {scale_factor}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %run ./helpers/lineage_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and seeded random state

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import string
import hashlib
import itertools

rng = np.random.RandomState(random_seed)
rpt_date = datetime.strptime(reporting_date, "%Y-%m-%d").date()

# Initialise lineage logger
pipeline_params = {
    "catalog_name": catalog, "schema_name": schema, "reporting_date": reporting_date,
    "entity_name": entity_name, "random_seed": random_seed, "scale_factor": scale_factor
}
lineage = LineageLogger(spark, catalog, schema, reporting_period)
lineage.start_step("01_generate_bronze_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data & helper functions

# COMMAND ----------

# ── Lines of business ────────────────────────────────────────────────
LOB_CONFIG = [
    {"code": 1,  "name": "Medical expense insurance",                   "gwp_share": 0.08},
    {"code": 2,  "name": "Income protection insurance",                 "gwp_share": 0.06},
    {"code": 4,  "name": "Motor vehicle liability insurance",           "gwp_share": 0.25},
    {"code": 5,  "name": "Other motor insurance",                       "gwp_share": 0.15},
    {"code": 7,  "name": "Fire and other damage to property insurance", "gwp_share": 0.25},
    {"code": 8,  "name": "General liability insurance",                 "gwp_share": 0.13},
    {"code": 12, "name": "Miscellaneous financial loss",                "gwp_share": 0.08},
]
LOB_CODES = [l["code"] for l in LOB_CONFIG]
LOB_NAMES = {l["code"]: l["name"] for l in LOB_CONFIG}
GWP_SHARES = {l["code"]: l["gwp_share"] for l in LOB_CONFIG}

# ── EUR targets (millions) ──────────────────────────────────────────
TOTAL_ASSETS_M = 6500.0
TOTAL_GWP_M = 2000.0
TARGET_COMBINED_RATIO = 0.96
TARGET_SCR_M = 1150.0
TARGET_BSCR_M = 1350.0
TARGET_OWN_FUNDS_M = 2000.0

# ── Country / Sovereign pools ───────────────────────────────────────
SOVEREIGN_COUNTRIES = ["DE", "FR", "NL", "IT", "ES", "BE", "AT"]
SOVEREIGN_NAMES = {
    "DE": "Federal Republic of Germany",
    "FR": "Republic of France",
    "NL": "Kingdom of the Netherlands",
    "IT": "Republic of Italy",
    "ES": "Kingdom of Spain",
    "BE": "Kingdom of Belgium",
    "AT": "Republic of Austria",
}
SOVEREIGN_WEIGHTS = [0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.07]

CORPORATE_SECTORS_NACE = {
    "K64": "Financial service activities",
    "K65": "Insurance and reinsurance",
    "C20": "Manufacture of chemicals",
    "D35": "Electricity and gas supply",
    "H49": "Land transport",
    "J61": "Telecommunications",
    "C29": "Manufacture of motor vehicles",
    "F41": "Construction of buildings",
    "G47": "Retail trade",
    "M69": "Legal and accounting activities",
}

CUSTODIANS = [
    "Euroclear Bank SA/NV",
    "Clearstream Banking AG",
    "BNP Paribas Securities Services",
    "Deutsche Bank AG – Custody",
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

EUROPEAN_CITIES = ["Munich", "Paris", "Amsterdam", "Milan", "Madrid",
                   "Brussels", "Vienna", "Zurich", "Dublin", "Frankfurt",
                   "Stockholm", "Helsinki", "Lisbon", "Copenhagen", "Luxembourg"]

# Reinsurer names
REINSURER_NAMES = [
    "Munich Re AG", "Swiss Re Ltd", "Hannover Rueck SE",
    "SCOR SE", "General Reinsurance AG", "PartnerRe Ltd",
    "Everest Re Group", "TransRe", "RenaissanceRe Holdings",
    "Arch Capital Group", "Mapfre RE", "Korean Reinsurance Company",
]

CLAIM_CAUSES = {
    1:  ["illness", "hospitalisation", "outpatient_treatment", "chronic_condition"],
    2:  ["disability", "long_term_illness", "accident_injury", "mental_health"],
    4:  ["collision", "pedestrian_injury", "multi_vehicle", "single_vehicle", "fatal_accident"],
    5:  ["theft", "vandalism", "hail_damage", "windscreen", "fire", "flood_damage"],
    7:  ["fire", "water_damage", "storm", "burglary", "subsidence", "explosion", "natural_catastrophe"],
    8:  ["product_liability", "professional_indemnity", "public_liability", "employers_liability", "environmental"],
    12: ["fraud", "business_interruption", "cyber_incident", "credit_default", "contract_frustration"],
}

# Average claim severity by LoB (EUR), used as mu of log-normal
LOB_SEVERITY_MU = {1: 8.5, 2: 8.8, 4: 9.2, 5: 8.0, 7: 9.0, 8: 9.5, 12: 9.0}
LOB_SEVERITY_SIGMA = {1: 1.2, 2: 1.3, 4: 1.4, 5: 1.1, 7: 1.5, 8: 1.6, 12: 1.4}

# Claim development speed (proportion paid by dev year)
LOB_DEV_SPEED = {
    1:  [0.55, 0.30, 0.10, 0.03, 0.01, 0.01],
    2:  [0.40, 0.25, 0.15, 0.10, 0.06, 0.04],
    4:  [0.30, 0.25, 0.20, 0.12, 0.08, 0.05],
    5:  [0.65, 0.25, 0.07, 0.02, 0.01, 0.00],
    7:  [0.55, 0.30, 0.10, 0.03, 0.01, 0.01],
    8:  [0.15, 0.20, 0.20, 0.18, 0.15, 0.12],
    12: [0.35, 0.30, 0.15, 0.10, 0.06, 0.04],
}

# Reinsurance cession rates by LoB
LOB_CESSION = {1: 0.15, 2: 0.15, 4: 0.25, 5: 0.20, 7: 0.30, 8: 0.25, 12: 0.20}

# ── Helper functions ────────────────────────────────────────────────

def make_lei(seed_str: str) -> str:
    """Deterministic 20-char pseudo-LEI from a seed string."""
    h = hashlib.sha256(f"{random_seed}_{seed_str}".encode()).hexdigest().upper()
    prefix = h[:4]
    middle = h[4:18]
    check = h[18:20]
    return (prefix + middle + check)[:20]

def make_isin(country: str, idx: int) -> str:
    """Pseudo-ISIN: country + 9 alphanum + check digit."""
    h = hashlib.md5(f"{random_seed}_{country}_{idx}".encode()).hexdigest().upper()
    return f"{country}{h[:9]}0"

def random_date(start: date, end: date, n: int = 1) -> list:
    """Return n random dates between start and end."""
    delta = (end - start).days
    if delta <= 0:
        return [start] * n
    days = rng.randint(0, delta, size=n)
    return [start + timedelta(int(d)) for d in days]

def quarter_label(d: date) -> str:
    return f"{d.year}-Q{(d.month - 1) // 3 + 1}"

def to_eur(x):
    """Round to 2dp."""
    return round(float(x), 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Counterparties (~500 rows)

# COMMAND ----------

N_COUNTERPARTIES = 500

# ── Build name pools ─────────────────────────────────────────────────
_corp_first = ["Alpha", "Beta", "Gamma", "Delta", "Euro", "Nord", "Süd", "Atlas",
               "Hansa", "Rhein", "Danube", "Baltic", "Iberian", "Celtic", "Nordic",
               "Adriatic", "Pannonia", "Helvetia", "Benelux", "Meridian", "Boreal",
               "Continental", "Maritime", "Alpen", "Titan", "Orion", "Polaris",
               "Vega", "Sirius", "Cygnus", "Lyra", "Apex", "Nexus", "Vertex",
               "Zenith", "Summit", "Pinnacle", "Prima", "Optima", "Nova"]
_corp_suffix = ["AG", "SE", "GmbH", "NV", "SA", "SAS", "BV", "SpA", "Ltd", "Plc",
                "AB", "OY", "AS"]
_corp_mid = ["Capital", "Finance", "Holdings", "Industries", "Group", "Invest",
             "Partners", "Solutions", "Services", "Technologies", "Energy",
             "Logistics", "Trading", "Insurance", "Reinsurance", "Banking",
             "Securities", "Asset Management", "Ventures", "Real Estate"]

counterparties = []
countries_pool = SOVEREIGN_COUNTRIES + ["LU", "IE", "FI", "SE", "DK", "PT", "CH"]
nace_codes = list(CORPORATE_SECTORS_NACE.keys())
cp_types = ["issuer", "issuer", "issuer", "issuer", "reinsurer", "bank", "broker"]
# 19 weights matching SP_RATINGS: AAA(1), AA+/AA/AA-(3), A+/A/A-(3), BBB+/BBB/BBB-(3),
# BB+/BB/BB-(3), B+/B/B-(3), CCC+/CCC(2), NR(1)
rating_weights = ([0.05] + [0.07]*3 + [0.10]*3 + [0.09]*3 +
                  [0.04]*3 + [0.02]*3 + [0.01]*2 + [0.01])

for i in range(N_COUNTERPARTIES):
    first = _corp_first[rng.randint(len(_corp_first))]
    mid = _corp_mid[rng.randint(len(_corp_mid))]
    suffix = _corp_suffix[rng.randint(len(_corp_suffix))]
    name = f"{first} {mid} {suffix}"
    country = countries_pool[rng.randint(len(countries_pool))]
    rating = rng.choice(SP_RATINGS, p=np.array(rating_weights)/sum(rating_weights))
    cqs = RATING_TO_CQS[rating]
    cp_type = cp_types[rng.randint(len(cp_types))]
    sector = nace_codes[rng.randint(len(nace_codes))]
    lei = make_lei(f"cp_{i}")
    group_name = f"{first} Group" if rng.random() < 0.3 else None
    group_lei = make_lei(f"grp_{first}") if group_name else None

    counterparties.append({
        "counterparty_id": f"CP{i+1:05d}",
        "counterparty_name": name,
        "lei": lei,
        "country": country,
        "sector_nace": sector,
        "credit_rating": rating,
        "credit_quality_step": int(cqs),
        "counterparty_type": cp_type,
        "is_regulated": bool(rng.random() < 0.7),
        "group_name": group_name,
        "group_lei": group_lei,
    })

df_counterparties = pd.DataFrame(counterparties)
print(f"Counterparties generated: {len(df_counterparties)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Assets (~5000 * scale_factor rows)

# COMMAND ----------

N_ASSETS = int(5000 * scale_factor)
TOTAL_MV = TOTAL_ASSETS_M * 1e6  # 6.5B EUR

# Allocation buckets
alloc = {
    "government_bonds": 0.60,
    "corporate_bonds":  0.20,
    "equity":           0.10,
    "ciu":              0.05,
    "property_other":   0.05,
}

# Number of assets per bucket
n_gov  = int(N_ASSETS * 0.60)
n_corp = int(N_ASSETS * 0.20)
n_eq   = int(N_ASSETS * 0.10)
n_ciu  = int(N_ASSETS * 0.05)
n_oth  = N_ASSETS - n_gov - n_corp - n_eq - n_ciu

# Filter counterparties by type for reuse
issuers = df_counterparties[df_counterparties["counterparty_type"] == "issuer"]

def _gen_market_values(n: int, total_target: float, sigma: float = 0.8) -> np.ndarray:
    """Log-normal market values scaled to total_target."""
    raw = rng.lognormal(mean=0.0, sigma=sigma, size=n)
    return raw / raw.sum() * total_target

assets_rows = []
asset_idx = 0

# ─── Government bonds ──────────────────────────────────────────────
gov_mv = _gen_market_values(n_gov, TOTAL_MV * alloc["government_bonds"], sigma=0.9)
for i in range(n_gov):
    country = rng.choice(SOVEREIGN_COUNTRIES, p=SOVEREIGN_WEIGHTS)
    cic = f"{country}11"
    acq_date = random_date(date(2015, 1, 1), date(2025, 6, 30))[0]
    mat_years = rng.uniform(3, 15)
    mat_date = acq_date + timedelta(days=int(mat_years * 365.25))
    coupon = round(rng.uniform(0.005, 0.035), 4)
    par = to_eur(gov_mv[i] * rng.uniform(0.92, 1.08))
    mod_dur = round(rng.uniform(2.5, 12.0), 2)
    rating_pool = ["AAA", "AA+", "AA", "AA-", "A+", "A"]
    rating = rng.choice(rating_pool, p=[0.25, 0.20, 0.20, 0.15, 0.10, 0.10])
    cqs = RATING_TO_CQS[rating]

    assets_rows.append({
        "asset_id": f"A{asset_idx+1:06d}",
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
        "accrued_interest": to_eur(par * coupon * rng.uniform(0.0, 0.5)),
        "coupon_rate": coupon,
        "credit_rating": rating,
        "credit_quality_step": int(cqs),
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "valuation_date": rpt_date,
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": mod_dur,
        "asset_class": "government_bonds",
        "reporting_period": reporting_period,
    })
    asset_idx += 1

# ─── Corporate bonds ───────────────────────────────────────────────
corp_mv = _gen_market_values(n_corp, TOTAL_MV * alloc["corporate_bonds"], sigma=0.85)
for i in range(n_corp):
    cp = issuers.iloc[rng.randint(len(issuers))]
    cic_suffix = rng.choice(["21", "22"], p=[0.85, 0.15])
    cic = f"XL{cic_suffix}"
    acq_date = random_date(date(2016, 1, 1), date(2025, 6, 30))[0]
    mat_years = rng.uniform(2, 10)
    mat_date = acq_date + timedelta(days=int(mat_years * 365.25))
    coupon = round(rng.uniform(0.015, 0.06), 4)
    par = to_eur(corp_mv[i] * rng.uniform(0.90, 1.10))
    mod_dur = round(rng.uniform(1.5, 8.0), 2)

    rating_weights_corp = [0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.12,
                           0.10, 0.08, 0.06, 0.04, 0.03, 0.02,
                           0.01, 0.01, 0.005, 0.002, 0.002, 0.001]
    rating_weights_corp = np.array(rating_weights_corp)
    rating_weights_corp /= rating_weights_corp.sum()
    rating = rng.choice(SP_RATINGS, p=rating_weights_corp)
    cqs = RATING_TO_CQS[rating]

    assets_rows.append({
        "asset_id": f"A{asset_idx+1:06d}",
        "asset_name": f"{cp['counterparty_name']} {coupon*100:.2f}% {mat_date.year}",
        "issuer_name": cp["counterparty_name"],
        "issuer_lei": cp["lei"],
        "issuer_country": cp["country"],
        "issuer_sector": cp["sector_nace"],
        "cic_code": cic,
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": mat_date,
        "par_value": par,
        "acquisition_cost": to_eur(corp_mv[i] * rng.uniform(0.93, 1.05)),
        "market_value_eur": to_eur(corp_mv[i]),
        "accrued_interest": to_eur(par * coupon * rng.uniform(0.0, 0.5)),
        "coupon_rate": coupon,
        "credit_rating": rating,
        "credit_quality_step": int(cqs),
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "valuation_date": rpt_date,
        "is_listed": True,
        "infrastructure_flag": bool(rng.random() < 0.05),
        "modified_duration": mod_dur,
        "asset_class": "corporate_bonds",
        "reporting_period": reporting_period,
    })
    asset_idx += 1

# ─── Listed equity ─────────────────────────────────────────────────
eq_mv = _gen_market_values(n_eq, TOTAL_MV * alloc["equity"], sigma=0.7)
_equity_names = [
    "Allianz SE", "AXA SA", "Zurich Insurance", "Generali SpA",
    "SAP SE", "Siemens AG", "ASML Holding NV", "TotalEnergies SE",
    "LVMH SE", "Unilever NV", "Nestlé SA", "Roche Holding AG",
    "Novartis AG", "Sanofi SA", "BNP Paribas SA", "Deutsche Bank AG",
    "ING Group NV", "Banco Santander SA", "Iberdrola SA", "Enel SpA",
    "Volkswagen AG", "BMW AG", "Daimler Truck AG", "Airbus SE",
    "Philips NV", "Nokia OYJ", "Ericsson AB", "Volvo AB",
    "Schneider Electric SE", "Danone SA", "Bayer AG", "BASF SE",
]

for i in range(n_eq):
    eq_name = _equity_names[i % len(_equity_names)]
    cp_match = issuers.iloc[rng.randint(len(issuers))]
    acq_date = random_date(date(2015, 1, 1), date(2025, 6, 30))[0]

    assets_rows.append({
        "asset_id": f"A{asset_idx+1:06d}",
        "asset_name": eq_name,
        "issuer_name": eq_name,
        "issuer_lei": make_lei(f"eq_{i}"),
        "issuer_country": rng.choice(SOVEREIGN_COUNTRIES),
        "issuer_sector": rng.choice(["K64", "C29", "J61", "D35", "G47", "C20"]),
        "cic_code": "XL31",
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": None,
        "par_value": None,
        "acquisition_cost": to_eur(eq_mv[i] * rng.uniform(0.60, 1.10)),
        "market_value_eur": to_eur(eq_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": None,
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "valuation_date": rpt_date,
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": "equity",
        "reporting_period": reporting_period,
    })
    asset_idx += 1

# ─── Collective investment undertakings ─────────────────────────────
ciu_mv = _gen_market_values(n_ciu, TOTAL_MV * alloc["ciu"], sigma=0.65)
_ciu_types = [("41", "Equity Fund"), ("42", "Debt Fund"), ("43", "Money Market Fund"),
              ("44", "Asset Allocation Fund"), ("45", "Real Estate Fund"), ("46", "Alternative Fund")]
for i in range(n_ciu):
    suffix, fund_type = _ciu_types[i % len(_ciu_types)]
    fund_name = f"{_corp_first[rng.randint(len(_corp_first))]} {fund_type}"
    acq_date = random_date(date(2017, 1, 1), date(2025, 6, 30))[0]

    assets_rows.append({
        "asset_id": f"A{asset_idx+1:06d}",
        "asset_name": fund_name,
        "issuer_name": fund_name,
        "issuer_lei": make_lei(f"ciu_{i}"),
        "issuer_country": rng.choice(["LU", "IE", "DE", "FR", "NL"]),
        "issuer_sector": "K64",
        "cic_code": f"XL{suffix}",
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": None,
        "par_value": None,
        "acquisition_cost": to_eur(ciu_mv[i] * rng.uniform(0.85, 1.05)),
        "market_value_eur": to_eur(ciu_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": None,
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "valuation_date": rpt_date,
        "is_listed": True,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": "ciu",
        "reporting_period": reporting_period,
    })
    asset_idx += 1

# ─── Property, cash, other ─────────────────────────────────────────
oth_mv = _gen_market_values(n_oth, TOTAL_MV * alloc["property_other"], sigma=0.8)
_oth_types = [("91", "property", "Property for own use"),
              ("92", "property", "Investment property"),
              ("71", "cash", "Cash at bank"),
              ("72", "cash", "Term deposit")]
for i in range(n_oth):
    suffix, aclass, desc = _oth_types[i % len(_oth_types)]
    acq_date = random_date(date(2010, 1, 1), date(2025, 6, 30))[0]
    is_cash = aclass == "cash"

    assets_rows.append({
        "asset_id": f"A{asset_idx+1:06d}",
        "asset_name": f"{desc} – {EUROPEAN_CITIES[rng.randint(len(EUROPEAN_CITIES))]}",
        "issuer_name": rng.choice(CUSTODIANS) if is_cash else entity_name,
        "issuer_lei": make_lei(f"oth_{i}"),
        "issuer_country": "DE",
        "issuer_sector": "L68" if not is_cash else "K64",
        "cic_code": f"DE{suffix}",
        "currency": "EUR",
        "acquisition_date": acq_date,
        "maturity_date": None,
        "par_value": to_eur(oth_mv[i]) if is_cash else None,
        "acquisition_cost": to_eur(oth_mv[i] * rng.uniform(0.70, 1.0)),
        "market_value_eur": to_eur(oth_mv[i]),
        "accrued_interest": 0.0,
        "coupon_rate": None,
        "credit_rating": None,
        "credit_quality_step": 0 if is_cash else None,
        "portfolio_type": "Non-life",
        "custodian_name": rng.choice(CUSTODIANS),
        "valuation_date": rpt_date,
        "is_listed": False,
        "infrastructure_flag": False,
        "modified_duration": None,
        "asset_class": aclass,
        "reporting_period": reporting_period,
    })
    asset_idx += 1

df_assets = pd.DataFrame(assets_rows)
actual_total_mv = df_assets["market_value_eur"].sum()
print(f"Assets generated: {len(df_assets)}")
print(f"Total market value: EUR {actual_total_mv/1e9:.2f}B (target {TOTAL_ASSETS_M/1e3:.1f}B)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Policies (~20 000 * scale_factor rows)

# COMMAND ----------

N_POLICIES = int(20000 * scale_factor)
TOTAL_GWP = TOTAL_GWP_M * 1e6  # 2B EUR

policies_rows = []
underwriting_years = list(range(2020, reporting_year + 1))
channels = ["broker", "direct", "agent"]
channel_weights = [0.50, 0.30, 0.20]
premium_freq = ["annual", "semi-annual", "quarterly", "monthly"]
freq_weights = [0.50, 0.20, 0.20, 0.10]
countries_risk = SOVEREIGN_COUNTRIES + ["LU", "IE", "FI", "SE", "DK", "PT"]
country_risk_weights = np.array([0.25, 0.18, 0.12, 0.10, 0.08, 0.06, 0.04,
                                  0.04, 0.04, 0.03, 0.03, 0.02, 0.01])
country_risk_weights /= country_risk_weights.sum()

# Pre-compute per-LoB targets
lob_targets = {lob["code"]: TOTAL_GWP * lob["gwp_share"] for lob in LOB_CONFIG}
lob_counts = {}
for lob in LOB_CONFIG:
    code = lob["code"]
    share = lob["gwp_share"]
    lob_counts[code] = max(1, int(N_POLICIES * share))

# Adjust last LoB to hit total
total_assigned = sum(lob_counts.values())
diff = N_POLICIES - total_assigned
lob_counts[LOB_CODES[0]] += diff

for lob_code in LOB_CODES:
    n_lob = lob_counts[lob_code]
    target_gwp_lob = lob_targets[lob_code]
    # Log-normal premium distribution
    raw_premiums = rng.lognormal(mean=np.log(target_gwp_lob / n_lob), sigma=0.8, size=n_lob)
    # Scale to match target exactly
    raw_premiums = raw_premiums / raw_premiums.sum() * target_gwp_lob

    for i in range(n_lob):
        uw_year = rng.choice(underwriting_years, p=np.array(
            [0.05, 0.08, 0.12, 0.15, 0.25, 0.35]))
        inception = date(uw_year, rng.randint(1, 13), rng.randint(1, 29))
        expiry = inception + timedelta(days=365)
        is_active = expiry >= rpt_date
        if not is_active:
            status = rng.choice(["lapsed", "cancelled"], p=[0.85, 0.15])
        else:
            status = "active"
        annual_premium = to_eur(raw_premiums[i])
        sum_insured = to_eur(annual_premium * rng.uniform(5, 50))

        policies_rows.append({
            "policy_id": f"POL{len(policies_rows)+1:07d}",
            "line_of_business": LOB_NAMES[lob_code],
            "lob_code": int(lob_code),
            "inception_date": inception,
            "expiry_date": expiry,
            "currency": "EUR",
            "country_risk": rng.choice(countries_risk, p=country_risk_weights),
            "status": status,
            "sum_insured_eur": sum_insured,
            "annual_premium_eur": annual_premium,
            "premium_frequency": rng.choice(premium_freq, p=freq_weights),
            "channel": rng.choice(channels, p=channel_weights),
            "underwriting_year": int(uw_year),
            "reporting_period": reporting_period,
        })

df_policies = pd.DataFrame(policies_rows)
actual_gwp = df_policies["annual_premium_eur"].sum()
print(f"Policies generated: {len(df_policies)}")
print(f"Total annual premiums: EUR {actual_gwp/1e9:.2f}B (target {TOTAL_GWP_M/1e3:.1f}B)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Premium Transactions (~60 000 * scale_factor rows)

# COMMAND ----------

N_PREMIUM_TXN = int(60000 * scale_factor)

# 8 quarters: 2024-Q1 through 2025-Q4
quarters = []
for y in [reporting_year - 1, reporting_year]:
    for q in [1, 2, 3, 4]:
        q_start = date(y, (q - 1) * 3 + 1, 1)
        q_end_month = q * 3
        q_end = date(y, q_end_month, 28)  # simplified
        quarters.append((f"{y}-Q{q}", q_start, q_end, y))

# Distribute transactions across quarters roughly evenly
txn_per_quarter = N_PREMIUM_TXN // (len(quarters) * 2)  # 2 types (written, earned) main

premium_txns = []
txn_idx = 0

for q_label, q_start, q_end, acc_year in quarters:
    # Sample policies active in this quarter
    active_mask = ((df_policies["inception_date"] <= q_end) &
                   (df_policies["expiry_date"] >= q_start))
    active_policies = df_policies[active_mask]
    if len(active_policies) == 0:
        continue

    # Sample a subset for this quarter's transactions
    n_txns = min(txn_per_quarter, len(active_policies))
    sampled = active_policies.sample(n=n_txns, random_state=rng.randint(1e9))

    for _, pol in sampled.iterrows():
        quarterly_premium = pol["annual_premium_eur"] / 4.0
        cession = LOB_CESSION[pol["lob_code"]]
        txn_date = random_date(q_start, q_end)[0]

        # Written premium
        gross_w = to_eur(quarterly_premium * rng.uniform(0.95, 1.05))
        ri_w = to_eur(gross_w * cession)
        premium_txns.append({
            "transaction_id": f"PT{txn_idx+1:07d}",
            "policy_id": pol["policy_id"],
            "transaction_date": txn_date,
            "transaction_type": "written",
            "gross_amount_eur": gross_w,
            "reinsurance_amount_eur": ri_w,
            "net_amount_eur": to_eur(gross_w - ri_w),
            "reporting_period": q_label,
            "accounting_year": int(acc_year),
            "underwriting_year": int(pol["underwriting_year"]),
        })
        txn_idx += 1

        # Earned premium (pro-rata fraction exposed in quarter)
        policy_days = max((pol["expiry_date"] - pol["inception_date"]).days, 1)
        overlap_start = max(pol["inception_date"], q_start)
        overlap_end = min(pol["expiry_date"], q_end)
        exposed_days = max((overlap_end - overlap_start).days, 0)
        earn_fraction = exposed_days / policy_days
        gross_e = to_eur(pol["annual_premium_eur"] * earn_fraction)
        ri_e = to_eur(gross_e * cession)
        premium_txns.append({
            "transaction_id": f"PT{txn_idx+1:07d}",
            "policy_id": pol["policy_id"],
            "transaction_date": txn_date,
            "transaction_type": "earned",
            "gross_amount_eur": gross_e,
            "reinsurance_amount_eur": ri_e,
            "net_amount_eur": to_eur(gross_e - ri_e),
            "reporting_period": q_label,
            "accounting_year": int(acc_year),
            "underwriting_year": int(pol["underwriting_year"]),
        })
        txn_idx += 1

        # Ceded written / ceded earned (mirror entries)
        premium_txns.append({
            "transaction_id": f"PT{txn_idx+1:07d}",
            "policy_id": pol["policy_id"],
            "transaction_date": txn_date,
            "transaction_type": "ceded_written",
            "gross_amount_eur": ri_w,
            "reinsurance_amount_eur": ri_w,
            "net_amount_eur": 0.0,
            "reporting_period": q_label,
            "accounting_year": int(acc_year),
            "underwriting_year": int(pol["underwriting_year"]),
        })
        txn_idx += 1

        premium_txns.append({
            "transaction_id": f"PT{txn_idx+1:07d}",
            "policy_id": pol["policy_id"],
            "transaction_date": txn_date,
            "transaction_type": "ceded_earned",
            "gross_amount_eur": ri_e,
            "reinsurance_amount_eur": ri_e,
            "net_amount_eur": 0.0,
            "reporting_period": q_label,
            "accounting_year": int(acc_year),
            "underwriting_year": int(pol["underwriting_year"]),
        })
        txn_idx += 1

df_premiums = pd.DataFrame(premium_txns)
written_total = df_premiums[df_premiums["transaction_type"] == "written"]["gross_amount_eur"].sum()
print(f"Premium transactions generated: {len(df_premiums)}")
print(f"Total gross written (from txns): EUR {written_total/1e6:.0f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Claims Transactions (~40 000 * scale_factor rows)

# COMMAND ----------

N_CLAIMS_TARGET = int(40000 * scale_factor)

# Target claims ratio ~ 66% of GWP (combined = 96%, expenses ~30%)
target_loss_ratio = TARGET_COMBINED_RATIO - 0.30
target_total_claims = TOTAL_GWP * target_loss_ratio  # ~1.32B EUR

# Accident years 2020-2025, development years 0-5
accident_years = list(range(2020, reporting_year + 1))

claims_rows = []
claim_counter = 0

# Group policies by LoB for claim generation
for lob_code in LOB_CODES:
    lob_policies = df_policies[df_policies["lob_code"] == lob_code]
    if len(lob_policies) == 0:
        continue

    lob_gwp_share = GWP_SHARES[lob_code]
    lob_target_claims = target_total_claims * lob_gwp_share
    mu = LOB_SEVERITY_MU[lob_code]
    sigma = LOB_SEVERITY_SIGMA[lob_code]
    dev_speed = LOB_DEV_SPEED[lob_code]
    cession = LOB_CESSION[lob_code]
    causes = CLAIM_CAUSES[lob_code]

    # Generate claims per accident year
    for ay in accident_years:
        ay_policies = lob_policies[lob_policies["underwriting_year"] == ay]
        if len(ay_policies) == 0:
            ay_policies = lob_policies  # fall back to all policies

        # Allocate claims budget by accident year (more recent = more weight)
        ay_weight = 0.05 + 0.15 * (ay - min(accident_years)) / max(1, (max(accident_years) - min(accident_years)))
        ay_claims_budget = lob_target_claims * ay_weight / sum(
            0.05 + 0.15 * (a - min(accident_years)) / max(1, (max(accident_years) - min(accident_years)))
            for a in accident_years
        )

        # Number of claims (Poisson)
        avg_severity = np.exp(mu + sigma**2 / 2)
        n_claims_ay = max(1, int(rng.poisson(ay_claims_budget / avg_severity)))

        # Generate severities (log-normal)
        severities = rng.lognormal(mu, sigma, size=n_claims_ay)
        # Scale to hit budget
        severities = severities / severities.sum() * ay_claims_budget

        max_dev = min(5, reporting_year - ay)

        for c in range(n_claims_ay):
            claim_counter += 1
            claim_id = f"CL{claim_counter:07d}"
            pol = ay_policies.iloc[rng.randint(len(ay_policies))]
            loss_date = random_date(date(ay, 1, 1), date(ay, 12, 31))[0]
            notif_delay = int(rng.exponential(15)) + 1
            notif_date = loss_date + timedelta(days=notif_delay)
            ultimate = severities[c]
            large_flag = ultimate > 500000
            cause = rng.choice(causes)

            # Develop claim across development years
            cum_paid = 0.0
            for dy in range(max_dev + 1):
                dev_frac = dev_speed[dy] if dy < len(dev_speed) else 0.01
                paid_this_dy = ultimate * dev_frac * rng.uniform(0.8, 1.2)
                cum_paid += paid_this_dy
                txn_date_year = ay + dy
                if txn_date_year > reporting_year:
                    break
                txn_date = random_date(
                    date(txn_date_year, 1, 1),
                    min(date(txn_date_year, 12, 31), rpt_date)
                )[0]

                is_final_dev = (dy == max_dev) or (cum_paid >= ultimate * 0.98)
                status = "closed" if is_final_dev and rng.random() < 0.7 else "open"
                if status == "closed" and rng.random() < 0.03:
                    status = "reopened"

                gross_amt = to_eur(paid_this_dy)
                ri_recovery = to_eur(gross_amt * cession * rng.uniform(0.8, 1.0))
                net_amt = to_eur(gross_amt - ri_recovery)

                # Paid transaction
                claims_rows.append({
                    "claim_id": claim_id,
                    "policy_id": pol["policy_id"],
                    "loss_date": loss_date,
                    "notification_date": notif_date,
                    "transaction_date": txn_date,
                    "transaction_type": "paid",
                    "claim_status": status,
                    "gross_amount_eur": gross_amt,
                    "reinsurance_recovery_eur": ri_recovery,
                    "net_amount_eur": net_amt,
                    "large_loss_flag": large_flag,
                    "cause_of_loss": cause,
                    "reporting_period": quarter_label(txn_date),
                    "accident_year": int(ay),
                    "development_year": int(dy),
                })

                # Reserve change (incurred minus paid = reserve movement)
                if dy < max_dev:
                    reserve_gross = to_eur((ultimate - cum_paid) * rng.uniform(0.9, 1.1))
                else:
                    reserve_gross = 0.0
                reserve_ri = to_eur(reserve_gross * cession * rng.uniform(0.8, 1.0))

                claims_rows.append({
                    "claim_id": claim_id,
                    "policy_id": pol["policy_id"],
                    "loss_date": loss_date,
                    "notification_date": notif_date,
                    "transaction_date": txn_date,
                    "transaction_type": "reserve_change",
                    "claim_status": status,
                    "gross_amount_eur": reserve_gross,
                    "reinsurance_recovery_eur": reserve_ri,
                    "net_amount_eur": to_eur(reserve_gross - reserve_ri),
                    "large_loss_flag": large_flag,
                    "cause_of_loss": cause,
                    "reporting_period": quarter_label(txn_date),
                    "accident_year": int(ay),
                    "development_year": int(dy),
                })

            # Also add an incurred transaction at initial notification
            incurred_gross = to_eur(ultimate * rng.uniform(0.9, 1.1))
            incurred_ri = to_eur(incurred_gross * cession * rng.uniform(0.8, 1.0))
            claims_rows.append({
                "claim_id": claim_id,
                "policy_id": pol["policy_id"],
                "loss_date": loss_date,
                "notification_date": notif_date,
                "transaction_date": notif_date,
                "transaction_type": "incurred",
                "claim_status": "open",
                "gross_amount_eur": incurred_gross,
                "reinsurance_recovery_eur": incurred_ri,
                "net_amount_eur": to_eur(incurred_gross - incurred_ri),
                "large_loss_flag": large_flag,
                "cause_of_loss": cause,
                "reporting_period": quarter_label(notif_date),
                "accident_year": int(ay),
                "development_year": 0,
            })

df_claims = pd.DataFrame(claims_rows)
total_paid_gross = df_claims[df_claims["transaction_type"] == "paid"]["gross_amount_eur"].sum()
total_incurred_gross = df_claims[df_claims["transaction_type"] == "incurred"]["gross_amount_eur"].sum()
n_unique_claims = df_claims["claim_id"].nunique()
n_large = df_claims[df_claims["large_loss_flag"]]["claim_id"].nunique()
print(f"Claims transactions generated: {len(df_claims)}")
print(f"Unique claims: {n_unique_claims}")
print(f"Large losses (>500K): {n_large} ({n_large/max(1,n_unique_claims)*100:.1f}%)")
print(f"Total paid gross: EUR {total_paid_gross/1e6:.0f}M")
print(f"Total incurred gross: EUR {total_incurred_gross/1e6:.0f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Claims Triangles (pre-aggregated)

# COMMAND ----------

triangle_rows = []

for lob_code in LOB_CODES:
    lob_claims = df_claims[df_claims["policy_id"].isin(
        df_policies[df_policies["lob_code"] == lob_code]["policy_id"]
    )]
    if len(lob_claims) == 0:
        continue

    for ay in range(2020, reporting_year + 1):
        max_dy = min(5, reporting_year - ay)
        ay_claims = lob_claims[lob_claims["accident_year"] == ay]
        if len(ay_claims) == 0:
            continue

        cum_paid_gross = 0.0
        cum_paid_net = 0.0
        for dy in range(max_dy + 1):
            dy_paid = ay_claims[(ay_claims["development_year"] == dy) &
                                (ay_claims["transaction_type"] == "paid")]
            dy_reserve = ay_claims[(ay_claims["development_year"] == dy) &
                                   (ay_claims["transaction_type"] == "reserve_change")]

            paid_g = dy_paid["gross_amount_eur"].sum()
            paid_ri = dy_paid["reinsurance_recovery_eur"].sum()
            res_g = dy_reserve["gross_amount_eur"].sum()
            res_ri = dy_reserve["reinsurance_recovery_eur"].sum()

            cum_paid_gross += paid_g
            cum_paid_net += (paid_g - paid_ri)

            cum_incurred_gross = cum_paid_gross + res_g
            cum_incurred_net = cum_paid_net + (res_g - res_ri)

            n_open = ay_claims[(ay_claims["development_year"] == dy) &
                               (ay_claims["claim_status"] == "open")]["claim_id"].nunique()
            n_closed = ay_claims[(ay_claims["development_year"] == dy) &
                                 (ay_claims["claim_status"] == "closed")]["claim_id"].nunique()

            triangle_rows.append({
                "lob_code": int(lob_code),
                "line_of_business": LOB_NAMES[lob_code],
                "accident_year": int(ay),
                "development_year": int(dy),
                "cumulative_paid_gross": to_eur(cum_paid_gross),
                "cumulative_paid_net": to_eur(cum_paid_net),
                "cumulative_incurred_gross": to_eur(cum_incurred_gross),
                "cumulative_incurred_net": to_eur(cum_incurred_net),
                "case_reserves_gross": to_eur(res_g),
                "case_reserves_net": to_eur(res_g - res_ri),
                "claim_count_open": int(n_open),
                "claim_count_closed": int(n_closed),
                "reporting_period": reporting_period,
            })

df_triangles = pd.DataFrame(triangle_rows)
print(f"Triangle rows generated: {len(df_triangles)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Expenses (~2000 * scale_factor rows)

# COMMAND ----------

N_EXPENSES = int(2000 * scale_factor)

# Expense ratio targets as fraction of GWP
expense_categories = {
    "acquisition":           0.12,
    "administrative":        0.10,
    "claims_management":     0.05,
    "overhead":              0.03,
    "investment_management": 0.005,
    "other":                 0.005,
}
# Total ~30.5% to be safe
alloc_bases = {
    "acquisition": "premium",
    "administrative": "headcount",
    "claims_management": "claims",
    "overhead": "headcount",
    "investment_management": "direct",
    "other": "direct",
}
cost_centres = ["CC100-Underwriting", "CC200-Claims", "CC300-Finance",
                "CC400-IT", "CC500-HR", "CC600-Legal", "CC700-Actuarial",
                "CC800-Investments", "CC900-General"]

expense_rows = []
exp_idx = 0

for cat, ratio in expense_categories.items():
    cat_total = TOTAL_GWP * ratio
    n_cat = max(1, int(N_EXPENSES * ratio / sum(expense_categories.values())))

    for lob_code in LOB_CODES:
        lob_share = GWP_SHARES[lob_code]
        lob_cat_total = cat_total * lob_share
        n_lob_cat = max(1, int(n_cat * lob_share))

        amounts = rng.lognormal(mean=np.log(lob_cat_total / n_lob_cat), sigma=0.5, size=n_lob_cat)
        amounts = amounts / amounts.sum() * lob_cat_total

        for a in amounts:
            exp_idx += 1
            acc_year = rng.choice([reporting_year - 1, reporting_year], p=[0.35, 0.65])
            expense_rows.append({
                "expense_id": f"EXP{exp_idx:06d}",
                "line_of_business": LOB_NAMES[lob_code],
                "lob_code": int(lob_code),
                "expense_category": cat,
                "gross_amount_eur": to_eur(a),
                "allocation_basis": alloc_bases[cat],
                "cost_centre": rng.choice(cost_centres),
                "reporting_period": reporting_period,
                "accounting_year": int(acc_year),
            })

df_expenses = pd.DataFrame(expense_rows)
total_expenses = df_expenses["gross_amount_eur"].sum()
print(f"Expense records generated: {len(df_expenses)}")
print(f"Total expenses: EUR {total_expenses/1e6:.0f}M ({total_expenses/TOTAL_GWP*100:.1f}% of GWP)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Reinsurance Contracts (~50 rows)

# COMMAND ----------

ri_rows = []
ri_idx = 0

# Quota share per LoB
for lob_code in LOB_CODES:
    ri_idx += 1
    reinsurer = REINSURER_NAMES[rng.randint(len(REINSURER_NAMES))]
    cession = LOB_CESSION[lob_code]
    lob_gwp = TOTAL_GWP * GWP_SHARES[lob_code]
    ri_premium = lob_gwp * cession
    ri_rows.append({
        "contract_id": f"RI{ri_idx:04d}",
        "contract_name": f"QS {LOB_NAMES[lob_code]} {reporting_year}",
        "reinsurer_name": reinsurer,
        "reinsurer_lei": make_lei(f"ri_{reinsurer}"),
        "reinsurer_country": rng.choice(["DE", "CH", "FR", "BM", "IE"]),
        "reinsurer_credit_rating": rng.choice(["AA+", "AA", "AA-", "A+"]),
        "contract_type": "quota_share",
        "lob_codes_covered": str(lob_code),
        "inception_date": date(reporting_year, 1, 1),
        "expiry_date": date(reporting_year, 12, 31),
        "cession_rate": cession,
        "retention_eur": None,
        "limit_eur": None,
        "priority_eur": None,
        "premium_eur": to_eur(ri_premium),
        "commission_rate": round(rng.uniform(0.25, 0.35), 4),
        "reporting_period": reporting_period,
    })

# Excess of loss per LoB
for lob_code in LOB_CODES:
    ri_idx += 1
    reinsurer = REINSURER_NAMES[rng.randint(len(REINSURER_NAMES))]
    lob_gwp = TOTAL_GWP * GWP_SHARES[lob_code]
    priority = rng.choice([500000, 750000, 1000000, 1500000])
    limit_val = priority * rng.choice([3, 5, 10])
    ri_premium = lob_gwp * rng.uniform(0.02, 0.06)
    ri_rows.append({
        "contract_id": f"RI{ri_idx:04d}",
        "contract_name": f"XL {LOB_NAMES[lob_code]} {reporting_year}",
        "reinsurer_name": reinsurer,
        "reinsurer_lei": make_lei(f"ri_{reinsurer}"),
        "reinsurer_country": rng.choice(["DE", "CH", "FR", "BM", "IE"]),
        "reinsurer_credit_rating": rng.choice(["AA+", "AA", "AA-", "A+"]),
        "contract_type": "excess_of_loss",
        "lob_codes_covered": str(lob_code),
        "inception_date": date(reporting_year, 1, 1),
        "expiry_date": date(reporting_year, 12, 31),
        "cession_rate": None,
        "retention_eur": float(priority),
        "limit_eur": float(limit_val),
        "priority_eur": float(priority),
        "premium_eur": to_eur(ri_premium),
        "commission_rate": round(rng.uniform(0.10, 0.20), 4),
        "reporting_period": reporting_period,
    })

# Surplus treaties for property and motor
for lob_code in [4, 7]:
    ri_idx += 1
    reinsurer = REINSURER_NAMES[rng.randint(len(REINSURER_NAMES))]
    lob_gwp = TOTAL_GWP * GWP_SHARES[lob_code]
    ri_premium = lob_gwp * rng.uniform(0.03, 0.08)
    ri_rows.append({
        "contract_id": f"RI{ri_idx:04d}",
        "contract_name": f"Surplus {LOB_NAMES[lob_code]} {reporting_year}",
        "reinsurer_name": reinsurer,
        "reinsurer_lei": make_lei(f"ri_{reinsurer}"),
        "reinsurer_country": rng.choice(["DE", "CH", "FR", "BM"]),
        "reinsurer_credit_rating": rng.choice(["AA+", "AA"]),
        "contract_type": "surplus",
        "lob_codes_covered": str(lob_code),
        "inception_date": date(reporting_year, 1, 1),
        "expiry_date": date(reporting_year, 12, 31),
        "cession_rate": round(rng.uniform(0.15, 0.25), 4),
        "retention_eur": to_eur(rng.uniform(2e6, 5e6)),
        "limit_eur": to_eur(rng.uniform(20e6, 50e6)),
        "priority_eur": None,
        "premium_eur": to_eur(ri_premium),
        "commission_rate": round(rng.uniform(0.20, 0.30), 4),
        "reporting_period": reporting_period,
    })

# Stop loss (whole account)
for _ in range(2):
    ri_idx += 1
    reinsurer = REINSURER_NAMES[rng.randint(len(REINSURER_NAMES))]
    ri_rows.append({
        "contract_id": f"RI{ri_idx:04d}",
        "contract_name": f"Stop Loss {reporting_year}/{ri_idx}",
        "reinsurer_name": reinsurer,
        "reinsurer_lei": make_lei(f"ri_{reinsurer}"),
        "reinsurer_country": rng.choice(["CH", "BM", "IE"]),
        "reinsurer_credit_rating": rng.choice(["AA+", "AA", "A+"]),
        "contract_type": "stop_loss",
        "lob_codes_covered": ",".join(str(c) for c in LOB_CODES),
        "inception_date": date(reporting_year, 1, 1),
        "expiry_date": date(reporting_year, 12, 31),
        "cession_rate": None,
        "retention_eur": to_eur(TOTAL_GWP * rng.uniform(0.85, 0.95)),
        "limit_eur": to_eur(TOTAL_GWP * rng.uniform(1.10, 1.30)),
        "priority_eur": to_eur(TOTAL_GWP * rng.uniform(0.85, 0.95)),
        "premium_eur": to_eur(TOTAL_GWP * rng.uniform(0.01, 0.03)),
        "commission_rate": round(rng.uniform(0.05, 0.15), 4),
        "reporting_period": reporting_period,
    })

# Pad to ~50 with additional layered XL contracts
while ri_idx < 50:
    ri_idx += 1
    reinsurer = REINSURER_NAMES[rng.randint(len(REINSURER_NAMES))]
    lob_code = rng.choice(LOB_CODES)
    layer_priority = rng.choice([2e6, 5e6, 10e6, 15e6, 20e6])
    layer_limit = layer_priority * rng.choice([2, 3, 5])
    ri_rows.append({
        "contract_id": f"RI{ri_idx:04d}",
        "contract_name": f"XL Layer {ri_idx - 18} LoB {lob_code} {reporting_year}",
        "reinsurer_name": reinsurer,
        "reinsurer_lei": make_lei(f"ri_{reinsurer}"),
        "reinsurer_country": rng.choice(["DE", "CH", "FR", "BM", "IE", "SE"]),
        "reinsurer_credit_rating": rng.choice(["AA+", "AA", "AA-", "A+", "A"]),
        "contract_type": "excess_of_loss",
        "lob_codes_covered": str(lob_code),
        "inception_date": date(reporting_year, 1, 1),
        "expiry_date": date(reporting_year, 12, 31),
        "cession_rate": None,
        "retention_eur": float(layer_priority),
        "limit_eur": float(layer_limit),
        "priority_eur": float(layer_priority),
        "premium_eur": to_eur(layer_limit * rng.uniform(0.01, 0.04)),
        "commission_rate": round(rng.uniform(0.10, 0.20), 4),
        "reporting_period": reporting_period,
    })

df_reinsurance = pd.DataFrame(ri_rows)
print(f"Reinsurance contracts generated: {len(df_reinsurance)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Technical Provisions (~50 rows)

# COMMAND ----------

tp_rows = []

# Derive from claims data
total_outstanding_reserves = df_claims[
    df_claims["transaction_type"] == "reserve_change"
]["gross_amount_eur"].sum()

for lob_code in LOB_CODES:
    lob_name = LOB_NAMES[lob_code]
    lob_gwp = TOTAL_GWP * GWP_SHARES[lob_code]
    cession = LOB_CESSION[lob_code]

    # Best estimate claims reserve: based on outstanding reserves + IBNR margin
    lob_reserves = df_claims[
        (df_claims["policy_id"].isin(
            df_policies[df_policies["lob_code"] == lob_code]["policy_id"]
        )) & (df_claims["transaction_type"] == "reserve_change")
    ]["gross_amount_eur"].sum()

    ibnr_factor = rng.uniform(1.05, 1.30)  # IBNR is 5-30% on top
    be_claims_gross = lob_reserves * ibnr_factor
    be_claims_ri = be_claims_gross * cession * rng.uniform(0.85, 0.95)
    be_claims_net = be_claims_gross - be_claims_ri

    # Best estimate premium reserve: unearned premium minus expected costs
    be_prem_gross = lob_gwp * rng.uniform(-0.05, 0.10)  # can be negative (profitable)
    be_prem_ri = be_prem_gross * cession * rng.uniform(0.80, 0.95) if be_prem_gross > 0 else 0.0
    be_prem_net = be_prem_gross - be_prem_ri

    # Risk margin (~8-12% of BE claims)
    rm_gross = be_claims_gross * rng.uniform(0.08, 0.12)
    rm_ri = 0.0
    rm_net = rm_gross

    for ptype, gross, ri, net in [
        ("best_estimate_claims", be_claims_gross, be_claims_ri, be_claims_net),
        ("best_estimate_premium", be_prem_gross, be_prem_ri, be_prem_net),
        ("risk_margin", rm_gross, rm_ri, rm_net),
    ]:
        tp_rows.append({
            "lob_code": int(lob_code),
            "line_of_business": lob_name,
            "provision_type": ptype,
            "gross_amount_eur": to_eur(gross),
            "reinsurance_recoverable_eur": to_eur(ri),
            "net_amount_eur": to_eur(net),
            "reporting_period": reporting_period,
        })

# Add transitional measures (small, single row)
tp_rows.append({
    "lob_code": 0,
    "line_of_business": "All lines",
    "provision_type": "transitional",
    "gross_amount_eur": to_eur(rng.uniform(-20e6, -5e6)),
    "reinsurance_recoverable_eur": 0.0,
    "net_amount_eur": to_eur(rng.uniform(-20e6, -5e6)),
    "reporting_period": reporting_period,
})

df_tp = pd.DataFrame(tp_rows)
total_tp_gross = df_tp["gross_amount_eur"].sum()
total_tp_ri = df_tp["reinsurance_recoverable_eur"].sum()
print(f"Technical provision rows generated: {len(df_tp)}")
print(f"Total TP gross: EUR {total_tp_gross/1e6:.0f}M")
print(f"Total TP net:   EUR {(total_tp_gross - total_tp_ri)/1e6:.0f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Own Funds Components (~20 rows)

# COMMAND ----------

# Target own funds ~EUR 2B (solvency ratio ~170% of SCR 1.15B)
of_rows = []

# Tier 1 unrestricted (largest component)
t1u_total = TARGET_OWN_FUNDS_M * 1e6 * 0.80
of_components = [
    ("OF001", "Ordinary share capital",             "tier1_unrestricted", t1u_total * 0.25, "basic"),
    ("OF002", "Share premium account",              "tier1_unrestricted", t1u_total * 0.10, "basic"),
    ("OF003", "Surplus funds - Article 91(2)",      "tier1_unrestricted", t1u_total * 0.05, "basic"),
    ("OF004", "Reconciliation reserve",             "tier1_unrestricted", t1u_total * 0.50, "basic"),
    ("OF005", "Retained earnings",                  "tier1_unrestricted", t1u_total * 0.10, "basic"),
    ("OF006", "Preference shares (T1 restricted)",  "tier1_restricted",   TARGET_OWN_FUNDS_M * 1e6 * 0.05, "basic"),
    ("OF007", "Subordinated liabilities (T1)",      "tier1_restricted",   TARGET_OWN_FUNDS_M * 1e6 * 0.03, "basic"),
    ("OF008", "Subordinated liabilities (Tier 2)",  "tier2",              TARGET_OWN_FUNDS_M * 1e6 * 0.07, "basic"),
    ("OF009", "Deferred tax assets",                "tier3",              TARGET_OWN_FUNDS_M * 1e6 * 0.02, "basic"),
    ("OF010", "Ancillary own funds - letters of credit", "tier2",         TARGET_OWN_FUNDS_M * 1e6 * 0.02, "ancillary"),
    ("OF011", "Ancillary own funds - callable capital",  "tier2",         TARGET_OWN_FUNDS_M * 1e6 * 0.01, "ancillary"),
]

# Add small adjustments to hit target with some noise
noise_used = 0.0
for cid, cname, tier, amount, classif in of_components:
    noise = amount * rng.uniform(-0.02, 0.02)
    noise_used += noise
    of_rows.append({
        "component_id": cid,
        "component_name": cname,
        "tier": tier,
        "amount_eur": to_eur(amount + noise),
        "classification": classif,
        "reporting_period": reporting_period,
    })

# Deductions
of_rows.append({
    "component_id": "OF012",
    "component_name": "Deductions - participations in financial institutions",
    "tier": "tier1_unrestricted",
    "amount_eur": to_eur(-TARGET_OWN_FUNDS_M * 1e6 * 0.005),
    "classification": "basic",
    "reporting_period": reporting_period,
})

# Foreseeable dividends
of_rows.append({
    "component_id": "OF013",
    "component_name": "Foreseeable dividends and distributions",
    "tier": "tier1_unrestricted",
    "amount_eur": to_eur(-TARGET_OWN_FUNDS_M * 1e6 * 0.015),
    "classification": "basic",
    "reporting_period": reporting_period,
})

# Minority interests
of_rows.append({
    "component_id": "OF014",
    "component_name": "Minority interests at group level",
    "tier": "tier1_unrestricted",
    "amount_eur": to_eur(TARGET_OWN_FUNDS_M * 1e6 * 0.008),
    "classification": "basic",
    "reporting_period": reporting_period,
})

# Adjustments for ring-fenced
for adj_name, adj_frac in [
    ("Expected profits in future premiums (EPIFP) - non-life", 0.012),
    ("Expected profits in future premiums (EPIFP) - life", 0.002),
    ("Other own fund items approved by supervisor", 0.005),
]:
    of_rows.append({
        "component_id": f"OF{len(of_rows)+1:03d}",
        "component_name": adj_name,
        "tier": "tier1_unrestricted",
        "amount_eur": to_eur(TARGET_OWN_FUNDS_M * 1e6 * adj_frac),
        "classification": "basic",
        "reporting_period": reporting_period,
    })

df_own_funds = pd.DataFrame(of_rows)
total_of = df_own_funds["amount_eur"].sum()
print(f"Own funds components generated: {len(df_own_funds)}")
print(f"Total own funds: EUR {total_of/1e6:.0f}M (target {TARGET_OWN_FUNDS_M:.0f}M)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Risk Factors / SCR Sub-modules (~25 rows)

# COMMAND ----------

# SCR Standard Formula structure
# BSCR target ~1350M, final SCR ~1150M after op risk and LAC

# Market risk sub-modules (total ~600M)
market_total_target = TARGET_BSCR_M * 1e6 * 0.40  # ~540M
market_subs = {
    "interest_rate":  0.15,
    "equity":         0.25,
    "property":       0.12,
    "spread":         0.30,
    "currency":       0.10,
    "concentration":  0.08,
}

# Counterparty default (total ~180M)
default_total_target = TARGET_BSCR_M * 1e6 * 0.12
default_subs = {"type1": 0.65, "type2": 0.35}

# Non-life risk (total ~650M)
nl_total_target = TARGET_BSCR_M * 1e6 * 0.45
nl_subs = {"premium_reserve": 0.55, "catastrophe": 0.30, "lapse": 0.15}

# Health risk (total ~120M)
health_total_target = TARGET_BSCR_M * 1e6 * 0.08
health_subs = {"similar_life": 0.25, "similar_nl": 0.55, "catastrophe": 0.20}

rf_rows = []

def _add_module(module_name, total_target, sub_dict):
    charges = {}
    for sub, share in sub_dict.items():
        gross = total_target * share * rng.uniform(0.92, 1.08)
        charges[sub] = gross

    # Diversification within module (~15-25% reduction)
    undiversified = sum(charges.values())
    diversified = undiversified * rng.uniform(0.75, 0.85)
    div_benefit = undiversified - diversified

    for sub, gross in charges.items():
        sub_div = div_benefit * (gross / undiversified)
        net = gross - sub_div
        rf_rows.append({
            "risk_module": module_name,
            "risk_sub_module": sub,
            "gross_capital_charge_eur": to_eur(gross),
            "diversification_within_module_eur": to_eur(-sub_div),
            "net_capital_charge_eur": to_eur(net),
            "reporting_period": reporting_period,
        })

_add_module("market", market_total_target, market_subs)
_add_module("counterparty_default", default_total_target, default_subs)
_add_module("non_life", nl_total_target, nl_subs)
_add_module("health", health_total_target, health_subs)

# Operational risk
op_risk = TARGET_SCR_M * 1e6 * rng.uniform(0.06, 0.10)
rf_rows.append({
    "risk_module": "operational",
    "risk_sub_module": "operational",
    "gross_capital_charge_eur": to_eur(op_risk),
    "diversification_within_module_eur": 0.0,
    "net_capital_charge_eur": to_eur(op_risk),
    "reporting_period": reporting_period,
})

# Intangible asset risk (typically small or zero for P&C)
intangible = rng.uniform(0, 5e6)
rf_rows.append({
    "risk_module": "intangible",
    "risk_sub_module": "intangible",
    "gross_capital_charge_eur": to_eur(intangible),
    "diversification_within_module_eur": 0.0,
    "net_capital_charge_eur": to_eur(intangible),
    "reporting_period": reporting_period,
})

df_risk_factors = pd.DataFrame(rf_rows)
total_net_charges = df_risk_factors["net_capital_charge_eur"].sum()
print(f"Risk factor rows generated: {len(df_risk_factors)}")
print(f"Sum of net capital charges: EUR {total_net_charges/1e6:.0f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. SCR Parameters (~40 rows)

# COMMAND ----------

# Inter-module correlations (from config)
corr_matrix = {
    ("market", "market"): 1.00,
    ("market", "default"): 0.25,
    ("market", "life"): 0.25,
    ("market", "health"): 0.25,
    ("market", "non_life"): 0.25,
    ("default", "default"): 1.00,
    ("default", "life"): 0.25,
    ("default", "health"): 0.25,
    ("default", "non_life"): 0.50,
    ("life", "life"): 1.00,
    ("life", "health"): 0.25,
    ("life", "non_life"): 0.00,
    ("health", "health"): 1.00,
    ("health", "non_life"): 0.00,
    ("non_life", "non_life"): 1.00,
}

scr_params = []
for (m1, m2), val in corr_matrix.items():
    scr_params.append({
        "parameter_name": f"corr_{m1}_{m2}",
        "parameter_value": str(val),
        "parameter_category": "correlation",
        "description": f"BSCR correlation between {m1} and {m2} risk modules",
        "reporting_period": reporting_period,
    })

# LAC adjustments
lac_tp = total_tp_gross * rng.uniform(0.04, 0.08)
lac_dt = TARGET_OWN_FUNDS_M * 1e6 * rng.uniform(0.03, 0.06)
scr_params.extend([
    {"parameter_name": "lac_tp_ratio", "parameter_value": str(round(lac_tp / TARGET_BSCR_M / 1e6, 4)),
     "parameter_category": "adjustment", "description": "Loss-absorbing capacity of technical provisions as ratio of BSCR",
     "reporting_period": reporting_period},
    {"parameter_name": "lac_tp_amount", "parameter_value": str(to_eur(lac_tp)),
     "parameter_category": "adjustment", "description": "Loss-absorbing capacity of technical provisions (EUR)",
     "reporting_period": reporting_period},
    {"parameter_name": "lac_dt_amount", "parameter_value": str(to_eur(lac_dt)),
     "parameter_category": "adjustment", "description": "Loss-absorbing capacity of deferred taxes (EUR)",
     "reporting_period": reporting_period},
])

# Operational risk factors
scr_params.extend([
    {"parameter_name": "op_risk_premium_factor", "parameter_value": "0.04",
     "parameter_category": "threshold", "description": "Operational risk charge as % of earned premiums",
     "reporting_period": reporting_period},
    {"parameter_name": "op_risk_tp_factor", "parameter_value": "0.0045",
     "parameter_category": "threshold", "description": "Operational risk charge as % of technical provisions",
     "reporting_period": reporting_period},
    {"parameter_name": "op_risk_cap", "parameter_value": "0.30",
     "parameter_category": "threshold", "description": "Operational risk cap as % of BSCR",
     "reporting_period": reporting_period},
])

# NL premium & reserve risk sigma parameters by LoB
nl_sigma = {1: 0.05, 2: 0.08, 4: 0.10, 5: 0.08, 7: 0.10, 8: 0.14, 12: 0.13}
nl_reserve_sigma = {1: 0.10, 2: 0.12, 4: 0.11, 5: 0.10, 7: 0.12, 8: 0.15, 12: 0.20}
for lob_code in LOB_CODES:
    scr_params.append({
        "parameter_name": f"nl_prem_sigma_lob{lob_code}",
        "parameter_value": str(nl_sigma[lob_code]),
        "parameter_category": "threshold",
        "description": f"NL premium risk sigma for LoB {lob_code}",
        "reporting_period": reporting_period,
    })
    scr_params.append({
        "parameter_name": f"nl_reserve_sigma_lob{lob_code}",
        "parameter_value": str(nl_reserve_sigma[lob_code]),
        "parameter_category": "threshold",
        "description": f"NL reserve risk sigma for LoB {lob_code}",
        "reporting_period": reporting_period,
    })

# Market risk sub-module parameters
scr_params.extend([
    {"parameter_name": "equity_type1_shock", "parameter_value": "0.39",
     "parameter_category": "threshold", "description": "Equity shock for type 1 equities (EEA listed)",
     "reporting_period": reporting_period},
    {"parameter_name": "equity_type2_shock", "parameter_value": "0.49",
     "parameter_category": "threshold", "description": "Equity shock for type 2 equities (other)",
     "reporting_period": reporting_period},
    {"parameter_name": "property_shock", "parameter_value": "0.25",
     "parameter_category": "threshold", "description": "Property risk shock factor",
     "reporting_period": reporting_period},
    {"parameter_name": "currency_shock", "parameter_value": "0.25",
     "parameter_category": "threshold", "description": "Currency risk shock factor",
     "reporting_period": reporting_period},
    {"parameter_name": "symmetric_equity_adjustment", "parameter_value": str(round(rng.uniform(-0.10, 0.10), 4)),
     "parameter_category": "adjustment", "description": "Symmetric adjustment to equity shock",
     "reporting_period": reporting_period},
])

df_scr_params = pd.DataFrame(scr_params)
print(f"SCR parameter rows generated: {len(df_scr_params)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Balance Sheet Items (~60 rows)

# COMMAND ----------

# ── Assets side ──────────────────────────────────────────────────────
total_investments = actual_total_mv
ri_recoverables = df_tp["reinsurance_recoverable_eur"].sum()
receivables_insurance = TOTAL_GWP * rng.uniform(0.08, 0.12)
receivables_ri = ri_recoverables * rng.uniform(0.05, 0.10)
cash_and_equiv = df_assets[df_assets["asset_class"] == "cash"]["market_value_eur"].sum()
deferred_acq_costs = TOTAL_GWP * rng.uniform(0.03, 0.06)
intangible_assets = rng.uniform(10e6, 30e6)
deferred_tax_asset = TARGET_OWN_FUNDS_M * 1e6 * 0.02 * rng.uniform(0.8, 1.2)
property_ppe = df_assets[df_assets["asset_class"] == "property"]["market_value_eur"].sum()
other_assets = rng.uniform(20e6, 60e6)

total_assets_bs = (total_investments + ri_recoverables + receivables_insurance +
                   receivables_ri + deferred_acq_costs + intangible_assets +
                   deferred_tax_asset + other_assets)

# ── Liabilities side ────────────────────────────────────────────────
tp_gross_total = total_tp_gross
other_liabilities_insurance = TOTAL_GWP * rng.uniform(0.02, 0.05)
payables_insurance = TOTAL_GWP * rng.uniform(0.03, 0.06)
payables_ri = TOTAL_GWP * rng.uniform(0.02, 0.04)
deferred_tax_liab = rng.uniform(30e6, 80e6)
provisions_other = rng.uniform(20e6, 50e6)
subordinated_liab = (df_own_funds[df_own_funds["tier"].isin(["tier2", "tier3"])]["amount_eur"].sum()
                     * rng.uniform(0.8, 1.0))
other_liab = rng.uniform(10e6, 40e6)

total_liabilities_bs = (tp_gross_total + other_liabilities_insurance + payables_insurance +
                        payables_ri + deferred_tax_liab + provisions_other +
                        subordinated_liab + other_liab)

# Excess of assets over liabilities = own funds
excess = total_assets_bs - total_liabilities_bs

bs_items = []
row_counter = 0

def _add_bs(item_name, row_id, category, amount):
    bs_items.append({
        "item_id": f"BS{len(bs_items)+1:03d}",
        "item_name": item_name,
        "s0201_row_id": row_id,
        "category": category,
        "amount_eur": to_eur(amount),
        "reporting_period": reporting_period,
    })

# Assets
_add_bs("Goodwill", "R0010", "assets", 0.0)
_add_bs("Deferred acquisition costs", "R0020", "assets", deferred_acq_costs)
_add_bs("Intangible assets", "R0030", "assets", intangible_assets)
_add_bs("Deferred tax assets", "R0040", "assets", deferred_tax_asset)
_add_bs("Pension benefit surplus", "R0050", "assets", 0.0)
_add_bs("Property, plant and equipment for own use", "R0060", "assets", property_ppe)
_add_bs("Investments (other than index-linked/unit-linked)", "R0070", "assets", total_investments - cash_and_equiv - property_ppe)
_add_bs("  Property (other than for own use)", "R0080", "assets",
        df_assets[(df_assets["asset_class"] == "property") & (df_assets["cic_code"].str.contains("92"))]["market_value_eur"].sum())
_add_bs("  Holdings in related undertakings", "R0090", "assets", 0.0)
_add_bs("  Equities - listed", "R0100", "assets",
        df_assets[df_assets["asset_class"] == "equity"]["market_value_eur"].sum())
_add_bs("  Equities - unlisted", "R0110", "assets", 0.0)
_add_bs("  Government bonds", "R0120", "assets",
        df_assets[df_assets["asset_class"] == "government_bonds"]["market_value_eur"].sum())
_add_bs("  Corporate bonds", "R0130", "assets",
        df_assets[df_assets["asset_class"] == "corporate_bonds"]["market_value_eur"].sum())
_add_bs("  Structured notes", "R0140", "assets", 0.0)
_add_bs("  Collateralised securities", "R0150", "assets", 0.0)
_add_bs("  Collective investments undertakings", "R0160", "assets",
        df_assets[df_assets["asset_class"] == "ciu"]["market_value_eur"].sum())
_add_bs("  Derivatives", "R0170", "assets", 0.0)
_add_bs("  Deposits other than cash equivalents", "R0180", "assets",
        df_assets[(df_assets["asset_class"] == "cash") & (df_assets["cic_code"].str.contains("72"))]["market_value_eur"].sum())
_add_bs("  Other investments", "R0190", "assets", 0.0)
_add_bs("Assets held for index-linked and unit-linked", "R0200", "assets", 0.0)
_add_bs("Loans and mortgages", "R0210", "assets", 0.0)
_add_bs("Reinsurance recoverables", "R0270", "assets", ri_recoverables)
_add_bs("  RI recoverables - non-life (excl health)", "R0280", "assets", ri_recoverables * 0.75)
_add_bs("  RI recoverables - non-life health", "R0290", "assets", ri_recoverables * 0.15)
_add_bs("  RI recoverables - life", "R0300", "assets", ri_recoverables * 0.10)
_add_bs("Deposits to cedants", "R0350", "assets", 0.0)
_add_bs("Insurance and intermediaries receivables", "R0360", "assets", receivables_insurance)
_add_bs("Reinsurance receivables", "R0370", "assets", receivables_ri)
_add_bs("Receivables (trade, not insurance)", "R0380", "assets", rng.uniform(5e6, 15e6))
_add_bs("Own shares", "R0390", "assets", 0.0)
_add_bs("Amounts due from own fund items", "R0400", "assets", 0.0)
_add_bs("Cash and cash equivalents", "R0410", "assets",
        df_assets[(df_assets["asset_class"] == "cash") & (df_assets["cic_code"].str.contains("71"))]["market_value_eur"].sum())
_add_bs("Any other assets, not elsewhere shown", "R0420", "assets", other_assets)
_add_bs("TOTAL ASSETS", "R0500", "assets", total_assets_bs)

# Liabilities
_add_bs("Technical provisions - non-life", "R0510", "liabilities", tp_gross_total * 0.85)
_add_bs("  TP non-life (excl health) - best estimate", "R0520", "liabilities",
        df_tp[(df_tp["lob_code"].isin([4,5,7,8,12])) & (df_tp["provision_type"].str.contains("best_estimate"))]["gross_amount_eur"].sum())
_add_bs("  TP non-life (excl health) - risk margin", "R0530", "liabilities",
        df_tp[(df_tp["lob_code"].isin([4,5,7,8,12])) & (df_tp["provision_type"] == "risk_margin")]["gross_amount_eur"].sum())
_add_bs("  TP non-life health - best estimate", "R0540", "liabilities",
        df_tp[(df_tp["lob_code"].isin([1,2])) & (df_tp["provision_type"].str.contains("best_estimate"))]["gross_amount_eur"].sum())
_add_bs("  TP non-life health - risk margin", "R0550", "liabilities",
        df_tp[(df_tp["lob_code"].isin([1,2])) & (df_tp["provision_type"] == "risk_margin")]["gross_amount_eur"].sum())
_add_bs("Technical provisions - life", "R0600", "liabilities", tp_gross_total * 0.15)
_add_bs("Other technical provisions", "R0700", "liabilities", 0.0)
_add_bs("Contingent liabilities", "R0740", "liabilities", 0.0)
_add_bs("Provisions other than technical provisions", "R0750", "liabilities", provisions_other)
_add_bs("Pension benefit obligations", "R0760", "liabilities", rng.uniform(5e6, 15e6))
_add_bs("Deposits from reinsurers", "R0770", "liabilities", 0.0)
_add_bs("Deferred tax liabilities", "R0780", "liabilities", deferred_tax_liab)
_add_bs("Derivatives", "R0790", "liabilities", 0.0)
_add_bs("Debts owed to credit institutions", "R0800", "liabilities", 0.0)
_add_bs("Financial liabilities other than debts owed to credit institutions", "R0810", "liabilities", 0.0)
_add_bs("Insurance and intermediaries payables", "R0820", "liabilities", payables_insurance)
_add_bs("Reinsurance payables", "R0830", "liabilities", payables_ri)
_add_bs("Payables (trade, not insurance)", "R0840", "liabilities", rng.uniform(10e6, 25e6))
_add_bs("Subordinated liabilities", "R0850", "liabilities", subordinated_liab)
_add_bs("  Subordinated liabilities not in BOF", "R0860", "liabilities", 0.0)
_add_bs("  Subordinated liabilities in BOF", "R0870", "liabilities", subordinated_liab)
_add_bs("Any other liabilities, not elsewhere shown", "R0880", "liabilities", other_liab)
_add_bs("TOTAL LIABILITIES", "R0900", "liabilities", total_liabilities_bs)

# Own funds / excess
_add_bs("Excess of assets over liabilities", "R1000", "own_funds", excess)

df_balance_sheet = pd.DataFrame(bs_items)
print(f"Balance sheet rows generated: {len(df_balance_sheet)}")
print(f"Total assets:      EUR {total_assets_bs/1e9:.2f}B")
print(f"Total liabilities: EUR {total_liabilities_bs/1e9:.2f}B")
print(f"Excess (own funds): EUR {excess/1e6:.0f}M")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write all tables to Unity Catalog (Delta)

# COMMAND ----------

def write_delta(df_pandas, table_name, description=""):
    """Write a pandas DataFrame as a Delta table and log lineage."""
    sdf = spark.createDataFrame(df_pandas)
    full_name = f"{catalog}.{schema}.{table_name}"
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    columns_out = list(df_pandas.columns)
    print(f"  {full_name}: {count} rows")

    # Log lineage for this table
    lineage.log_lineage(
        step_name="01_generate_bronze_data",
        step_sequence=1,
        source_tables=[],
        target_table=full_name,
        transformation_type="generation",
        transformation_desc=description or f"Synthetic data generation for {table_name}",
        row_count_in=None,
        row_count_out=count,
        columns_out=columns_out,
        parameters=pipeline_params,
        status="success",
    )
    return count

# COMMAND ----------

BRONZE_TABLE_DESCRIPTIONS = {
    "counterparties":        "Generate 500 synthetic counterparties (issuers, reinsurers, banks) with LEIs, ratings, NACE sectors. Referenced by assets and reinsurance_contracts.",
    "assets":                "Generate ~5000 investment holdings (govt bonds 60%, corp bonds 20%, equity 10%, CIU 5%, property/cash 5%). Log-normal market values totalling EUR 6.5B. Realistic CIC codes, durations, credit ratings.",
    "policies":              "Generate ~20000 P&C policies across 7 non-life LoB (motor, property, liability, medical, income protection, misc). Pareto-distributed premiums totalling EUR 2B GWP. UW years 2020-2025.",
    "premiums_transactions": "Generate premium transactions for 8 quarters (2024-Q1 to 2025-Q4). Four types per policy-quarter: written, earned (pro-rata exposure), ceded_written, ceded_earned. LoB-specific cession rates 15-30%.",
    "claims_transactions":   "Generate claims with Poisson frequency and log-normal severity. Accident years 2020-2025, development years 0-5. LoB-specific tail lengths. 2% large losses (>EUR 500K). Paid/reserve_change/incurred types.",
    "claims_triangles":      "Pre-aggregate claims_transactions into cumulative development triangles by LoB × accident year × development year. Includes paid, incurred, case reserves, and claim counts.",
    "expenses":              "Generate ~2000 expense records across 6 categories (acquisition 12%, admin 10%, claims mgmt 5%, overhead 3%, investment mgmt 0.5%, other 0.5%). Allocated by LoB using premium/claims/headcount bases.",
    "reinsurance_contracts": "Generate ~50 reinsurance contracts: quota share per LoB, XL per LoB, surplus for property/motor, stop loss whole-account, and layered XL. Realistic programme structure with major reinsurers.",
    "technical_provisions":  "Derive Solvency II technical provisions per LoB: best estimate claims (from case reserves + IBNR factor), best estimate premium (UPR net of expected costs), risk margin (8-12% of BE claims), transitional measures.",
    "own_funds_components":  "Generate ~17 own funds components: Tier 1 unrestricted (80%, incl. share capital, reconciliation reserve, retained earnings), Tier 1 restricted, Tier 2 (subordinated debt), Tier 3 (DTA), ancillary, deductions. Total ~EUR 2B.",
    "risk_factors":          "Generate SCR Standard Formula sub-module capital charges: market (6 sub-modules), counterparty default (type 1/2), non-life (premium-reserve, cat, lapse), health (3 sub-modules), operational, intangible. BSCR ~EUR 1.35B.",
    "scr_parameters":        "Generate SCR calibration parameters: 10 inter-module correlations, LAC TP/DT amounts, operational risk factors, non-life premium/reserve sigma per LoB, equity/property/currency shocks, symmetric adjustment.",
    "balance_sheet_items":   "Derive full S.02.01 balance sheet: assets side reconciled to investment register, liabilities include TP + other liabilities, excess = own funds. ~55 line items matching EIOPA row references.",
}

print("Writing bronze tables to Unity Catalog...")
counts = {}
counts["bronze_counterparties"]        = write_delta(df_counterparties, "bronze_counterparties",        BRONZE_TABLE_DESCRIPTIONS["counterparties"])
counts["bronze_assets"]                = write_delta(df_assets, "bronze_assets",                        BRONZE_TABLE_DESCRIPTIONS["assets"])
counts["bronze_policies"]              = write_delta(df_policies, "bronze_policies",                    BRONZE_TABLE_DESCRIPTIONS["policies"])
counts["bronze_premiums_transactions"] = write_delta(df_premiums, "bronze_premiums_transactions",       BRONZE_TABLE_DESCRIPTIONS["premiums_transactions"])
counts["bronze_claims_transactions"]   = write_delta(df_claims, "bronze_claims_transactions",           BRONZE_TABLE_DESCRIPTIONS["claims_transactions"])
counts["bronze_claims_triangles"]      = write_delta(df_triangles, "bronze_claims_triangles",           BRONZE_TABLE_DESCRIPTIONS["claims_triangles"])
counts["bronze_expenses"]              = write_delta(df_expenses, "bronze_expenses",                    BRONZE_TABLE_DESCRIPTIONS["expenses"])
counts["bronze_reinsurance_contracts"] = write_delta(df_reinsurance, "bronze_reinsurance_contracts",    BRONZE_TABLE_DESCRIPTIONS["reinsurance_contracts"])
counts["bronze_technical_provisions"]  = write_delta(df_tp, "bronze_technical_provisions",              BRONZE_TABLE_DESCRIPTIONS["technical_provisions"])
counts["bronze_own_funds_components"]  = write_delta(df_own_funds, "bronze_own_funds_components",       BRONZE_TABLE_DESCRIPTIONS["own_funds_components"])
counts["bronze_risk_factors"]          = write_delta(df_risk_factors, "bronze_risk_factors",            BRONZE_TABLE_DESCRIPTIONS["risk_factors"])
counts["bronze_scr_parameters"]        = write_delta(df_scr_params, "bronze_scr_parameters",           BRONZE_TABLE_DESCRIPTIONS["scr_parameters"])
counts["bronze_balance_sheet_items"]   = write_delta(df_balance_sheet, "bronze_balance_sheet_items",    BRONZE_TABLE_DESCRIPTIONS["balance_sheet_items"])
print("All tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Table and Column Descriptions (Unity Catalog Governance)
# MAGIC Adds COMMENT metadata to every table and column for data discovery, lineage audit, and governance.

# COMMAND ----------

TABLE_METADATA = {
    "bronze_counterparties": {
        "table_comment": "Master counterparty register for all entities referenced in the investment portfolio, reinsurance programme, and banking relationships. Source system: synthetic (counterparty management). Grain: one row per counterparty. Used by: S.06.02 (issuer info), S.31.01 (reinsurer shares), counterparty default risk SCR.",
        "columns": {
            "counterparty_id":      "Unique internal identifier for the counterparty (e.g. CP00001). Primary key.",
            "counterparty_name":    "Legal registered name of the counterparty entity.",
            "lei":                  "Legal Entity Identifier (LEI) — 20-character ISO 17442 code. Synthetic but format-compliant.",
            "country":              "Country of domicile — ISO 3166-1 alpha-2 code (e.g. DE, FR, NL).",
            "sector_nace":          "NACE Rev.2 sector classification code of the counterparty's primary business activity.",
            "credit_rating":        "External credit rating on Standard & Poor's scale (AAA to D, including NR for not rated).",
            "credit_quality_step":  "EIOPA Credit Quality Step (CQS) mapped from the external rating. Range 0 (AAA) to 6 (CCC and below).",
            "counterparty_type":    "Role classification: issuer (bond/equity issuer), reinsurer, bank, or broker.",
            "is_regulated":         "Whether the counterparty is regulated by a financial supervisory authority (true/false).",
            "group_name":           "Name of the parent group, if the counterparty belongs to a group. Null if standalone.",
            "group_lei":            "LEI of the parent group entity. Null if standalone or group LEI not available.",
        }
    },
    "bronze_assets": {
        "table_comment": "Investment register containing all financial assets held by the undertaking at the reporting date. Source system: synthetic (investment management / custodian). Grain: one row per individual asset holding. Primary source for QRT S.06.02 (List of Assets). Also feeds S.02.01 balance sheet (investment assets) and market risk SCR inputs.",
        "columns": {
            "asset_id":             "Unique internal asset identifier (e.g. A000001). Primary key.",
            "asset_name":           "Descriptive name of the asset holding (e.g. 'Federal Republic of Germany 1.25% 2032').",
            "issuer_name":          "Legal name of the issuer or counterparty. References counterparties.counterparty_name for corporates.",
            "issuer_lei":           "LEI of the issuer. References counterparties.lei for corporates; synthetic sovereign LEIs for government bonds.",
            "issuer_country":       "Country of domicile of the issuer — ISO 3166-1 alpha-2.",
            "issuer_sector":        "NACE Rev.2 sector code of the issuer. O84 for government/sovereign.",
            "cic_code":             "EIOPA Complementary Identification Code (4 characters). First 2 = country (or XL for supranational), last 2 = asset category per EIOPA CIC table.",
            "currency":             "ISO 4217 currency code of the asset. All EUR in this demo.",
            "acquisition_date":     "Date the asset was acquired/purchased.",
            "maturity_date":        "Contractual maturity date for fixed-income instruments. Null for equity, property, and perpetual holdings.",
            "par_value":            "Face value / nominal / par amount in EUR. For equity = number of shares * nominal value.",
            "acquisition_cost":     "Total acquisition cost in EUR (purchase price including transaction costs).",
            "market_value_eur":     "Fair market value in EUR at the valuation date. Mark-to-market for listed, mark-to-model for unlisted.",
            "accrued_interest":     "Accrued but not yet received interest in EUR. Zero for equity and property.",
            "coupon_rate":          "Annual coupon rate as a decimal (e.g. 0.0125 for 1.25%). Zero for zero-coupon bonds, equity, property.",
            "credit_rating":        "External credit rating of the asset/issuer on S&P scale.",
            "credit_quality_step":  "EIOPA Credit Quality Step (0-6) mapped from the external credit rating.",
            "portfolio_type":       "Solvency II portfolio allocation: Life, Non-life, Ring-fenced, or Other.",
            "custodian_name":       "Name of the custodian bank holding the asset.",
            "valuation_date":       "Date at which the market value was determined (= reporting reference date).",
            "is_listed":            "Whether the asset is listed on a regulated exchange (true/false). Affects equity risk sub-module.",
            "infrastructure_flag":  "Whether the asset qualifies as an infrastructure investment under Solvency II (true/false). Affects equity risk charge.",
            "modified_duration":    "Macaulay modified duration in years. Measures interest rate sensitivity. Null for equity/property.",
            "asset_class":          "Derived asset class category: government_bonds, corporate_bonds, equity, ciu, property, cash, other.",
            "reporting_period":     "Reporting period in YYYY-QN format (e.g. 2025-Q4).",
        }
    },
    "bronze_policies": {
        "table_comment": "Policy administration register containing all P&C insurance policies across underwriting years 2020-2025. Source system: synthetic (policy admin). Grain: one row per policy. Feeds premium calculations for S.05.01 and claims linkage. LoB classification follows EIOPA Annex I to Delegated Regulation.",
        "columns": {
            "policy_id":            "Unique policy identifier (e.g. POL0000001). Primary key. Referenced by premiums_transactions and claims_transactions.",
            "line_of_business":     "EIOPA Solvency II line of business name per Annex I (e.g. 'Motor vehicle liability insurance').",
            "lob_code":             "Numeric EIOPA LoB code: 1-12 for non-life direct business. Maps to S.05.01 column references.",
            "inception_date":       "Policy effective start date.",
            "expiry_date":          "Policy effective end date.",
            "currency":             "Policy denomination currency — ISO 4217. All EUR.",
            "country_risk":         "Country where the risk is situated — ISO 3166-1 alpha-2. Relevant for geographical diversification.",
            "status":               "Current policy status: active, lapsed, or cancelled.",
            "sum_insured_eur":      "Total sum insured / policy limit in EUR.",
            "annual_premium_eur":   "Annualised gross written premium in EUR for this policy.",
            "premium_frequency":    "Payment frequency: annual, semi_annual, quarterly, or monthly.",
            "channel":              "Distribution channel: broker, direct, or agent.",
            "underwriting_year":    "Year the policy was originally underwritten (inception year). Used for underwriting year analysis.",
            "reporting_period":     "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_premiums_transactions": {
        "table_comment": "Premium accounting transactions at policy-transaction level. Covers 8 quarters (2024-Q1 to 2025-Q4). Source system: synthetic (general ledger / sub-ledger). Grain: one row per policy per quarter per transaction type. Aggregates to S.05.01 premium rows (R0110-R0300). Transaction types: written (policy inception/renewal), earned (pro-rata exposure), ceded_written, ceded_earned.",
        "columns": {
            "transaction_id":       "Unique transaction identifier (e.g. PT0000001). Primary key.",
            "policy_id":            "Foreign key to policies.policy_id.",
            "transaction_date":     "Accounting date of the premium transaction.",
            "transaction_type":     "Type of premium transaction: written, earned, ceded_written, or ceded_earned.",
            "gross_amount_eur":     "Gross premium amount in EUR (before reinsurance).",
            "reinsurance_amount_eur": "Reinsurance ceded premium amount in EUR. Equals gross for ceded transaction types.",
            "net_amount_eur":       "Net premium amount in EUR (gross minus reinsurance). Zero for ceded transaction types.",
            "reporting_period":     "Reporting quarter in YYYY-QN format. Determines which QRT period the transaction falls into.",
            "accounting_year":      "Calendar year of the accounting entry.",
            "underwriting_year":    "Original underwriting year of the policy generating this premium.",
        }
    },
    "bronze_claims_transactions": {
        "table_comment": "Claims accounting transactions with development history. Covers accident years 2020-2025 with development years 0-5. Source system: synthetic (claims management). Grain: one row per claim per development event. Includes paid, reserve_change, incurred, and recovery transaction types. Supports S.05.01 claims rows (R0310-R0400), S.19.01 triangles, and technical provisions derivation.",
        "columns": {
            "claim_id":                 "Unique claim identifier (e.g. CLM0000001). Multiple rows per claim (one per development event).",
            "policy_id":                "Foreign key to policies.policy_id. Links claim to the originating policy.",
            "loss_date":                "Date of the loss/accident event.",
            "notification_date":        "Date the claim was reported/notified to the insurer.",
            "transaction_date":         "Accounting date of this claims transaction entry.",
            "transaction_type":         "Type: incurred (initial estimate at notification), paid (actual payment), reserve_change (case reserve adjustment), recovery (salvage/subrogation).",
            "claim_status":             "Status at the time of this transaction: open, closed, or reopened.",
            "gross_amount_eur":         "Gross claim amount in EUR for this transaction.",
            "reinsurance_recovery_eur": "Reinsurance recovery amount in EUR.",
            "net_amount_eur":           "Net claim amount in EUR (gross minus reinsurance recovery).",
            "large_loss_flag":          "True if the claim's ultimate gross incurred exceeds EUR 500,000. Relevant for XL reinsurance and large loss analysis.",
            "cause_of_loss":            "Cause/peril category (e.g. collision, theft, fire, flood, bodily_injury). Varies by line of business.",
            "reporting_period":         "Reporting quarter in YYYY-QN format of this transaction.",
            "accident_year":            "Year of the loss event. Used for triangle development and accident-year analysis.",
            "development_year":         "Number of years since the accident year (0 = same year as loss). Used for claims triangle construction.",
        }
    },
    "bronze_claims_triangles": {
        "table_comment": "Pre-aggregated claims development triangles by LoB, accident year, and development year. Source: derived from claims_transactions. Grain: one row per LoB × accident year × development year. Designed for S.19.01 (Non-life Claims Information) and actuarial reserving analysis.",
        "columns": {
            "lob_code":                 "EIOPA line of business numeric code.",
            "line_of_business":         "EIOPA line of business name.",
            "accident_year":            "Accident/loss year.",
            "development_year":         "Development year (0 = same year as accident).",
            "cumulative_paid_gross":    "Cumulative gross paid claims in EUR from accident year to this development year.",
            "cumulative_paid_net":      "Cumulative net paid claims in EUR (after reinsurance recovery).",
            "cumulative_incurred_gross": "Cumulative gross incurred claims in EUR (paid + outstanding reserves).",
            "cumulative_incurred_net":  "Cumulative net incurred claims in EUR.",
            "case_reserves_gross":      "Outstanding case reserves (gross) at this development year in EUR.",
            "case_reserves_net":        "Outstanding case reserves (net) at this development year in EUR.",
            "claim_count_open":         "Number of open claims at this development point.",
            "claim_count_closed":       "Number of closed claims at this development point.",
            "reporting_period":         "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_expenses": {
        "table_comment": "Expense allocation records by line of business and expense category. Source system: synthetic (general ledger / cost allocation). Grain: one row per expense record (allocated to LoB and category). Aggregates to S.05.01 expense rows (R0550-R1300). Total expense ratio targets ~30% of GWP.",
        "columns": {
            "expense_id":           "Unique expense record identifier (e.g. EXP000001). Primary key.",
            "line_of_business":     "EIOPA line of business to which this expense is allocated.",
            "lob_code":             "Numeric EIOPA LoB code.",
            "expense_category":     "Expense type: acquisition, administrative, claims_management, overhead, investment_management, or other.",
            "gross_amount_eur":     "Expense amount in EUR.",
            "allocation_basis":     "Method used to allocate expense to LoB: premium (pro-rata GWP), claims (pro-rata claims), headcount, or direct.",
            "cost_centre":          "Organisational cost centre responsible (e.g. CC100-Underwriting, CC700-Actuarial).",
            "reporting_period":     "Reporting period in YYYY-QN format.",
            "accounting_year":      "Calendar year of the expense entry.",
        }
    },
    "bronze_reinsurance_contracts": {
        "table_comment": "Reinsurance programme structure — all treaty reinsurance contracts in force. Source system: synthetic (reinsurance management). Grain: one row per reinsurance contract. Covers quota share, excess of loss, surplus, and stop loss treaties. Supports S.30.03 (Outgoing Reinsurance Programme), S.31.01 (Share of Reinsurers), and cession calculations for all QRTs.",
        "columns": {
            "contract_id":          "Unique reinsurance contract identifier (e.g. RI0001). Primary key.",
            "contract_name":        "Descriptive contract name (e.g. 'QS Motor vehicle liability insurance 2025').",
            "reinsurer_name":       "Legal name of the reinsurance counterparty.",
            "reinsurer_lei":        "LEI of the reinsurer — 20-character ISO 17442 format.",
            "reinsurer_country":    "Country of domicile of the reinsurer — ISO 3166-1 alpha-2.",
            "reinsurer_credit_rating": "External credit rating of the reinsurer on S&P scale.",
            "contract_type":        "Treaty type: quota_share, excess_of_loss, surplus, or stop_loss.",
            "lob_codes_covered":    "Comma-separated list of EIOPA LoB codes covered by this contract.",
            "inception_date":       "Contract effective start date.",
            "expiry_date":          "Contract effective end date.",
            "cession_rate":         "Proportional cession rate as decimal (e.g. 0.20 for 20%). Null for non-proportional treaties.",
            "retention_eur":        "Retention amount in EUR. For XL = attachment point, for surplus = retained line. Null for quota share.",
            "limit_eur":            "Maximum cover limit in EUR. For XL = layer limit, for stop loss = aggregate limit. Null for quota share.",
            "priority_eur":         "Priority / attachment point in EUR for non-proportional treaties. Null for proportional.",
            "premium_eur":          "Reinsurance premium payable in EUR for this contract.",
            "commission_rate":      "Reinsurance commission rate as decimal (e.g. 0.30 for 30% ceding commission).",
            "reporting_period":     "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_technical_provisions": {
        "table_comment": "Solvency II technical provisions by line of business and provision type. Source: derived from claims reserves and actuarial best estimates. Grain: one row per LoB × provision type. Feeds S.17.01 (Non-Life Technical Provisions), S.02.01 balance sheet (liabilities side), and SCR calculations.",
        "columns": {
            "lob_code":                 "EIOPA line of business code. 0 = all lines (used for transitional measures).",
            "line_of_business":         "EIOPA line of business name.",
            "provision_type":           "Type of provision: best_estimate_claims (discounted future claims payments), best_estimate_premium (unearned premium reserve net of expected costs), risk_margin (cost-of-capital risk margin per Art. 37), transitional (transitional measures on TP per Art. 308d).",
            "gross_amount_eur":         "Gross technical provision amount in EUR.",
            "reinsurance_recoverable_eur": "Reinsurance recoverables amount in EUR. Deducted from gross to derive net TP.",
            "net_amount_eur":           "Net technical provision amount in EUR (gross minus RI recoverables).",
            "reporting_period":         "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_own_funds_components": {
        "table_comment": "Solvency II own funds breakdown by tier and component. Source: synthetic (balance sheet / capital management). Grain: one row per own funds component. Feeds S.23.01 (Own Funds) and solvency ratio calculation. Target total ~EUR 2B providing ~170% solvency ratio.",
        "columns": {
            "component_id":     "Unique component identifier (e.g. OF001). Primary key.",
            "component_name":   "Descriptive name of the own funds component (e.g. 'Ordinary share capital', 'Reconciliation reserve').",
            "tier":             "Solvency II tier classification: tier1_unrestricted, tier1_restricted, tier2, or tier3. Determines eligibility to cover SCR/MCR.",
            "amount_eur":       "Amount in EUR. Negative for deductions (e.g. participations deduction, foreseeable dividends).",
            "classification":   "basic (balance sheet derived) or ancillary (off-balance sheet, requires supervisory approval).",
            "reporting_period":  "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_risk_factors": {
        "table_comment": "SCR Standard Formula sub-module capital charges. Source: synthetic (actuarial / risk management). Grain: one row per risk module × sub-module. Feeds S.25.01 (SCR Standard Formula) and S.26/S.27 detailed risk modules. BSCR target ~EUR 1.35B, final SCR ~EUR 1.15B.",
        "columns": {
            "risk_module":                      "Top-level SCR risk module: market, counterparty_default, non_life, health, operational, or intangible.",
            "risk_sub_module":                  "Sub-module within the risk module (e.g. interest_rate, equity, premium_reserve). 'total' for modules without sub-modules (operational, intangible).",
            "gross_capital_charge_eur":          "Gross (pre-diversification within module) capital charge in EUR.",
            "diversification_within_module_eur": "Intra-module diversification benefit in EUR. Negative value representing capital relief.",
            "net_capital_charge_eur":            "Net capital charge after intra-module diversification in EUR. Used in inter-module BSCR aggregation.",
            "reporting_period":                  "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_scr_parameters": {
        "table_comment": "SCR Standard Formula calibration parameters including inter-module correlation matrix, loss-absorbing capacity adjustments, operational risk factors, non-life premium/reserve sigma factors, and equity/property stress parameters. Source: EIOPA calibration (Delegated Regulation) plus entity-specific inputs. Grain: one row per parameter.",
        "columns": {
            "parameter_name":       "Parameter identifier. Naming convention: corr_<mod1>_<mod2> for correlations, lac_* for loss-absorbing capacity, op_risk_* for operational risk, nl_* for non-life, equity_*/property_*/currency_* for market risk shocks.",
            "parameter_value":      "Parameter value as string (cast to numeric for use). Correlations are 0-1, shock factors are decimals, monetary amounts are in EUR.",
            "parameter_category":   "Category: correlation (inter-module), adjustment (LAC, EPIFP), threshold (op risk caps), calibration (risk factor shocks), or sigma (volatility parameters).",
            "description":          "Human-readable description of the parameter and its regulatory source.",
            "reporting_period":     "Reporting period in YYYY-QN format.",
        }
    },
    "bronze_balance_sheet_items": {
        "table_comment": "Solvency II balance sheet items in the S.02.01 reporting structure. Source: derived from assets, technical_provisions, and own_funds_components tables. Grain: one row per balance sheet line item. Asset totals reconcile to the assets table, liabilities include TP and other liabilities, excess of assets over liabilities equals own funds.",
        "columns": {
            "item_id":          "Unique balance sheet line item identifier (e.g. BS001). Primary key.",
            "item_name":        "Descriptive name matching EIOPA S.02.01 row labels (e.g. 'Government bonds', 'Best estimate - non-life').",
            "s0201_row_id":     "EIOPA S.02.01 template row reference (e.g. R0140 for Government bonds, R0510 for Best estimate non-life).",
            "category":         "Balance sheet section: assets, liabilities, or own_funds.",
            "amount_eur":       "Amount in EUR. Assets and own funds are positive, liabilities are positive on the liabilities side.",
            "reporting_period":  "Reporting period in YYYY-QN format.",
        }
    },
}

# COMMAND ----------

def apply_table_comments(table_name, metadata):
    """Apply COMMENT ON TABLE and ALTER COLUMN COMMENT for governance metadata."""
    full_name = f"{catalog}.{schema}.{table_name}"

    # Table-level comment
    tbl_comment = metadata["table_comment"].replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{tbl_comment}'")

    # Column-level comments
    existing_cols = {f.name for f in spark.table(full_name).schema.fields}
    applied = 0
    skipped = []
    for col_name, col_comment in metadata["columns"].items():
        if col_name in existing_cols:
            safe_comment = col_comment.replace("'", "''")
            spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'")
            applied += 1
        else:
            skipped.append(col_name)

    status = f"  {table_name}: table comment + {applied} column comments"
    if skipped:
        status += f" (skipped {len(skipped)} not found: {skipped})"
    print(status)

# COMMAND ----------

print("Applying table and column descriptions for Unity Catalog governance...")
for table_name, metadata in TABLE_METADATA.items():
    apply_table_comments(table_name, metadata)
print("All governance metadata applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 70)
print(f"  BRONZE DATA GENERATION COMPLETE — {entity_name}")
print(f"  Reporting period: {reporting_period}  |  Seed: {random_seed}  |  Scale: {scale_factor}")
print("=" * 70)

print(f"\n{'Table':<30} {'Rows':>10}")
print("-" * 42)
for tbl, cnt in counts.items():
    print(f"  {tbl:<28} {cnt:>10,}")
total_rows = sum(counts.values())
print("-" * 42)
print(f"  {'TOTAL':<28} {total_rows:>10,}")

# Key financial metrics
gwp_total = df_policies["annual_premium_eur"].sum()
paid_gross = df_claims[df_claims["transaction_type"] == "paid"]["gross_amount_eur"].sum()
incurred_gross = df_claims[df_claims["transaction_type"] == "incurred"]["gross_amount_eur"].sum()
expense_total = df_expenses["gross_amount_eur"].sum()
loss_ratio = paid_gross / gwp_total if gwp_total > 0 else 0
expense_ratio = expense_total / gwp_total if gwp_total > 0 else 0
combined_ratio = loss_ratio + expense_ratio
scr_total = df_risk_factors["net_capital_charge_eur"].sum()
of_total = df_own_funds["amount_eur"].sum()
solvency_ratio = of_total / scr_total if scr_total > 0 else 0

print(f"\n{'Metric':<40} {'Value':>20}")
print("-" * 62)
print(f"  {'Total assets (market value)':<38} EUR {actual_total_mv/1e9:>10.2f}B")
print(f"  {'Gross written premium':<38} EUR {gwp_total/1e9:>10.2f}B")
print(f"  {'Total paid claims (gross)':<38} EUR {paid_gross/1e6:>10.0f}M")
print(f"  {'Total incurred (gross)':<38} EUR {incurred_gross/1e6:>10.0f}M")
print(f"  {'Total expenses':<38} EUR {expense_total/1e6:>10.0f}M")
print(f"  {'Loss ratio (paid/GWP)':<38} {loss_ratio:>10.1%}")
print(f"  {'Expense ratio':<38} {expense_ratio:>10.1%}")
print(f"  {'Combined ratio':<38} {combined_ratio:>10.1%}")
print(f"  {'Technical provisions (gross)':<38} EUR {total_tp_gross/1e6:>10.0f}M")
print(f"  {'Own funds (total)':<38} EUR {of_total/1e6:>10.0f}M")
print(f"  {'SCR (sum of net charges)':<38} EUR {scr_total/1e6:>10.0f}M")
print(f"  {'Solvency II ratio':<38} {solvency_ratio:>10.1%}")
print("=" * 62)
