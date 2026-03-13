# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: SCR Standard Formula — Correlation Matrix & BSCR Aggregation
# MAGIC Applies the EIOPA 5x5 inter-module correlation matrix using the square-root formula
# MAGIC to produce the SCR waterfall for S.25.01.

# COMMAND ----------

import dlt
import numpy as np
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCR Modules Table

# COMMAND ----------

@dlt.table(
    name="silver_scr_modules",
    comment="SCR standard formula waterfall: sub-modules, modules, BSCR, adjustments, and final SCR"
)
@dlt.expect("bscr_positive", "level != 'bscr' OR net_capital_charge > 0")
@dlt.expect("scr_positive", "level != 'scr' OR net_capital_charge > 0")
def silver_scr_modules():
    # ── Step 1: Read risk factors ──────────────────────────────────────
    df_risk_factors = dlt.read("bronze_risk_factors")
    risk_rows = df_risk_factors.select(
        "risk_module", "risk_sub_module", "reporting_period",
        "gross_capital_charge_eur", "diversification_within_module_eur", "net_capital_charge_eur"
    ).collect()

    # Find the reporting period from the data
    reporting_periods = set(r["reporting_period"] for r in risk_rows)
    reporting_period = sorted(reporting_periods)[-1] if reporting_periods else "2025-Q4"

    # Build sub-module data dict
    sub_module_data = {}
    module_net_charges = {}
    for r in risk_rows:
        if r["reporting_period"] != reporting_period:
            continue
        key = (r["risk_module"], r["risk_sub_module"])
        sub_module_data[key] = {
            "gross": r["gross_capital_charge_eur"],
            "div": r["diversification_within_module_eur"],
            "net": r["net_capital_charge_eur"],
        }
        mod = r["risk_module"]
        module_net_charges[mod] = module_net_charges.get(mod, 0.0) + r["net_capital_charge_eur"]

    # ── Step 2: Read correlation matrix ────────────────────────────────
    df_corr_params = dlt.read("bronze_scr_parameters")
    corr_rows = df_corr_params.filter(
        "parameter_category = 'correlation'"
    ).select("parameter_name", "parameter_value").collect()

    MODULE_ORDER = ["market", "default", "life", "health", "non_life"]
    n_modules = len(MODULE_ORDER)
    mod_idx = {m: i for i, m in enumerate(MODULE_ORDER)}

    corr_matrix = np.eye(n_modules)
    for r in corr_rows:
        name = r["parameter_name"].replace("corr_", "")
        for m1 in MODULE_ORDER:
            if name.startswith(m1 + "_"):
                m2_candidate = name[len(m1) + 1:]
                if m2_candidate in MODULE_ORDER:
                    i, j = mod_idx[m1], mod_idx[m2_candidate]
                    corr_matrix[i, j] = float(r["parameter_value"])
                    corr_matrix[j, i] = float(r["parameter_value"])
                    break

    # ── Step 3: Calculate BSCR (square-root formula) ───────────────────
    scr_vector = np.array([
        module_net_charges.get("market", 0.0),
        module_net_charges.get("counterparty_default", 0.0),
        0.0,  # life SCR = 0 for P&C
        module_net_charges.get("health", 0.0),
        module_net_charges.get("non_life", 0.0),
    ])

    bscr_sum = 0.0
    for i in range(n_modules):
        for j in range(n_modules):
            bscr_sum += corr_matrix[i, j] * scr_vector[i] * scr_vector[j]

    bscr_pre_intangible = float(np.sqrt(bscr_sum))
    intangible_scr = module_net_charges.get("intangible", 0.0)
    bscr = bscr_pre_intangible + intangible_scr

    sum_of_modules = float(sum(scr_vector)) + intangible_scr
    inter_module_diversification = bscr - sum_of_modules

    # ── Step 4: Adjustments ────────────────────────────────────────────
    operational_risk = module_net_charges.get("operational", 0.0)

    lac_rows = dlt.read("bronze_scr_parameters").filter(
        "parameter_name IN ('lac_tp_amount', 'lac_dt_amount')"
    ).select("parameter_name", "parameter_value").collect()
    lac_params = {r["parameter_name"]: float(r["parameter_value"]) for r in lac_rows}
    lac_tp = -abs(lac_params.get("lac_tp_amount", 0.0))
    lac_dt = -abs(lac_params.get("lac_dt_amount", 0.0))

    final_scr = bscr + operational_risk + lac_tp + lac_dt

    # ── Step 5: Build output rows ──────────────────────────────────────
    MODULE_DISPLAY = {
        "market": "Market risk",
        "counterparty_default": "Counterparty default risk",
        "non_life": "Non-life underwriting risk",
        "health": "Health underwriting risk",
    }
    MODULE_SUBS = {
        "market": ["interest_rate", "equity", "property", "spread", "currency", "concentration"],
        "counterparty_default": ["type1", "type2"],
        "non_life": ["premium_reserve", "catastrophe", "lapse"],
        "health": ["similar_life", "similar_nl", "catastrophe"],
    }

    output_rows = []
    for mod_key in ["market", "counterparty_default", "non_life", "health"]:
        display_name = MODULE_DISPLAY[mod_key]
        mod_gross, mod_div, mod_net = 0.0, 0.0, 0.0

        for sub in MODULE_SUBS[mod_key]:
            data = sub_module_data.get((mod_key, sub), {"gross": 0.0, "div": 0.0, "net": 0.0})
            mod_gross += data["gross"]
            mod_div += data["div"]
            mod_net += data["net"]
            output_rows.append(Row(
                module_name=display_name, sub_module_name=sub, level="sub_module",
                gross_capital_charge=data["gross"], intra_module_diversification=data["div"],
                net_capital_charge=data["net"], is_diversification=False,
                parent_module=display_name, reporting_period=reporting_period,
            ))

        output_rows.append(Row(
            module_name=display_name, sub_module_name="diversification", level="sub_module",
            gross_capital_charge=0.0, intra_module_diversification=mod_div,
            net_capital_charge=mod_div, is_diversification=True,
            parent_module=display_name, reporting_period=reporting_period,
        ))
        output_rows.append(Row(
            module_name=display_name, sub_module_name="TOTAL", level="module",
            gross_capital_charge=mod_gross, intra_module_diversification=mod_div,
            net_capital_charge=mod_net, is_diversification=False,
            parent_module=None, reporting_period=reporting_period,
        ))

    # Intangible risk module
    output_rows.append(Row(
        module_name="Intangible risk", sub_module_name="TOTAL", level="module",
        gross_capital_charge=intangible_scr, intra_module_diversification=0.0,
        net_capital_charge=intangible_scr, is_diversification=False,
        parent_module=None, reporting_period=reporting_period,
    ))

    # Intermediate: sum of modules, diversification
    output_rows.append(Row(
        module_name="BSCR (before diversification)", sub_module_name="sum_of_modules",
        level="intermediate", gross_capital_charge=sum_of_modules,
        intra_module_diversification=0.0, net_capital_charge=sum_of_modules,
        is_diversification=False, parent_module=None, reporting_period=reporting_period,
    ))
    output_rows.append(Row(
        module_name="Inter-module diversification", sub_module_name="diversification",
        level="intermediate", gross_capital_charge=0.0,
        intra_module_diversification=inter_module_diversification,
        net_capital_charge=inter_module_diversification, is_diversification=True,
        parent_module=None, reporting_period=reporting_period,
    ))

    # BSCR total
    output_rows.append(Row(
        module_name="BSCR", sub_module_name="total", level="bscr",
        gross_capital_charge=0.0, intra_module_diversification=0.0,
        net_capital_charge=bscr, is_diversification=False,
        parent_module=None, reporting_period=reporting_period,
    ))

    # Adjustments
    for name, val, lvl in [
        ("Operational risk", operational_risk, "adjustment"),
        ("LAC Technical Provisions", lac_tp, "adjustment"),
        ("LAC Deferred Taxes", lac_dt, "adjustment"),
    ]:
        output_rows.append(Row(
            module_name=name, sub_module_name="total", level=lvl,
            gross_capital_charge=val, intra_module_diversification=0.0,
            net_capital_charge=val, is_diversification=False,
            parent_module=None, reporting_period=reporting_period,
        ))

    # Final SCR
    output_rows.append(Row(
        module_name="Solvency Capital Requirement", sub_module_name="total", level="scr",
        gross_capital_charge=final_scr, intra_module_diversification=0.0,
        net_capital_charge=final_scr, is_diversification=False,
        parent_module=None, reporting_period=reporting_period,
    ))

    return spark.createDataFrame(output_rows)
