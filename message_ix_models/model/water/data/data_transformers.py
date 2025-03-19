from __future__ import annotations
import pandas as pd

# assume make_df and broadcast are defined elsewhere in the project
# from ..utils import make_df, broadcast

# dsl rules for water demand transformation; each defines the source data key(s)
DSL_RULES = [
    {
        "commodity": "urban_mw",
        "withdrawal": "urban_withdrawal",
        "rate": "urban_connection_rate",
        "conversion": 1e-3,
        "rate_op": "identity",
        "sign": 1,
    },
    {
        "commodity": "urban_disconnected",
        "withdrawal": "urban_withdrawal",
        "rate": "urban_connection_rate",
        "conversion": 1e-3,
        "rate_op": "invert",
        "sign": 1,
    },
    {
        "commodity": "rural_mw",
        "withdrawal": "rural_withdrawal",
        "rate": "rural_connection_rate",
        "conversion": 1e-3,
        "rate_op": "identity",
        "sign": 1,
    },
    {
        "commodity": "rural_disconnected",
        "withdrawal": "rural_withdrawal",
        "rate": "rural_connection_rate",
        "conversion": 1e-3,
        "rate_op": "invert",
        "sign": 1,
    },
    {
        "commodity": "industry_mw",
        "withdrawal": "manufacturing_withdrawal",
        "conversion": 1e-3,
        "rate_op": None,
        "sign": 1,
    },
    {
        "commodity": "industry_uncollected_wst",
        "withdrawal": "manufacturing_return",
        "conversion": 1e-3,
        "rate_op": None,
        "sign": -1,
    },
    {
        "commodity": "urban_collected_wst",
        "withdrawal": "urban_return",
        "rate": "urban_treatment_rate",
        "conversion": 1e-3,
        "rate_op": "identity",
        "sign": -1,
    },
    {
        "commodity": "rural_collected_wst",
        "withdrawal": "rural_return",
        "rate": "rural_treatment_rate",
        "conversion": 1e-3,
        "rate_op": "identity",
        "sign": -1,
    },
    {
        "commodity": "urban_uncollected_wst",
        "withdrawal": "urban_return",
        "rate": "urban_treatment_rate",
        "conversion": 1e-3,
        "rate_op": "invert",
        "sign": -1,
    },
    {
        "commodity": "rural_uncollected_wst",
        "withdrawal": "rural_return",
        "rate": "rural_treatment_rate",
        "conversion": 1e-3,
        "rate_op": "invert",
        "sign": -1,
    },
]

# new: unit conversion helper using a reference dict for common unit conversions
def convert_units(value, from_unit: str, to_unit: str):
    # convert between common water demand units
    conversion_factors = {
        ("km3/year", "mcm/year"): 1000,
        ("mcm/year", "km3/year"): 0.001,
        ("km3", "mcm"): 1000,
        ("mcm", "km3"): 0.001,
    }
    if from_unit.lower() == to_unit.lower():
        return value
    key = (from_unit.lower(), to_unit.lower())
    if key in conversion_factors:
        return value * conversion_factors[key]
    raise ValueError(f"conversion from {from_unit} to {to_unit} not defined")

def _compute_value(withdrawal, rate, conversion, rate_op):
    # use pattern matching to choose the appropriate rate adjustment
    match rate_op:
        case "identity":
            return conversion * withdrawal * rate
        case "invert":
            return conversion * withdrawal * (1 - rate)
        case None:
            return conversion * withdrawal
        case _:
            raise ValueError(f"unknown rate_op {rate_op}")

def apply_transformation_rule(rule: dict, comps: dict, node_prefix: str = "B") -> pd.DataFrame:
    # get the withdrawal dataframe; key names come from the dsl rule
    df_withd = comps[rule["withdrawal"]].reset_index(drop=True)
    if rule.get("rate") is not None:
        df_rate = comps[rule["rate"]].drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        df = df_withd.merge(df_rate)
        df["value"] = _compute_value(df["value"], df["rate"], rule["conversion"], rule["rate_op"])
    else:
        df = df_withd.copy()
        df["value"] = rule["conversion"] * df["value"]
    if rule.get("sign", 1) < 0:
        df["value"] = -df["value"]

    # convert output from km3/year to mcm/year
    df_converted_value = convert_units(df["value"], "km3/year", "mcm/year")
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=rule["commodity"],
        level="final",
        year=df["year"],
        time=df["time"],
        value=df_converted_value,
        unit="mcm/year",
    ) 