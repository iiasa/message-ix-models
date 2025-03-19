import os
import numpy as np
from collections.abc import Sequence
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models.util import package_data_path
if TYPE_CHECKING:
    from message_ix_models import Context

# constant for buffer factor
BUFFER_FACTOR = 0.95

# dsl rule config for water availability; each rule describes file naming,
# transformation steps and mapping to message ix dataframes
DSL_AWA_RULES = [
    {
        "rule_name": "surface_availability",
        "file_prefix": "qtot",
        "commodity": "surfacewater_basin",
        "level": "water_avail_basin",
        "sign": -1,
        "unit": "km3/year",
        "time_mode": "annual",
        "file_name_pattern": "{file_prefix}_5y{suffix}_{RCP}_{REL}_{regions}.csv",
        "transformations": [
            {"action": "drop_columns", "columns": ["Unnamed: 0"]},
            {"action": "set_index_from_basins", "basin_col": "BCU_name"},
            {"action": "stack_df"},
            {"action": "rename_columns", "mapping": {"level_0": "Region", "level_1": "years", 0: "value"}},
            {"action": "fillna", "value": 0},
            {"action": "reset_index", "drop": True},
            {"action": "extract_time", "mode": "annual"},
            {"action": "apply_year_patch"}
        ],
    },
    {
        "rule_name": "groundwater_availability",
        "file_prefix": "qr",
        "commodity": "groundwater_basin",
        "level": "water_avail_basin",
        "sign": -1,
        "unit": "km3/year",
        "time_mode": "annual",
        "file_name_pattern": "{file_prefix}_5y{suffix}_{RCP}_{REL}_{regions}.csv",
        "transformations": [
            {"action": "drop_columns", "columns": ["Unnamed: 0"]},
            {"action": "set_index_from_basins", "basin_col": "BCU_name"},
            {"action": "stack_df"},
            {"action": "rename_columns", "mapping": {"level_0": "Region", "level_1": "years", 0: "value"}},
            {"action": "fillna", "value": 0},
            {"action": "reset_index", "drop": True},
            {"action": "extract_time", "mode": "annual"},
            {"action": "apply_year_patch"}
        ],
    },
]

def _read_basins(context: "Context") -> pd.DataFrame:
    # read basins file
    path = package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv")
    return pd.read_csv(path)

def apply_transformations(df: pd.DataFrame, transformations: list, context: "Context", df_basins: pd.DataFrame, info) -> pd.DataFrame:
    # transformation pipeline applying each rule in sequence
    for trx in transformations:
        action = trx["action"]
        match action:
            case "drop_columns":
                df = df.drop(columns=trx["columns"])
            case "set_index_from_basins":
                # set index using basin names from df_basins
                df.index = df_basins[trx["basin_col"]].to_list()
            case "stack_df":
                df = df.stack().reset_index()
            case "rename_columns":
                df = df.rename(columns=trx["mapping"])
            case "fillna":
                df = df.fillna(trx["value"])
            case "reset_index":
                df = df.reset_index(drop=trx.get("drop", False))
            case "extract_time":
                mode = trx["mode"]
                if mode == "annual":
                    df["year"] = pd.DatetimeIndex(df["years"]).year
                    df["time"] = "year"
                elif mode == "subannual":
                    df = df.sort_values(["Region", "years", "value"])
                    df["year"] = pd.DatetimeIndex(df["years"]).year
                    df["time"] = pd.DatetimeIndex(df["years"]).month
                else:
                    raise ValueError(f"unknown time mode {mode}")
            case "apply_year_patch":
                patch = df[df["year"] == 2100].copy()
                patch["year"] = 2110
                df = pd.concat([df, patch])
                df = df[df["year"].isin(info.Y)]
            case _:
                raise ValueError(f"unknown transformation action: {action}")
    return df

def apply_awa_rule_generic(rule: dict, context: "Context", df_basins: pd.DataFrame, info) -> pd.DataFrame:
    suffix = "" if rule.get("time_mode", "annual") == "annual" else "_m"
    filename = rule["file_name_pattern"].format(
        file_prefix=rule["file_prefix"],
        suffix=suffix,
        RCP=context.RCP,
        REL=context.REL,
        regions=context.regions
    )
    path = package_data_path("water", "availability", filename)
    df = pd.read_csv(path)
    df = apply_transformations(df, rule["transformations"], context, df_basins, info)
    dmd_df = make_df(
        "demand",
        node="B" + df["Region"].astype(str),
        commodity=rule["commodity"],
        level=rule["level"],
        year=df["year"],
        time=df["time"],
        value=rule["sign"] * df["value"],
        unit=rule["unit"],
    )
    # clamp positive values to zero
    dmd_df["value"] = np.minimum(dmd_df["value"], 0)
    return dmd_df

def read_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    # facade for water availability via dsl rules
    info = context["water build info"]
    df_basins = _read_basins(context)
    results = {}
    for rule in DSL_AWA_RULES:
        results[rule["rule_name"]] = apply_awa_rule_generic(rule, context, df_basins, info)
    return results

def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    # compile water supply constraints from dsl-driven availability
    results = {}
    awa_dfs = read_water_availability(context)
    results["demand"] = pd.concat(list(awa_dfs.values()))
    sw = awa_dfs["surface_availability"]
    gw = awa_dfs["groundwater_availability"]
    share_value = (gw["value"].abs() / (sw["value"].abs() + gw["value"].abs())) * BUFFER_FACTOR
    df_share = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + gw["Region"].astype(str),
        year_act=gw["year"],
        time=gw["time"],
        value=share_value,
        unit="-",
    )
    df_share["value"] = df_share["value"].fillna(0)
    results["share_commodity_lo"] = df_share
    return results

def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    # process irrigation demand from globiom
    results = {}
    scen = context.get_scenario()
    def _get_irrigation_df(commodity: str, level: str) -> pd.DataFrame:
        df = scen.par("land_output", {"commodity": commodity})
        df["level"] = level
        return df
    land_out = pd.concat([
        _get_irrigation_df("Water|Withdrawal|Irrigation|Cereals", "irr_cereal"),
        _get_irrigation_df("Water|Withdrawal|Irrigation|Oilcrops", "irr_oilcrops"),
        _get_irrigation_df("Water|Withdrawal|Irrigation|Sugarcrops", "irr_sugarcrops")
    ])
    land_out["commodity"] = "freshwater"
    land_out["value"] = 1e-3 * land_out["value"]
    results["land_input"] = land_out
    return results