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

def _read_basins(context: "Context") -> pd.DataFrame:
    # read basins file
    path = package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv")
    return pd.read_csv(path)

def process_availability_data(df: pd.DataFrame, df_basins: pd.DataFrame, time_mode: str, info) -> pd.DataFrame:
    # common transformation pipeline for water data
    df = df.drop(columns=["Unnamed: 0"])
    df.index = df_basins["BCU_name"].to_list()
    df = df.stack().reset_index()
    df = df.rename(columns={"level_0": "Region", "level_1": "years", 0: "value"})
    df = df.fillna(0)
    df = df.reset_index(drop=True)
    match time_mode:
        case "annual":
            df["year"] = pd.DatetimeIndex(df["years"]).year
            df["time"] = "year"
        case "subannual":
            df = df.sort_values(["Region", "years", "value"])
            df["year"] = pd.DatetimeIndex(df["years"]).year
            df["time"] = pd.DatetimeIndex(df["years"]).month
        case _:
            raise ValueError(f"unknown time mode {time_mode}")
    # duplicate 2100 as 2110 and filter relevant years
    df_patch = df[df["year"] == 2100].copy()
    df_patch["year"] = 2110
    df = pd.concat([df, df_patch])
    df = df[df["year"].isin(info.Y)]
    return df

def read_water_availability(context: "Context") -> tuple[pd.DataFrame, pd.DataFrame]:
    # read water availability data in an imperative style
    info = context["water build info"]
    df_basins = _read_basins(context)
    time_mode = "annual" if "year" in context.time else "subannual"
    suffix = "" if time_mode == "annual" else "_m"
    # process surface water data
    path_sw = package_data_path("water", "availability", f"qtot_5y{suffix}_{context.RCP}_{context.REL}_{context.regions}.csv")
    df_sw = pd.read_csv(path_sw)
    df_sw = process_availability_data(df_sw, df_basins, time_mode, info)
    # process groundwater data
    path_gw = package_data_path("water", "availability", f"qr_5y{suffix}_{context.RCP}_{context.REL}_{context.regions}.csv")
    df_gw = pd.read_csv(path_gw)
    df_gw = process_availability_data(df_gw, df_basins, time_mode, info)
    return df_sw, df_gw

def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    # compile water supply constraints using the imperative transforms
    results = {}
    df_sw, df_gw = read_water_availability(context)
    dmd_sw = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=-df_sw["value"],
        unit="km3/year",
    )
    dmd_gw = make_df(
        "demand",
        node="B" + df_gw["Region"].astype(str),
        commodity="groundwater_basin",
        level="water_avail_basin",
        year=df_gw["year"],
        time=df_gw["time"],
        value=-df_gw["value"],
        unit="km3/year",
    )
    dmd_df = pd.concat([dmd_sw, dmd_gw])
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)
    results["demand"] = dmd_df
    share_val = (df_gw["value"].abs() / (df_sw["value"].abs() + df_gw["value"].abs())) * BUFFER_FACTOR
    df_share = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + df_gw["Region"].astype(str),
        year_act=df_gw["year"],
        time=df_gw["time"],
        value=share_val,
        unit="-",
    )
    df_share["value"] = df_share["value"].fillna(0)
    results["share_commodity_lo"] = df_share
    return results

def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    # process irrigation water demand from globiom
    results = {}
    scen = context.get_scenario()
    land_out_1 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"})
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"})
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"})
    land_out_3["level"] = "irr_sugarcrops"
    land_out = pd.concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"
    land_out["value"] = 1e-3 * land_out["value"]
    results["land_input"] = land_out
    return results