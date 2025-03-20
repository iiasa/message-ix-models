"""prepare data for water use for cooling & energy technologies."""

import numpy as np
import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models import Context
from message_ix_models.model.water.data.demands import read_water_availability
from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    broadcast,
    minimum_version,
    package_data_path,
    same_node,
    same_time,
)

@minimum_version("message_ix 3.7")
def map_basin_region_wat(context: "Context") -> pd.DataFrame:
    """calculate water share for basins by region"""
    info = context["water build info"]
    # read delineation file
    path = package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv")
    df_x = pd.read_csv(path)
    return _process_common(context, df_x, info)

def _process_common(context: "Context", df_x: pd.DataFrame, info) -> pd.DataFrame:
    # use pattern matching on context.time to determine if processing should be annual or monthly
    match context.time:
        case t if "year" in t:
            freq = "annual"
        case _:
            freq = "monthly"
    
    # set file suffix based on frequency; monthly has an additional _m suffix
    file_suffix = "" if freq == "annual" else "_m"
    # construct qtot file path based on frequency
    path_qtot = package_data_path("water", "availability", f"qtot_5y{file_suffix}_{context.RCP}_{context.REL}_{context.regions}.csv")
    df_sw = pd.read_csv(path_qtot)
    
    if freq == "annual":
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)
    else:
        # for monthly, re-read delineation to ensure consistency and drop the unnamed column later
        path_delineation = package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv")
        df_x = pd.read_csv(path_delineation)
        df_sw.drop(columns=["Unnamed: 0"], inplace=True)
    
    # assign basin names and determine region mapping
    df_sw["BCU_name"] = df_x["BCU_name"]
    df_sw["MSGREG"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_sw["BCU_name"].str.split("|").str[-1]
    )
    df_sw = df_sw.set_index(["MSGREG", "BCU_name"])
    df_sw = df_sw.groupby("MSGREG").apply(lambda x: x / x.sum())
    df_sw.reset_index(level=0, drop=True, inplace=True)
    df_sw.reset_index(inplace=True)
    df_sw["Region"] = "B" + df_sw["BCU_name"].astype(str)
    df_sw["Mode"] = df_sw["Region"].replace(regex=["^B"], value="M")
    df_sw.drop(columns=["BCU_name"], inplace=True)
    df_sw.set_index(["MSGREG", "Region", "Mode"], inplace=True)
    df_sw = df_sw.stack().reset_index(level=0).reset_index()
    
    # rename columns and set time information based on frequency
    if freq == "annual":
        df_sw.columns = pd.Index(["region", "mode", "date", "MSGREG", "share"])
    else:
        df_sw.columns = pd.Index(["node", "mode", "date", "MSGREG", "share"])
    
    sort_key = "region" if freq == "annual" else "node"
    df_sw.sort_values([sort_key, "date", "MSGREG", "share"], inplace=True)
    df_sw["year"] = pd.DatetimeIndex(df_sw["date"]).year
    df_sw["time"] = "year" if freq == "annual" else pd.DatetimeIndex(df_sw["date"]).month
    df_sw = df_sw[df_sw["year"].isin(info.Y)]
    df_sw.reset_index(drop=True, inplace=True)
    return df_sw

    