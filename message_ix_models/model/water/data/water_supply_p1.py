"""Prepare data for water use for cooling & energy technologies."""

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
    """
    Calculate share of water availability of basins per each parent region.

    The parent region could be global message regions or country

    Parameters
    ----------
        context : .Context

    Returns
    -------
        data : pandas.DataFrame
    """
    info = context["water build info"]

    if "year" in context.time:
        PATH = package_data_path(
            "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
        )
        df_x = pd.read_csv(PATH)
        # Adding freshwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
        )

        df_sw = pd.read_csv(path1)
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)

        # Reading data, the data is spatially and temporally aggregated from GHMs
        df_sw["BCU_name"] = df_x["BCU_name"]
        df_sw["MSGREG"] = (
            context.map_ISO_c[context.regions]
            if context.type_reg == "country"
            else f"{context.regions}_" + df_sw["BCU_name"].str.split("|").str[-1]
        )

        df_sw = df_sw.set_index(["MSGREG", "BCU_name"])

        # Calculating ratio of water availability in basin by region
        df_sw = df_sw.groupby(["MSGREG"]).apply(lambda x: x / x.sum())
        df_sw.reset_index(level=0, drop=True, inplace=True)
        df_sw.reset_index(inplace=True)
        df_sw["Region"] = "B" + df_sw["BCU_name"].astype(str)
        df_sw["Mode"] = df_sw["Region"].replace(regex=["^B"], value="M")
        df_sw.drop(columns=["BCU_name"], inplace=True)
        df_sw.set_index(["MSGREG", "Region", "Mode"], inplace=True)
        df_sw = df_sw.stack().reset_index(level=0).reset_index()
        df_sw.columns = pd.Index(["region", "mode", "date", "MSGREG", "share"])
        df_sw.sort_values(["region", "date", "MSGREG", "share"], inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["date"]).year
        df_sw["time"] = "year"
        df_sw = df_sw[df_sw["year"].isin(info.Y)]
        df_sw.reset_index(drop=True, inplace=True)

    else:
        # add water return flows for cooling tecs
        # Use share of basin availability to distribute the return flow from
        path3 = package_data_path(
            "water",
            "availability",
            f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        df_sw = pd.read_csv(path3)

        # reading sample for assiging basins
        PATH = package_data_path(
            "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
        )
        df_x = pd.read_csv(PATH)

        # Reading data, the data is spatially and temporally aggregated from GHMs
        df_sw["BCU_name"] = df_x["BCU_name"]

        df_sw["MSGREG"] = (
            context.map_ISO_c[context.regions]
            if context.type_reg == "country"
            else f"{context.regions}_" + df_sw["BCU_name"].str.split("|").str[-1]
        )

        df_sw = df_sw.set_index(["MSGREG", "BCU_name"])
        df_sw.drop(columns="Unnamed: 0", inplace=True)

        # Calculating ratio of water availability in basin by region
        df_sw = df_sw.groupby(["MSGREG"]).apply(lambda x: x / x.sum())
        df_sw.reset_index(level=0, drop=True, inplace=True)
        df_sw.reset_index(inplace=True)
        df_sw["Region"] = "B" + df_sw["BCU_name"].astype(str)
        df_sw["Mode"] = df_sw["Region"].replace(regex=["^B"], value="M")
        df_sw.drop(columns=["BCU_name"], inplace=True)
        df_sw.set_index(["MSGREG", "Region", "Mode"], inplace=True)
        df_sw = df_sw.stack().reset_index(level=0).reset_index()
        df_sw.columns = pd.Index(["node", "mode", "date", "MSGREG", "share"])
        df_sw.sort_values(["node", "date", "MSGREG", "share"], inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["date"]).year
        df_sw["time"] = pd.DatetimeIndex(df_sw["date"]).month
        df_sw = df_sw[df_sw["year"].isin(info.Y)]
        df_sw.reset_index(drop=True, inplace=True)

    return df_sw



