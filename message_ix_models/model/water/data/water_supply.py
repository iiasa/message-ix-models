"""Prepare data for water use for cooling & energy technologies."""

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.water.data.demands import read_water_availability
from message_ix_models.model.water.utils import (
    ANNUAL_CAPACITY_FACTOR,
    KM3_TO_MCM,
    USD_KM3_TO_USD_MCM,
    GWa_KM3_TO_GWa_MCM,
    get_vintage_and_active_years,
)
from message_ix_models.util import (
    broadcast,
    package_data_path,
    same_node,
    same_time,
)


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


def add_water_supply(context: "Context") -> dict[str, pd.DataFrame]:
    """Add Water supply infrastructure
    This function links the water supply based on different settings and options.
    It defines the supply linkages for freshwater, groundwater and salinewater.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """
    # define an empty dictionary
    results = {}

    # Reference to the water configuration
    info = context["water build info"]
    # load the scenario from context
    scen = context.get_scenario()

    fut_year = info.Y
    year_wat = (*range(2010, info.Y[0] + 1, 5), *info.Y)
    last_vtg_yr = info.Y[0] - 5
    sub_time = context.time
    scen_info = ScenarioInfo(scen)

    print(" future year = ", fut_year)
    print(" year_wat = ", year_wat)

    # reading basin_delineation
    FILE = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    # Storing the energy MESSAGE region names
    node_region = df_node["region"].unique()

    # reading groundwater energy intensity data
    FILE1 = f"gw_energy_intensity_depth_{context.regions}.csv"
    PATH1 = package_data_path("water", "availability", FILE1)
    df_gwt = pd.read_csv(PATH1)
    df_gwt["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_gwt["REGION"].astype(str)
    )

    # reading groundwater energy intensity data
    FILE2 = f"historical_new_cap_gw_sw_km3_year_{context.regions}.csv"
    PATH2 = package_data_path("water", "availability", FILE2)
    df_hist = pd.read_csv(PATH2)
    df_hist["BCU_name"] = "B" + df_hist["BCU_name"].astype(str)

    if context.nexus_set == "cooling":
        # Add output df  for surfacewater supply for regions
        output_df = (
            make_df(
                "output",
                technology="extract_surfacewater",
                value=1,
                unit="MCM",
                year_vtg=year_wat,
                year_act=year_wat,
                level="water_supply",
                commodity="freshwater",
                mode="M1",
                time="year",
                time_dest="year",
                time_origin="year",
            )
            .pipe(broadcast, node_loc=node_region)
            .pipe(same_node)
        )

        # Add output df  for groundwater supply for regions
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="extract_groundwater",
                    value=1,
                    unit="MCM",
                    year_vtg=year_wat,
                    year_act=year_wat,
                    level="water_supply",
                    commodity="freshwater",
                    mode="M1",
                    time="year",
                    time_dest="year",
                    time_origin="year",
                )
                .pipe(broadcast, node_loc=node_region)
                .pipe(same_node),
            ]
        )
        # Add output of saline water supply for regions
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="extract_salinewater_cool",
                    value=1,
                    unit="MCM",
                    year_vtg=year_wat,
                    year_act=year_wat,
                    level="saline_supply",
                    commodity="saline_ppl",
                    mode="M1",
                    time="year",
                    time_dest="year",
                    time_origin="year",
                )
                .pipe(broadcast, node_loc=node_region)
                .pipe(same_node),
            ]
        )
        results["output"] = output_df

        # Electricity input for extract_surfacewater
        # Value: 0.018835616 GWa/km3 (mid estimate from literature)
        inp_sw = (
            make_df(
                "input",
                technology="extract_surfacewater",
                value=0.018835616 * GWa_KM3_TO_GWa_MCM,
                unit="GWa/MCM",
                level="final",
                commodity="electr",
                mode="M1",
                time="year",
                time_origin="year",
                year_vtg=year_wat,
                year_act=year_wat,
            )
            .pipe(broadcast, node_loc=node_region)
            .pipe(same_node)
        )

        # Electricity input for extract_groundwater (depth-dependent + fixed)
        inp_gw = (
            make_df(
                "input",
                technology="extract_groundwater",
                value=(df_gwt["GW_per_km3_per_year"].mean() + 0.043464579)
                * GWa_KM3_TO_GWa_MCM,
                unit="GWa/MCM",
                level="final",
                commodity="electr",
                mode="M1",
                time="year",
                time_origin="year",
                year_vtg=year_wat,
                year_act=year_wat,
            )
            .pipe(broadcast, node_loc=node_region)
            .pipe(same_node)
        )

        results["input"] = pd.concat([inp_sw, inp_gw], ignore_index=True)

    elif context.nexus_set == "nexus":
        # input data frame  for slack technology balancing equality with demands
        inp = (
            make_df(
                "input",
                technology="return_flow",
                value=1,
                unit="MCM",
                level="water_avail_basin",
                commodity="surfacewater_basin",
                mode="M1",
                year_vtg=year_wat,
                year_act=year_wat,
            )
            .pipe(
                broadcast,
                node_loc=df_node["node"],
                time=pd.Series(sub_time),
            )
            .pipe(same_node)
            .pipe(same_time)
        )

        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="gw_recharge",
                    value=1,
                    unit="MCM",
                    level="water_avail_basin",
                    commodity="groundwater_basin",
                    mode="M1",
                    year_vtg=year_wat,
                    year_act=year_wat,
                )
                .pipe(
                    broadcast,
                    node_loc=df_node["node"],
                    time=pd.Series(sub_time),
                )
                .pipe(same_node)
                .pipe(same_time),
            ]
        )

        # input dataframe  linking water supply to energy dummy technology
        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="basin_to_reg_core",
                    value=1,
                    unit="MCM",
                    level="water_avail_basin",
                    commodity="surfacewater_basin",
                    mode=df_node["mode"],
                    node_origin=df_node["node"],
                    node_loc=df_node["region"],
                )
                .pipe(
                    broadcast,
                    year_vtg=year_wat,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )

        # input dataframe for basin_to_reg_plus technology (irrigation freshwater)
        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="basin_to_reg_plus",
                    value=1,
                    unit="MCM",
                    level="water_supply_basin",
                    commodity="freshwater_basin",
                    mode=df_node["mode"],
                    node_origin=df_node["node"],
                    node_loc=df_node["region"],
                )
                .pipe(
                    broadcast,
                    year_vtg=year_wat,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )
        inp["year_act"] = inp["year_vtg"]
        # # input data frame  for slack technology balancing equality with demands
        # inp = pd.concat([inp,
        #     make_df(
        #         "input",
        #         technology="salinewater_return",
        #         value=1,
        #         unit="MCM",
        #         level="water_avail_basin",
        #         commodity="salinewater_basin",
        #         mode="M1",
        #         time="year",
        #         time_origin="year",
        #         node_origin=df_node["node"],
        #         node_loc=df_node["node"],
        #     ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat)
        # ])

        # input data frame  for freshwater supply
        # Use first basin node for vintage-activity year mapping
        yv_ya_sw = get_vintage_and_active_years(scen_info, 20)

        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_surfacewater",
                    value=1,
                    unit="MCM",
                    level="water_avail_basin",
                    commodity="surfacewater_basin",
                    mode="M1",
                    node_origin=df_node["node"],
                    node_loc=df_node["node"],
                )
                .pipe(
                    broadcast,
                    yv_ya_sw,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )

        # input dataframe  for groundwater supply
        # Use first basin node for vintage-activity year mapping
        yv_ya_gw = get_vintage_and_active_years(scen_info, 20)
        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_groundwater",
                    value=1,
                    unit="MCM",
                    level="water_avail_basin",
                    commodity="groundwater_basin",
                    mode="M1",
                    node_origin=df_node["node"],
                    node_loc=df_node["node"],
                )
                .pipe(
                    broadcast,
                    yv_ya_gw,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )

        # electricity input dataframe  for extract freshwater supply
        # low: 0.001141553, mid: 0.018835616, high: 0.03652968
        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_surfacewater",
                    value=0.018835616 * GWa_KM3_TO_GWa_MCM,
                    unit="GWa/MCM",
                    level="final",
                    commodity="electr",
                    mode="M1",
                    time_origin="year",
                    node_origin=df_node["region"],
                    node_loc=df_node["node"],
                ).pipe(
                    broadcast,
                    yv_ya_sw,
                    time=pd.Series(sub_time),
                ),
            ]
        )

        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_groundwater",
                    value=(df_gwt["GW_per_km3_per_year"] + 0.043464579)
                    * GWa_KM3_TO_GWa_MCM,
                    unit="GWa/MCM",
                    level="final",
                    commodity="electr",
                    mode="M1",
                    time_origin="year",
                    node_origin=df_node["region"],
                    node_loc=df_node["node"],
                ).pipe(
                    broadcast,
                    yv_ya_gw,
                    time=pd.Series(sub_time),
                ),
            ]
        )

        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_gw_fossil",
                    value=((df_gwt["GW_per_km3_per_year"] + 0.043464579) * 5)
                    * GWa_KM3_TO_GWa_MCM,  # reduced from 50 to 5
                    unit="GWa/MCM",
                    level="final",
                    commodity="electr",
                    mode="M1",
                    time_origin="year",
                    node_origin=df_node["region"],
                    node_loc=df_node["node"],
                ).pipe(
                    broadcast,
                    yv_ya_gw,
                    time=pd.Series(sub_time),
                ),
            ]
        )

        if context.type_reg == "global":
            inp.loc[
                (inp["technology"].str.contains("extract_gw_fossil"))
                & (inp["year_act"] == 2020)
                & (inp["node_loc"] == "R11_SAS"),
                "value",
            ] *= 0.5

        results["input"] = inp

        # Add output df  for freshwater supply for basins
        output_df = (
            make_df(
                "output",
                technology="extract_surfacewater",
                value=1,
                unit="MCM",
                level="water_supply_basin",
                commodity="freshwater_basin",
                mode="M1",
                node_loc=df_node["node"],
                node_dest=df_node["node"],
            )
            .pipe(
                broadcast,
                yv_ya_sw,
                time=pd.Series(sub_time),
            )
            .pipe(same_time)
        )
        # Add output df  for groundwater supply for basins
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="extract_groundwater",
                    value=1,
                    unit="MCM",
                    level="water_supply_basin",
                    commodity="freshwater_basin",
                    mode="M1",
                    node_loc=df_node["node"],
                    node_dest=df_node["node"],
                )
                .pipe(
                    broadcast,
                    yv_ya_gw,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )

        # Add output df  for groundwater supply for basins
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="extract_gw_fossil",
                    value=1,
                    unit="MCM",
                    level="water_supply_basin",
                    commodity="freshwater_basin",
                    mode="M1",
                    node_loc=df_node["node"],
                    node_dest=df_node["node"],
                    time_origin="year",
                )
                .pipe(
                    broadcast,
                    yv_ya_gw,
                    time=pd.Series(sub_time),
                )
                .pipe(same_time),
            ]
        )

        # Add output of saline water supply for regions
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="extract_salinewater_cool",
                    value=1,
                    unit="MCM",
                    year_vtg=year_wat,
                    year_act=year_wat,
                    level="saline_supply",
                    commodity="saline_ppl",
                    mode="M1",
                    time="year",
                    time_dest="year",
                    time_origin="year",
                )
                .pipe(broadcast, node_loc=node_region)
                .pipe(same_node),
            ]
        )

        # Technical lifetime for extract_salinewater_cool
        saline_water_cool_tl = make_df(
            "technical_lifetime",
            technology="extract_salinewater_cool",
            value=1,
            unit="y",
        ).pipe(broadcast, node_loc=node_region, year_vtg=year_wat)

        # Investment cost for extract_salinewater_cool
        saline_water_cool_inv_cost = make_df(
            "inv_cost",
            technology="extract_salinewater_cool",
            value=1e-2,
            unit="USD/MCM",
        ).pipe(broadcast, node_loc=node_region, year_vtg=year_wat)

        hist_new_cap = make_df(
            "historical_new_capacity",
            node_loc=df_hist["BCU_name"],
            technology="extract_surfacewater",
            value=KM3_TO_MCM
            * df_hist["hist_cap_sw_km3_year"]
            / ANNUAL_CAPACITY_FACTOR,  # n period
            unit="MCM/year",
            year_vtg=last_vtg_yr,
        )

        hist_new_cap = pd.concat(
            [
                hist_new_cap,
                make_df(
                    "historical_new_capacity",
                    node_loc=df_hist["BCU_name"],
                    technology="extract_groundwater",
                    value=KM3_TO_MCM
                    * df_hist["hist_cap_gw_km3_year"]
                    / ANNUAL_CAPACITY_FACTOR,
                    unit="MCM/year",
                    year_vtg=last_vtg_yr,
                ),
            ]
        )

        results["historical_new_capacity"] = hist_new_cap

        # output data frame linking water supply to energy dummy technology
        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="basin_to_reg_core",
                    value=1,
                    unit="MCM",
                    level="water_supply",
                    commodity="surfacewater",
                    time_dest="year",
                    node_loc=df_node["region"],
                    node_dest=df_node["region"],
                    mode=df_node["mode"],
                ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time)),
            ]
        )

        # output data frame for basin_to_reg_plus technology (irrigation freshwater)

        output_df = pd.concat(
            [
                output_df,
                make_df(
                    "output",
                    technology="basin_to_reg_plus",
                    value=1,
                    unit="MCM",
                    level="water_supply",
                    commodity="freshwater",
                    time_dest="year",
                    node_loc=df_node["region"],
                    node_dest=df_node["region"],
                    mode=df_node["mode"],
                ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time)),
            ]
        )

        output_df["year_act"] = output_df["year_vtg"]

        results["output"] = output_df

        # dummy variable cost for dummy water to energy technology
        var = make_df(
            "var_cost",
            technology="basin_to_reg_core",
            mode=df_node["mode"],
            node_loc=df_node["region"],
            value=20 * USD_KM3_TO_USD_MCM,
            unit="USD/MCM",
        ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time))
        var["year_act"] = var["year_vtg"]

        # dummy variable cost for basin_to_reg_plus technology (irrigation freshwater)
        var = pd.concat(
            [
                var,
                make_df(
                    "var_cost",
                    technology="basin_to_reg_plus",
                    mode=df_node["mode"],
                    node_loc=df_node["region"],
                    value=20 * USD_KM3_TO_USD_MCM,
                    unit="USD/MCM",
                ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time)),
            ]
        )
        basin_to_reg_plus_mask = var["technology"] == "basin_to_reg_plus"
        var.loc[basin_to_reg_plus_mask, "year_act"] = var.loc[
            basin_to_reg_plus_mask, "year_vtg"
        ]
        # # Dummy cost for extract surface ewater to prioritize water sources
        # var = pd.concat([var, make_df(
        #     "var_cost",
        #     technology='extract_surfacewater',
        #     value= 0.0001,
        #     unit="USD/km3",
        #     mode="M1",
        #     time="year",
        #     ).pipe(broadcast, year_vtg=year_wat,
        #       year_act=year_wat, node_loc=df_node["node"]
        #        )
        #                  ])
        # # Dummy cost for extract groundwater
        # var = pd.concat([var, make_df(
        #     "var_cost",
        #     technology='extract_groundwater',
        #     value= 0.001,
        #     unit="USD/km3",
        #     mode="M1",
        #     time="year",
        # ).pipe(broadcast, year_vtg=year_wat,
        #   year_act=year_wat, node_loc=df_node["node"]
        # ])
        #                )
        results["var_cost"] = var

        # load the share of sw
        df_sw = map_basin_region_wat(context)

        share = make_df(
            "share_mode_up",
            shares="share_basin",
            technology="basin_to_reg_core",
            mode=df_sw["mode"],
            node_share=df_sw["MSGREG"],
            time=df_sw["time"],
            value=df_sw["share"],
            unit="%",
            year_act=df_sw["year"],
        )

        # share_mode_up for basin_to_reg_plus technology (irrigation freshwater)
        share = pd.concat(
            [
                share,
                make_df(
                    "share_mode_up",
                    shares="share_basin",
                    technology="basin_to_reg_plus",
                    mode=df_sw["mode"],
                    node_share=df_sw["MSGREG"],
                    time=df_sw["time"],
                    value=df_sw["share"],
                    unit="%",
                    year_act=df_sw["year"],
                ),
            ]
        )

        results["share_mode_up"] = share

        tl = (
            make_df(
                "technical_lifetime",
                technology="extract_surfacewater",
                value=50,
                unit="y",
            )
            .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
            .pipe(same_node)
        )

        tl = pd.concat(
            [
                tl,
                make_df(
                    "technical_lifetime",
                    technology="extract_groundwater",
                    value=20,
                    unit="y",
                )
                .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
                .pipe(same_node),
            ]
        )

        tl = pd.concat(
            [
                tl,
                make_df(
                    "technical_lifetime",
                    technology="extract_gw_fossil",
                    value=5,  # 5 Year TL to further discourage use.
                    unit="y",
                )
                .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
                .pipe(same_node),
            ]
        )

        results["technical_lifetime"] = tl

        # Prepare dataframe for investments
        inv_cost = make_df(
            "inv_cost",
            technology="extract_surfacewater",
            value=155.57 * USD_KM3_TO_USD_MCM,
            unit="USD/MCM",
        ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])

        inv_cost = pd.concat(
            [
                inv_cost,
                make_df(
                    "inv_cost",
                    technology="extract_groundwater",
                    value=54.52 * USD_KM3_TO_USD_MCM,
                    unit="USD/MCM",
                ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"]),
            ]
        )

        inv_cost = pd.concat(
            [
                inv_cost,
                make_df(
                    "inv_cost",
                    technology="extract_gw_fossil",
                    value=7808.22 * USD_KM3_TO_USD_MCM,
                    # 50% higher than membrane desalination (5205.48 * 1.5)
                    unit="USD/MCM",
                ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"]),
            ]
        )

        results["inv_cost"] = inv_cost
        results["technical_lifetime"] = pd.concat(
            [results["technical_lifetime"], saline_water_cool_tl]
        )
        results["inv_cost"] = pd.concat(
            [results["inv_cost"], saline_water_cool_inv_cost]
        )
        fix_cost = make_df(
            "fix_cost",
            technology="extract_gw_fossil",
            value=6780.83 * USD_KM3_TO_USD_MCM,
            # 50% higher than distillation fix_cost (4520.55 * 1.5)
            unit="USD/MCM",
        ).pipe(broadcast, yv_ya_gw, node_loc=df_node["node"])

        results["fix_cost"] = fix_cost

        # Add variable cost for fossil groundwater to make it truly expensive
        var_cost_fossil = make_df(
            "var_cost",
            technology="extract_gw_fossil",
            value=1000 * USD_KM3_TO_USD_MCM,
            # High variable cost to ensure it's only used as last resort
            unit="USD/MCM",
            mode="M1",
        ).pipe(
            broadcast,
            yv_ya_gw,
            node_loc=df_node["node"],
            time=pd.Series(sub_time),
        )

        if "var_cost" in results:
            results["var_cost"] = pd.concat([results["var_cost"], var_cost_fossil])
        else:
            results["var_cost"] = var_cost_fossil

        # Add growth constraint for extract_surfacewater (10% annual growth limit)
        growth_activity_up = make_df(
            "growth_activity_up",
            technology="extract_surfacewater",
            value=0.02,
            unit="-",
        ).pipe(
            broadcast,
            year_act=year_wat,
            node_loc=df_node["node"],
            time=pd.Series(sub_time),
        )

        results["growth_activity_up"] = growth_activity_up

        # Remove duplicates and NaN values from all DataFrames in results
        for key, df in results.items():
            results[key] = df.dropna().drop_duplicates().reset_index(drop=True)
    return results


def add_e_flow(context: "Context") -> dict[str, pd.DataFrame]:
    """Add environmental flows
    This function bounds the available water and allocates the environmental
    flows.Environmental flow bounds are calculated using Variable Monthly Flow
    (VMF) method. The VMF method is applied to wet and dry seasonal runoff
    values. These wet and dry seasonal values are then aggregated to annual
    values.Environmental flows in the model will be incorporated as bounds on
    'return_flow' technology. The lower bound on this technology will ensure
    that certain amount of water remain

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """
    # define an empty dictionary
    results = {}

    info = context["water build info"]

    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs
    df_sw, df_gw = read_water_availability(context)

    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=df_sw["value"] * KM3_TO_MCM,
        unit="MCM/year",
    )
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    if "year" in context.time:
        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"e-flow_{context.RCP}_{context.regions}.csv",
        )
        df_env = pd.read_csv(path1)
        df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
        df_env.index = df_x["BCU_name"].index
        df_env = df_env.stack().reset_index()
        df_env.columns = pd.Index(["Region", "years", "value"])
        df_env.sort_values(["Region", "years", "value"], inplace=True)
        df_env.fillna(0, inplace=True)
        df_env.reset_index(drop=True, inplace=True)
        df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
        df_env["time"] = "year"
        df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
        df_env2210 = df_env[df_env["year"] == 2100].copy()
        df_env2210["year"] = 2110
        df_env = pd.concat([df_env, df_env2210])
        df_env = df_env[df_env["year"].isin(info.Y)]
    else:
        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"e-flow_5y_m_{context.RCP}_{context.regions}.csv",
        )
        df_env = pd.read_csv(path1)
        df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
        # new_cols = pd.to_datetime(df_env.columns, format="%Y/%m/%d")
        # df_env.columns = new_cols
        df_env.index = df_x["BCU_name"].index
        df_env = df_env.stack().reset_index()
        df_env.columns = pd.Index(["Region", "years", "value"])
        df_env.sort_values(["Region", "years", "value"], inplace=True)
        df_env.fillna(0, inplace=True)
        df_env.reset_index(drop=True, inplace=True)
        df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
        df_env["time"] = pd.DatetimeIndex(df_env["years"]).month
        df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
        df_env2210 = df_env[df_env["year"] == 2100].copy()
        df_env2210["year"] = 2110
        df_env = pd.concat([df_env, df_env2210])
        df_env = df_env[df_env["year"].isin(info.Y)]

    # Return a processed dataframe for env flow calculations
    if context.SDG != "baseline":
        # dataframe to put constraints on env flows
        eflow_df = make_df(
            "bound_activity_lo",
            node_loc="B" + df_env["Region"].astype(str),
            technology="return_flow",
            year_act=df_env["year"],
            mode="M1",
            time=df_env["time"],
            value=df_env["value"] * KM3_TO_MCM,
            unit="MCM/year",
        )

        eflow_df["value"] = eflow_df["value"].apply(lambda x: x if x >= 0 else 0)
        eflow_df = eflow_df[eflow_df["year_act"] >= 2025].reset_index(drop=True)

        dmd_df.sort_values(by=["node", "year"], inplace=True)
        dmd_df.reset_index(drop=True, inplace=True)
        eflow_df.sort_values(by=["node_loc", "year_act"], inplace=True)
        eflow_df.reset_index(drop=True, inplace=True)

        eflow_df["value"] = np.where(
            eflow_df["value"] >= 0.7 * dmd_df["value"],
            0.7 * dmd_df["value"],
            eflow_df["value"],
        )

        results["bound_activity_lo"] = eflow_df

    return results
