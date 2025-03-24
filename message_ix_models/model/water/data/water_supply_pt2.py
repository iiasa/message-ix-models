
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
    # scen = context.get_scenario()
    scen = Scenario(context.get_platform(), **context.core.scenario_info)

    # year_wat = (2010, 2015)
    fut_year = info.Y
    year_wat = (2010, 2015, *info.Y)
    sub_time = context.time
    print(sub_time)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

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
                unit="km3",
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
                    unit="km3",
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
                    technology="extract_salinewater",
                    value=1,
                    unit="km3",
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

    elif context.nexus_set == "nexus":
        # input data frame  for slack technology balancing equality with demands
        inp = (
            make_df(
                "input",
                technology="return_flow",
                value=1,
                unit="-",
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
                    unit="-",
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
                    technology="basin_to_reg",
                    value=1,
                    unit="-",
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
        #         unit="-",
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
        yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)

        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_surfacewater",
                    value=1,
                    unit="-",
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
        yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)
        inp = pd.concat(
            [
                inp,
                make_df(
                    "input",
                    technology="extract_groundwater",
                    value=1,
                    unit="-",
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
                    value=0.018835616,
                    unit="-",
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
                    value=df_gwt["GW_per_km3_per_year"] + 0.043464579,
                    unit="-",
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
                    value=(df_gwt["GW_per_km3_per_year"] + 0.043464579)
                    * 2,  # twice as much normal gw
                    unit="-",
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
                unit="-",
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
                    unit="-",
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
                    unit="-",
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
                    technology="extract_salinewater",
                    value=1,
                    unit="km3",
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

        hist_new_cap = make_df(
            "historical_new_capacity",
            node_loc=df_hist["BCU_name"],
            technology="extract_surfacewater",
            value=df_hist["hist_cap_sw_km3_year"] / 5,  # n period
            unit="km3/year",
            year_vtg=2015,
        )

        hist_new_cap = pd.concat(
            [
                hist_new_cap,
                make_df(
                    "historical_new_capacity",
                    node_loc=df_hist["BCU_name"],
                    technology="extract_groundwater",
                    value=df_hist["hist_cap_gw_km3_year"] / 5,
                    unit="km3/year",
                    year_vtg=2015,
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
                    technology="basin_to_reg",
                    value=1,
                    unit="-",
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
            technology="basin_to_reg",
            mode=df_node["mode"],
            node_loc=df_node["region"],
            value=20,
            unit="-",
        ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time))
        var["year_act"] = var["year_vtg"]
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
            technology="basin_to_reg",
            mode=df_sw["mode"],
            node_share=df_sw["MSGREG"],
            time=df_sw["time"],
            value=df_sw["share"],
            unit="%",
            year_act=df_sw["year"],
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
                    value=20,
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
            value=155.57,
            unit="USD/km3",
        ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])

        inv_cost = pd.concat(
            [
                inv_cost,
                make_df(
                    "inv_cost",
                    technology="extract_groundwater",
                    value=54.52,
                    unit="USD/km3",
                ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"]),
            ]
        )

        inv_cost = pd.concat(
            [
                inv_cost,
                make_df(
                    "inv_cost",
                    technology="extract_gw_fossil",
                    value=54.52 * 150,  # assume higher as normal GW
                    unit="USD/km3",
                ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"]),
            ]
        )

        results["inv_cost"] = inv_cost

        fix_cost = make_df(
            "fix_cost",
            technology="extract_gw_fossil",
            value=300,  # assumed
            unit="USD/km3",
        ).pipe(broadcast, yv_ya_gw, node_loc=df_node["node"])

        results["fix_cost"] = fix_cost

    return results
