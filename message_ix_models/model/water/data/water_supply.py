"""Prepare data for water use for cooling & energy technologies."""

import numpy as np
import pandas as pd
from message_ix import make_df
from message_ix_models.util import broadcast, private_data_path, same_node

from message_data.model.water.utils import map_yv_ya_lt


def add_water_supply(context):
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

    year_wat = [2010, 2015]
    fut_year = info.Y
    year_wat.extend(info.Y)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    print(" future year = ", fut_year)
    print(" year_wat = ", year_wat)

    # reading basin_delineation
    FILE = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = private_data_path("water", "delineation", FILE)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = f"{context.regions}_" + df_node["REGION"].astype(str)

    # Storing the energy MESSAGE region names
    node_region = df_node["region"].unique()

    # reading groundwater energy intensity data
    FILE1 = f"gw_energy_intensity_depth_{context.regions}.csv"
    PATH1 = private_data_path("water", "water_availability", FILE1)
    df_gwt = pd.read_csv(PATH1)
    df_gwt["REGION"] = f"{context.regions}_" + df_gwt["REGION"].astype(str)

    # reading groundwater energy intensity data
    FILE2 = f"historical_new_cap_gw_sw_km3_year_{context.regions}.csv"
    PATH2 = private_data_path("water", "water_availability", FILE2)
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
        output_df = output_df.append(
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
            .pipe(same_node)
        )
        # Add output of saline water supply for regions
        output_df = output_df.append(
            make_df(
                "output",
                technology="extract_salinewater",
                value=1,
                unit="km3",
                year_vtg=year_wat,
                year_act=year_wat,
                level="water_supply",
                commodity="saline_ppl",
                mode="M1",
                time="year",
                time_dest="year",
                time_origin="year",
            )
            .pipe(broadcast, node_loc=node_region)
            .pipe(same_node)
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
                time="year",
                time_origin="year",
                year_vtg=year_wat,
                year_act=year_wat,
            )
            .pipe(broadcast, node_loc=df_node["node"])
            .pipe(same_node)
        )

        # input data frame  for slack technology balancing equality with demands
        inp = inp.append(
            make_df(
                "input",
                technology="gw_recharge",
                value=1,
                unit="-",
                level="water_avail_basin",
                commodity="groundwater_basin",
                mode="M1",
                time="year",
                time_origin="year",
                year_vtg=year_wat,
                year_act=year_wat,
            )
            .pipe(broadcast, node_loc=df_node["node"])
            .pipe(same_node)
        )

        # input dataframe  linking water supply to energy dummy technology
        inp = inp.append(
            make_df(
                "input",
                technology="basin_to_reg",
                value=1,
                unit="-",
                level="water_supply_basin",
                commodity="freshwater_basin",
                mode=df_node["mode"],
                time="year",
                time_origin="year",
                node_origin=df_node["node"],
                node_loc=df_node["region"],
            ).pipe(broadcast, year_vtg=year_wat)
        )
        inp["year_act"] = inp["year_vtg"]
        # # input data frame  for slack technology balancing equality with demands
        # inp = inp.append(
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
        # )

        # input data frame  for freshwater supply
        yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)

        inp = inp.append(
            make_df(
                "input",
                technology="extract_surfacewater",
                value=1,
                unit="-",
                level="water_avail_basin",
                commodity="surfacewater_basin",
                mode="M1",
                time="year",
                time_origin="year",
                node_origin=df_node["node"],
                node_loc=df_node["node"],
            ).pipe(broadcast, yv_ya_sw)
        )

        # input dataframe  for groundwater supply
        yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)
        inp = inp.append(
            make_df(
                "input",
                technology="extract_groundwater",
                value=1,
                unit="-",
                level="water_avail_basin",
                commodity="groundwater_basin",
                mode="M1",
                time="year",
                time_origin="year",
                node_origin=df_node["node"],
                node_loc=df_node["node"],
            ).pipe(broadcast, yv_ya_gw)
        )

        # electricity input dataframe  for extract freshwater supply
        # low: 0.001141553, mid: 0.018835616, high: 0.03652968
        inp = inp.append(
            make_df(
                "input",
                technology="extract_surfacewater",
                value=0.018835616,
                unit="-",
                level="final",
                commodity="electr",
                mode="M1",
                time="year",
                time_origin="year",
                node_origin=df_node["region"],
                node_loc=df_node["node"],
            ).pipe(broadcast, yv_ya_sw)
        )

        inp = inp.append(
            make_df(
                "input",
                technology="extract_groundwater",
                value=df_gwt["GW_per_km3_per_year"] + 0.043464579,
                unit="-",
                level="final",
                commodity="electr",
                mode="M1",
                time="year",
                time_origin="year",
                node_origin=df_gwt["REGION"],
                node_loc=df_node["node"],
            ).pipe(broadcast, yv_ya_gw)
        )

        inp = inp.append(
            make_df(
                "input",
                technology="extract_gw_fossil",
                value=(df_gwt["GW_per_km3_per_year"] + 0.043464579)
                * 2,  # twice as much normal gw
                unit="-",
                level="final",
                commodity="electr",
                mode="M1",
                time="year",
                time_origin="year",
                node_origin=df_gwt["REGION"],
                node_loc=df_node["node"],
            ).pipe(broadcast, yv_ya_gw)
        )

        inp.loc[
            (inp["technology"].str.contains("extract_gw_fossil"))
            & (inp["year_act"] == 2020)
            & (inp["node_loc"] == "R11_SAS"),
            "value",
        ] *= 0.5

        results["input"] = inp

        # Add output df  for freshwater supply for basins
        output_df = make_df(
            "output",
            technology="extract_surfacewater",
            value=1,
            unit="-",
            level="water_supply_basin",
            commodity="freshwater_basin",
            mode="M1",
            node_loc=df_node["node"],
            node_dest=df_node["node"],
            time="year",
            time_dest="year",
            time_origin="year",
        ).pipe(broadcast, yv_ya_sw)
        # Add output df  for groundwater supply for basins
        output_df = output_df.append(
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
                time="year",
                time_dest="year",
                time_origin="year",
            ).pipe(broadcast, yv_ya_gw)
        )

        # Add output df  for groundwater supply for basins
        output_df = output_df.append(
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
                time="year",
                time_dest="year",
                time_origin="year",
            ).pipe(broadcast, yv_ya_gw)
        )

        # Add output of saline water supply for regions
        output_df = output_df.append(
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
            .pipe(same_node)
        )

        hist_new_cap = make_df(
            "historical_new_capacity",
            node_loc=df_hist["BCU_name"],
            technology="extract_surfacewater",
            value=df_hist["hist_cap_sw_km3_year"] / 5,  # n period
            unit="km3/year",
            year_vtg=2015,
        )

        hist_new_cap = hist_new_cap.append(
            make_df(
                "historical_new_capacity",
                node_loc=df_hist["BCU_name"],
                technology="extract_groundwater",
                value=df_hist["hist_cap_gw_km3_year"] / 5,
                unit="km3/year",
                year_vtg=2015,
            )
        )

        results["historical_new_capacity"] = hist_new_cap

        # output data frame linking water supply to energy dummy technology
        output_df = output_df.append(
            make_df(
                "output",
                technology="basin_to_reg",
                value=1,
                unit="-",
                level="water_supply",
                commodity="freshwater",
                time="year",
                time_dest="year",
                time_origin="year",
                node_loc=df_node["region"],
                node_dest=df_node["region"],
                mode=df_node["mode"],
            ).pipe(broadcast, year_vtg=year_wat)
        )

        output_df["year_act"] = output_df["year_vtg"]

        results["output"] = output_df

        # dummy variable cost for dummy water to energy technology
        var = make_df(
            "var_cost",
            technology="basin_to_reg",
            mode=df_node["mode"],
            node_loc=df_node["region"],
            time="year",
            value=20,
            unit="-",
        ).pipe(broadcast, year_vtg=year_wat)
        var["year_act"] = var["year_vtg"]
        # # Dummy cost for extract surface ewater to prioritize water sources
        # var = var.append(make_df(
        #     "var_cost",
        #     technology='extract_surfacewater',
        #     value= 0.0001,
        #     unit="USD/km3",
        #     mode="M1",
        #     time="year",
        #     ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat, node_loc=df_node["node"]
        #        )
        #                  )
        # # Dummy cost for extract groundwater
        # var = var.append(make_df(
        #     "var_cost",
        #     technology='extract_groundwater',
        #     value= 0.001,
        #     unit="USD/km3",
        #     mode="M1",
        #     time="year",
        # ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat, node_loc=df_node["node"])
        #                )
        results["var_cost"] = var

        path1 = private_data_path(
            "water", "water_availability", f"qtot_{context.RCP}_{context.REL}.csv"
        )
        df_sw = pd.read_csv(path1)

        # reading sample for assiging basins
        PATH = private_data_path("water", "water_availability", "sample.csv")
        df_x = pd.read_csv(PATH)

        # Reading data, the data is spatially and temporally aggregated from GHMs
        df_sw["BCU_name"] = df_x["BCU_name"]
        df_sw["MSGREG"] = (
            f"{context.regions}_" + df_sw["BCU_name"].str[-3:]
        )  # R11 hard coded, TODO remove
        df_sw = df_sw.set_index(["MSGREG", "BCU_name"])
        df_sw.drop(columns="Unnamed: 0", inplace=True)

        years = list(range(2010, 2105, 5))
        df_sw.columns = years
        df_sw[2110] = df_sw[2100]
        df_sw.drop(columns=[2065, 2075, 2085, 2095], inplace=True)

        # Calculating ratio of water availability in basin by region
        df_sw = df_sw.groupby(["MSGREG"]).apply(lambda x: x / x.sum())
        df_sw.reset_index(inplace=True)
        df_sw["Region"] = "B" + df_sw["BCU_name"].astype(str)
        df_sw["Mode"] = df_sw["Region"].replace(regex=["^B"], value="M")
        df_sw.drop(columns=["BCU_name", "Region"], inplace=True)
        df_sw.set_index(["MSGREG", "Mode"], inplace=True)
        df_sw = df_sw.stack().reset_index(level=0).reset_index()
        df_sw.columns = ["Mode", "years", "Region", "value"]
        df_sw.sort_values(["Mode", "years", "Region", "value"], inplace=True)
        df_sw.fillna(0, inplace=True)
        df_sw.reset_index(drop=True, inplace=True)

        share = make_df(
            "share_mode_up",
            shares="share_basin",
            technology="basin_to_reg",
            mode=df_sw["Mode"],
            node_share=df_sw["Region"],
            time="year",
            value=df_sw["value"],
            unit="%",
            year_act=df_sw["years"],
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

        tl = tl.append(
            make_df(
                "technical_lifetime",
                technology="extract_groundwater",
                value=20,
                unit="y",
            )
            .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
            .pipe(same_node)
        )

        tl = tl.append(
            make_df(
                "technical_lifetime",
                technology="extract_gw_fossil",
                value=20,
                unit="y",
            )
            .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
            .pipe(same_node)
        )

        results["technical_lifetime"] = tl

        # Prepare dataframe for investments
        inv_cost = make_df(
            "inv_cost", technology="extract_surfacewater", value=155.57, unit="USD/km3",
        ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])

        inv_cost = inv_cost.append(
            make_df(
                "inv_cost",
                technology="extract_groundwater",
                value=54.52,
                unit="USD/km3",
            ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
        )

        inv_cost = inv_cost.append(
            make_df(
                "inv_cost",
                technology="extract_gw_fossil",
                value=54.52 * 150,  # assume higher as normal GW
                unit="USD/km3",
            ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
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


def add_e_flow(context):
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

    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs
    path1 = private_data_path(
        "water", "water_availability", f"qtot_{context.RCP}_{context.REL}.csv"
    )
    df_sw = pd.read_csv(path1)
    # reading sample for assiging basins
    PATH = private_data_path("water", "water_availability", "sample.csv")
    df_x = pd.read_csv(PATH)

    df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)
    years = list(range(2010, 2105, 5))
    df_sw.columns = years
    df_sw.index = df_x["BCU_name"]
    df_sw[2110] = df_sw[2100]
    df_sw.drop(columns=[2065, 2075, 2085, 2095], inplace=True)
    df_sw = df_sw.stack().reset_index()
    df_sw.columns = ["Region", "years", "value"]
    df_sw.sort_values(["Region", "years", "value"], inplace=True)
    df_sw.fillna(0, inplace=True)
    df_sw.reset_index(drop=True, inplace=True)

    # Reading data, the data is spatially and temporally aggregated from GHMs
    path1 = private_data_path(
        "water", "water_availability", f"qr_{context.RCP}_{context.REL}.csv"
    )
    df_gw = pd.read_csv(path1)

    df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)
    df_gw.columns = years
    df_gw.index = df_x["BCU_name"]
    df_gw[2110] = df_gw[2100]
    df_gw.drop(columns=[2065, 2075, 2085, 2095], inplace=True)
    df_gw = df_gw.stack().reset_index()
    df_gw.columns = ["Region", "years", "value"]
    df_gw.sort_values(["Region", "years", "value"], inplace=True)
    df_gw.fillna(0, inplace=True)
    df_gw.reset_index(drop=True, inplace=True)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["years"],
        time="year",
        value=df_sw["value"],
        unit="km3/year",
    )
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    # Reading e flow data
    PATH = private_data_path("water", "water_availability", f"e-flow_{context.RCP}.csv")
    df_env = pd.read_csv(PATH)

    df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
    years = list(range(2010, 2105, 5))
    df_env.columns = years
    df_env.index = df_x["BCU_name"]
    df_env[2110] = df_env[2100]
    df_env.drop(columns=[2065, 2075, 2085, 2095], inplace=True)
    df_env = df_env.stack().reset_index()
    df_env.columns = ["Region", "years", "value"]
    df_env.sort_values(["Region", "years", "value"], inplace=True)
    df_env.fillna(0, inplace=True)
    df_env.reset_index(drop=True, inplace=True)

    # Return a processed dataframe for env flow calculations
    if context.SDG:
        # dataframe to put constraints on env flows
        eflow_df = make_df(
            "bound_activity_lo",
            node_loc="B" + df_env["Region"],
            technology="return_flow",
            year_act=df_env["years"],
            mode="M1",
            time="year",
            value=df_env["value"],
            unit="km3/year",
        )

        eflow_df["value"] = eflow_df["value"].apply(lambda x: x if x >= 0 else 0)
        eflow_df = eflow_df[eflow_df["year_act"] >= 2025].reset_index(drop=True)

        eflow_df["value"] = np.where(
            eflow_df["value"] >= 0.7 * dmd_df["value"],
            0.7 * dmd_df["value"],
            eflow_df["value"],
        )

        results["bound_activity_lo"] = eflow_df

    return results
