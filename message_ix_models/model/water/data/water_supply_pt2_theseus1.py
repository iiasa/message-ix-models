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
from message_ix_models.model.water.data.water_supply_rules import (
    COOLING_SUPPLY_RULES, EXTRACTION_INPUT_RULES, 
    TECHNICAL_LIFETIME_RULES, INVESTMENT_COST_RULES,
    SHARE_MODE_RULES
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

    # define mapping for freshwater supply based on a 50-year lifetime
    yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)
    yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)

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
        # cooling branch using cooling supply rules
        cooling_outputs = []
        for rule in COOLING_SUPPLY_RULES:
            r = rule.copy()
            r["year_vtg"] = year_wat
            r["year_act"] = year_wat
            r.setdefault("broadcast", {})["node_loc"] = node_region
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
                level=r["level"],
                commodity=r["commodity"],
                mode=r["mode"],
                time=r.get("time"),
                time_origin=r.get("time_origin"),
                time_dest=r.get("time_dest"),
                year_vtg=r.get("year_vtg"),
                year_act=r.get("year_act"),
            )
            if "broadcast" in r and r["broadcast"]:
                df_rule = broadcast(df_rule, **r["broadcast"])
            df_rule = same_node(df_rule)
            cooling_outputs.append(df_rule)
        results["output"] = pd.concat(cooling_outputs)

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
        yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)
        # apply dsl rules to build extraction input dataframes for water avail and electricity supply
        extraction_inputs = []
        for rule in EXTRACTION_INPUT_RULES:
            r = rule.copy()
            # if the value is a string expression, evaluate it in a context with df_gwt available
            if isinstance(r["value"], str):
                try:
                    r["value"] = eval(r["value"], {}, {"df_gwt": df_gwt})
                    if hasattr(r["value"], "astype"):
                        r["value"] = r["value"].astype(float)
                except Exception as e:
                    raise ValueError(f"error evaluating rule {r['technology']} value expression: {e}")
            broadcast_yr = yv_ya_sw if r["broadcast"]["yv_ya"] == "yv_ya_sw" else yv_ya_gw
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
                level=r["level"],
                commodity=r["commodity"],
                mode=r["mode"],
                time_origin=r.get("time_origin"),
                node_origin=df_node["node"] if r.get("node_origin") == "node" else df_node["region"],
                node_loc=df_node["node"] if r.get("node_loc") == "node" else df_node["node"]
            )
            df_rule = broadcast(df_rule, broadcast_yr, time=pd.Series(sub_time))
            df_rule = same_time(df_rule)
            extraction_inputs.append(df_rule)
        inp = pd.concat([inp] + extraction_inputs)
        inp["value"] = pd.to_numeric(inp["value"], errors="raise")

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
        fix_cost = make_df(
            "fix_cost",
            technology="extract_gw_fossil",
            value=300,  # assumed
            unit="USD/km3",
        ).pipe(broadcast, yv_ya_gw, node_loc=df_node["node"])

        results["fix_cost"] = fix_cost

        # incorporate share_mode_rules for linking basin and region water supply
        df_sw = map_basin_region_wat(context)
        share_outputs = []
        for rule in SHARE_MODE_RULES:
            r = rule.copy()
            for field in ["mode", "node_share", "time", "value", "year_act"]:
                if field in r and isinstance(r[field], str):
                    try:
                        # evaluate the field expression in the context where df_sw is available
                        r[field] = eval(r[field], {}, {"df_sw": df_sw})
                    except Exception as e:
                        raise ValueError(f"error evaluating share_mode_up rule for field {field}: {e}")
            share_df = make_df(
                r["type"],
                shares=r["shares"],
                technology=r["technology"],
                mode=r["mode"],
                node_share=r["node_share"],
                time=r["time"],
                value=r["value"],
                unit=r["unit"],
                year_act=r["year_act"],
            )
            share_outputs.append(share_df)
        results["share_mode_up"] = pd.concat(share_outputs)

        # technical lifetime refactor using rules
        tl_list = []
        for rule in TECHNICAL_LIFETIME_RULES:
            r = rule.copy()
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
            )
            if "broadcast" in r and r["broadcast"]:
                df_rule = broadcast(df_rule, year_vtg=year_wat, node_loc=df_node["node"])
            df_rule = same_node(df_rule)
            tl_list.append(df_rule)
        results["technical_lifetime"] = pd.concat(tl_list)

        # refactored investment cost block using investment cost rules
        inv_costs = []
        for rule in INVESTMENT_COST_RULES:
            r = rule.copy()
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
            )
            if "broadcast" in r and r["broadcast"]:
                df_rule = broadcast(df_rule, year_vtg=year_wat, node_loc=df_node["node"])
            inv_costs.append(df_rule)
        results["inv_cost"] = pd.concat(inv_costs)

    return results

