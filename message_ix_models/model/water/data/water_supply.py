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
from message_ix_models.model.water.data.water_supply_rules import *
from message_ix_models.model.water.data.demand_rules import eval_field as eval_field


def _basin_region_preprocess(df: pd.DataFrame, df_x: pd.DataFrame, context: "Context", info : None, monthly : bool = False) -> pd.DataFrame:
    """
    Preprocess the basin region data for map_basin_region_wat function.
    """
    df = df.copy()
    df.drop(columns=["Unnamed: 0"], inplace=True)

    df["BCU_name"] = df_x["BCU_name"]

    df["MSGREG"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df["BCU_name"].str.split("|").str[-1]
    )

    df = df.set_index(["MSGREG", "BCU_name"])

    # Calculating ratio of water availability in basin by region
    df = df.groupby(["MSGREG"]).apply(lambda x: x / x.sum())
    df.reset_index(level=0, drop=True, inplace=True)
    df.reset_index(inplace=True)
    df["Region"] = "B" + df["BCU_name"].astype(str)
    df["Mode"] = df["Region"].replace(regex=["^B"], value="M")
    df.drop(columns=["BCU_name"], inplace=True)
    df.set_index(["MSGREG", "Region", "Mode"], inplace=True)
    df = df.stack().reset_index(level=0).reset_index()
    df.columns = pd.Index(["region", "mode", "date", "MSGREG", "share"])
    df.sort_values(["region", "date", "MSGREG", "share"], inplace=True)
    df["year"] = pd.DatetimeIndex(df["date"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["date"]).month
    df = df[df["year"].isin(info.Y)]
    df.reset_index(drop=True, inplace=True)

    return df

@minimum_version("message_ix 3.7")
@minimum_version("python 3.10")
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

    Requires Python 3.10+ for pattern matching support.
    """
    info = context["water build info"]

    PATH = package_data_path(
        "water",
        "delineation",
        f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)
    

    match context.time:
        case "year":
            # Adding freshwater supply constraints
            # Reading data, the data is spatially and temprally aggregated from GHMs
            monthly = False
            path = package_data_path(
                "water",
                "availability",
                f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case "month":
            # add water return flows for cooling tecs
            # Use share of basin availability to distribute the return flow from
            monthly = True
            path = package_data_path(
                "water",
                "availability",
                f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time: {context.time}")

    df_sw = pd.read_csv(path)
    df_sw = _basin_region_preprocess(df_sw, df_x, context, info, monthly)

    return df_sw

@minimum_version("python 3.10")
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
    
    Requires Python 3.10+ for pattern matching support.
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


    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    # define mapping for freshwater supply based on a 50-year lifetime
    yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)
    yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)


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
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
                level=r["level"],
                commodity=r["commodity"],
                mode=r["mode"],
                time=r["time"],
                time_origin=r["time_origin"],
                time_dest=r["time_dest"],
                year_vtg= year_wat,
                year_act= year_wat,
            ).pipe(broadcast, node_loc=node_region).pipe(same_node)
            cooling_outputs.append(df_rule)
        results["output"] = pd.concat(cooling_outputs)

    elif context.nexus_set == "nexus":

        slack_inputs = []
        for rule in SLACK_TECHNOLOGY_RULES:
            r = rule.copy()
            common_args = {
                "name": r["type"], 
                "technology": r["technology"], 
                "value": r["value"], 
                "unit": r["unit"], 
                "level": r["level"], 
                "commodity": r["commodity"], 
            }
            match rule["technology"]:
                case "return_flow"|"gw_recharge":
                    df_rule = make_df(
                        **common_args, 
                        mode=r["mode"], 
                        year_vtg=year_wat, 
                        year_act=year_wat, 
                    ).pipe(
                        broadcast, 
                        node_loc=df_node["node"], 
                        time=pd.Series(sub_time), 
                    ).pipe(same_node).pipe(same_time)
                    slack_inputs.append(df_rule)
                case "basin_to_reg":
                    df_rule = make_df(
                        **common_args, 
                        mode=eval_field(r["mode"], df_node), 
                        node_origin=eval_field(r["node_origin"], df_node), 
                        node_loc=eval_field(r["node_loc"], df_node), 
                    ).pipe(
                        broadcast, 
                        year_vtg=year_wat, 
                        time=pd.Series(sub_time), 
                    ).pipe(same_time)
                    slack_inputs.append(df_rule)

                case "salinewater_return":
                    #skip
                    pass
                case _:
                    raise ValueError(f"Invalid technology: {r['technology']}")
        slack_inputs = pd.concat(slack_inputs)
        slack_inputs["year_act"] = slack_inputs["year_vtg"]
        results["input"] = slack_inputs

        extraction_inputs = []
        for rule in EXTRACTION_INPUT_RULES:
            r = rule.copy()

            broadcast_yr = yv_ya_sw if r["broadcast"]["yv_ya"] == "yv_ya_sw" else yv_ya_gw

            common_args = {
                "technology": r["technology"],
                "value": eval_field(r["value"], df_gwt),
                "unit": r["unit"],
                "level": r["level"],
                "commodity": r["commodity"],
                "mode": r["mode"],
                "node_origin": eval_field(r["node_origin"], df_node),
                "node_loc": eval_field(r["node_loc"], df_node),
            }
            match r["technology"]:
                case "extract_surfacewater"|"extract_groundwater":
                    df_rule = make_df(
                        r["type"],
                        **common_args,
                    )   
                case _:
                    df_rule = make_df(
                        r["type"],
                        **common_args,
                        time_origin=r["time_origin"],
                    )
            df_rule = broadcast(df_rule, broadcast_yr, time=pd.Series(sub_time))
            df_rule = same_time(df_rule)
            extraction_inputs.append(df_rule)
        inp = pd.concat([slack_inputs] + extraction_inputs)
        inp["value"] = pd.to_numeric(inp["value"], errors="raise")

        if context.type_reg == "global":
            inp.loc[
                (inp["technology"].str.contains("extract_gw_fossil"))
                & (inp["year_act"] == 2020)
                & (inp["node_loc"] == "R11_SAS"),
                "value",
            ] *= 0.5

        results["input"] = inp

        # Refactored extraction_outputs block to reduce repetition
        extraction_outputs = []
        for rule in EXTRACTION_OUTPUT_RULES:
            r = rule.copy()
            r["type"] = "output"
            
            common_args = {
                "name": "output",
                "technology": r["technology"],
                "value": r["value"],
                "unit": r["unit"],
                "level": r["level"],
                "commodity": r["commodity"],
                "mode": r["mode"],                
            }
            match r["technology"]:
                case "extract_salinewater":
                    df_rule = make_df(
                        **common_args, 
                        year_vtg=year_wat,
                        year_act=year_wat,
                        time=r["time"],
                        time_dest=r["time_dest"],
                        time_origin=r["time_origin"],
                    )
                    df_rule = broadcast(df_rule, node_loc=node_region)
                    df_rule = same_node(df_rule)
                case _:
                    common_args.update(
                        node_loc= eval_field(r["node_loc"], df_node),
                        node_dest= eval_field(r["node_dest"], df_node),
                    )
                    if r["technology"] == "extract_gw_fossil":
                        df_rule = make_df(**common_args, time_origin=r["time_origin"])
                    else:
                        df_rule = make_df(**common_args)
                    
                    broadcast_year = yv_ya_gw if r["technology"] in ["extract_gw_fossil", "extract_groundwater"] else yv_ya_sw
                    df_rule = broadcast(df_rule, broadcast_year, time=pd.Series(sub_time))
                    df_rule = same_time(df_rule)
            extraction_outputs.append(df_rule)
        results["output"] = pd.concat(extraction_outputs)
        
        hist_new_cap = []
        for rule in HISTORICAL_NEW_CAPACITY_RULES:
            r = rule.copy()
            name = r["type"]
            df_rule = make_df(
                name,
                node_loc = eval_field(r["node_loc"], df_hist),
                technology = r["technology"],
                value = eval_field(r["value"], df_hist),
                unit = r["unit"],
                year_vtg = r["year_vtg"],
            )
            hist_new_cap.append(df_rule)

        results["historical_new_capacity"] = pd.concat(hist_new_cap)
        dummy_basin_to_reg_output = []
        for rule in DUMMY_BASIN_TO_REG_OUTPUT_RULES:
            r = rule.copy()
            df_rule = make_df(
                r["type"],
                technology=r["technology"],
                value=r["value"],
                unit=r["unit"],
                level=r["level"],
                commodity=r["commodity"],
                time_dest=r["time_dest"],
                mode=eval_field(r["mode"], df_node),
                node_loc=eval_field(r["node_loc"], df_node),
                node_dest=eval_field(r["node_dest"], df_node),
            ).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time))
            df_rule["year_act"] = df_rule["year_vtg"]
            dummy_basin_to_reg_output.append(df_rule)
        dummy_basin_to_reg_output = pd.concat(dummy_basin_to_reg_output)
        results["output"] = pd.concat([results["output"], dummy_basin_to_reg_output])

        results["output"]["year_act"] = results["output"]["year_vtg"]

        var_costs = []
        for rule in DUMMY_VARIABLE_COST_RULES:
            r = rule.copy()
            common_args = {
                "name": r["type"],
                "technology": r["technology"],
                "value": r["value"],
                "unit": r["unit"],

            }
            match r["technology"]:
                case "basin_to_reg":
                    common_args.update(
                        mode=eval_field(r["mode"], df_node),
                        node_loc=eval_field(r["node_loc"], df_node),
                    )
                    df_rule = make_df(**common_args).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time))
                    df_rule["year_act"] = df_rule["year_vtg"]
                    var_costs.append(df_rule)
                case "extract_surfacewater"|"extract_groundwater":
                    pass
                case _:
                    raise ValueError(f"Invalid technology: {r['technology']}")
        results["var_cost"] = pd.concat(var_costs)

        fix_costs = []
        for rule in FIXED_COST_RULES:
            r = rule.copy()
            common_args = {
                "name": r["type"],
                "technology": r["technology"],
                "value": r["value"],
                "unit": r["unit"],
            }
            match r["technology"]:
                case "extract_gw_fossil":
                    df_rule = make_df(**common_args).pipe(broadcast, yv_ya_gw, node_loc=df_node["node"])
                    fix_costs.append(df_rule)
                case _:
                    raise ValueError(f"Invalid technology: {r['technology']}")
        fix_cost = pd.concat(fix_costs)

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
                        r[field] = eval_field(r[field], df_sw)
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


def _e_flow_preprocess(df: pd.DataFrame, df_x: pd.DataFrame, context: "Context", info : None, monthly : bool = False) -> pd.DataFrame:
    """
    Preprocess the e-flow data for add_e_flow function.
    """
    df = df.copy()
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.index = df_x["BCU_name"].index
    df = df.stack().reset_index()
    df.columns = pd.Index(["Region", "years", "value"])
    df.sort_values(["Region", "years", "value"], inplace=True)
    df.fillna(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["year"] = pd.DatetimeIndex(df["years"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["years"]).month
    df["Region"] = df["Region"].map(df_x["BCU_name"])
    df2210 = df[df["year"] == 2100].copy()
    df2210["year"] = 2110
    df = pd.concat([df, df2210])
    df = df[df["year"].isin(info.Y)]

    return df
    

@minimum_version("python 3.10")
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
    
    Requires Python 3.10+ for pattern matching support.
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

    eflow_dmd_df = []
    for rule in E_FLOW_RULES_DMD:
        r = rule.copy()
        dmd_df = make_df(
            r["type"],
            node=eval_field(r["node"], df_sw),
            commodity=r["commodity"],
            level=r["level"],
            year=eval_field(r["year"], df_sw),
            time=eval_field(r["time"], df_sw),
            value=eval_field(r["value"], df_sw),
            unit=r["unit"],
        )
        eflow_dmd_df.append(dmd_df)
    dmd_df = pd.concat(eflow_dmd_df)
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    match context.time:
        case "year":
            # Reading data, the data is spatially and temporally aggregated from GHMs
            monthly = False
            path = package_data_path(
                "water",
                "availability",
                f"e-flow_{context.RCP}_{context.regions}.csv",
            )
        case "month":
            monthly = True
            path = package_data_path(
                "water",
                "availability",
                f"e-flow_5y_m_{context.RCP}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time: {context.time}")

    df_env = pd.read_csv(path)
    df_env = _e_flow_preprocess(df_env, df_x, context, info, monthly)

    # Return a processed dataframe for env flow calculations
    if context.SDG != "baseline":
        # dataframe to put constraints on env flows
        eflow_df = []
        for rule in E_FLOW_RULES_BOUND:
            r = rule.copy()
            df = make_df(
                r["type"],
                node_loc= "B" + eval_field(r["node"], df_env),
                technology=r["technology"],
                year_act=eval_field(r["year_act"], df_env),
                mode=r["mode"],
                time=eval_field(r["time"], df_env),
                value=eval_field(r["value"], df_env),
                unit=r["unit"],
            )
            eflow_df.append(df)
        eflow_df = pd.concat(eflow_df)

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


