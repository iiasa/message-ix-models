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
from message_ix_models.model.water.data.supply_tooling import *
from message_ix_models.model.water.data.water_supply_rules import *

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

def add_water_supply(context: "Context") -> dict[str, pd.DataFrame]:
    # common context setup
    results = {}
    info = context["water build info"]
    scen = Scenario(context.get_platform(), **context.core.scenario_info)
    fut_year = info.Y
    year_wat = (2010, 2015, *info.Y)
    sub_time = context.time
    first_year = scen.firstmodelyear

    # compute basin data (common to all cases)
    file = f"basins_by_region_simpl_{context.regions}.csv"
    path = package_data_path("water", "delineation", file)
    df_node = pd.read_csv(path)
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    match context.nexus_set:
        case "cooling":
            # use unique basin regions for updating rule keys
            node_region = df_node["region"].unique()
            cooling_rules = [
                update_rule(rule,
                            extra={"year_vtg": year_wat, "year_act": year_wat},
                            broadcast_updates={"node_loc": node_region})
                for rule in COOLING_SUPPLY_RULES
            ]
            output_df = apply_supply_dsl_transformations(cooling_rules, year_vtg=year_wat, year_act=year_wat)
            results["output"] = output_df

        case "nexus":
            # read additional data for nexus workflow
            file1 = f"gw_energy_intensity_depth_{context.regions}.csv"
            path1 = package_data_path("water", "availability", file1)
            df_gwt = pd.read_csv(path1)
            df_gwt["region"] = (
                context.map_ISO_c[context.regions]
                if context.type_reg == "country"
                else f"{context.regions}_" + df_gwt["REGION"].astype(str)
            )

            file2 = f"historical_new_cap_gw_sw_km3_year_{context.regions}.csv"
            path2 = package_data_path("water", "availability", file2)
            df_hist = pd.read_csv(path2)
            df_hist["BCU_name"] = "B" + df_hist["BCU_name"].astype(str)

            yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)
            yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)
            tech_year_map = {
                "extract_surfacewater": yv_ya_sw,
                "extract_groundwater": yv_ya_gw,
                "extract_gw_fossil": yv_ya_gw
            }

            nexus_input_rules = [
                update_rule(rule,
                            extra={"year_vtg": year_wat, "year_act": year_wat},
                            broadcast_updates={"node_loc": df_node["node"],
                                               "time": pd.Series(sub_time),
                                               "node_origin": df_node["node"]},
                            tech_year_mapping=tech_year_map)
                for rule in NEXUS_INPUT_RULES
            ]
            inp_df = apply_supply_dsl_transformations(nexus_input_rules, year_vtg=year_wat, year_act=year_wat)
            if context.type_reg == "global":
                cond = (inp_df["technology"].str.contains("extract_gw_fossil") &
                        (inp_df["year_act"] == 2020) &
                        (inp_df["node_loc"] == "R11_SAS"))
                inp_df.loc[cond, "value"] *= 0.5
            results["input"] = inp_df

            nexus_output_rules = [
                update_rule(rule,
                            extra={"year_vtg": year_wat, "year_act": year_wat},
                            broadcast_updates={"node_loc": df_node["node"], "node_dest": df_node["node"]},
                            tech_year_mapping=tech_year_map)
                for rule in NEXUS_OUTPUT_RULES
            ]
            out_df = apply_supply_dsl_transformations(nexus_output_rules, year_vtg=year_wat, year_act=year_wat)
            results["output"] = pd.concat([results.get("output", pd.DataFrame()), out_df], ignore_index=True)

            hist_rules = [
                update_rule(rule,
                            extra={},
                            broadcast_updates={"node_loc": df_hist["BCU_name"]},
                            update_value_fn=lambda r: (df_hist["hist_cap_sw_km3_year"] / 5
                                                       if r["technology"] == "extract_surfacewater"
                                                       else df_hist["hist_cap_gw_km3_year"] / 5))
                for rule in NEXUS_HIST_RULES
            ]
            results["historical_new_capacity"] = apply_supply_dsl_transformations(hist_rules)

            var_cost_rules = [
                update_rule(rule,
                            extra={"year_vtg": year_wat},
                            broadcast_updates={"node_loc": df_node["region"], "time": pd.Series(sub_time)})
                for rule in NEXUS_VAR_COST_RULES
            ]
            var_cost_df = apply_supply_dsl_transformations(var_cost_rules, year_vtg=year_wat, year_act=year_wat)
            var_cost_df["year_act"] = var_cost_df["year_vtg"]
            results["var_cost"] = var_cost_df

            df_sw = map_basin_region_wat(context)
            share_rules = [
                update_rule(rule,
                            extra={"year_act": df_sw["year"]},
                            broadcast_updates={"node_share": df_sw["MSGREG"], "time": df_sw["time"]},
                            update_value_fn=lambda r: df_sw["share"])
                for rule in NEXUS_SHARE_RULES
            ]
            results["share_mode_up"] = apply_supply_dsl_transformations(share_rules)

            lifetime_rules = [
                update_rule(rule,
                            extra={},
                            broadcast_updates={"year_vtg": year_wat, "node_loc": df_node["node"]})
                for rule in NEXUS_LIFETIME_RULES
            ]
            results["technical_lifetime"] = apply_supply_dsl_transformations(lifetime_rules)

            inv_cost_rules = [
                update_rule(rule,
                            extra={},
                            broadcast_updates={"year_vtg": year_wat, "node_loc": df_node["node"]})
                for rule in NEXUS_INV_COST_RULES
            ]
            results["inv_cost"] = apply_supply_dsl_transformations(inv_cost_rules)

            fix_cost_rules = [
                update_rule(rule,
                            extra={},
                            broadcast_updates={"year": map_yv_ya_lt(year_wat, 20, first_year), "node_loc": df_node["node"]})
                for rule in NEXUS_FIX_COST_RULES
            ]
            results["fix_cost"] = apply_supply_dsl_transformations(fix_cost_rules)

    return results

def add_e_flow(context: "Context") -> dict[str, pd.DataFrame]:
    """add env flows using dsl transformation"""
    # init results
    results = {}
    info = context["water build info"]

    # read water avail data
    df_sw, df_gw = read_water_availability(context)

    # load basin delineation data
    file_basin = f"basins_by_region_simpl_{context.regions}.csv"
    path_basin = package_data_path("water", "delineation", file_basin)
    df_x = pd.read_csv(path_basin)

    # prepare water demand df
    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=df_sw["value"],
        unit="km3/year",
    )
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    # read env flow data (annual or monthly)
    file_env = (
        f"e-flow_{context.RCP}_{context.regions}.csv"
        if "year" in context.time
        else f"e-flow_5y_m_{context.RCP}_{context.regions}.csv"
    )
    path_env = package_data_path("water", "availability", file_env)
    df_env = pd.read_csv(path_env)
    df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
    df_env.index = df_x["BCU_name"].index
    df_env = df_env.stack().reset_index()
    df_env.columns = pd.Index(["Region", "years", "value"])
    df_env.sort_values(["Region", "years", "value"], inplace=True)
    df_env.fillna(0, inplace=True)
    df_env.reset_index(drop=True, inplace=True)
    df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
    df_env["time"] = "year" if "year" in context.time else pd.DatetimeIndex(df_env["years"]).month
    df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
    df_env2210 = df_env[df_env["year"] == 2100].copy()
    df_env2210["year"] = 2110
    df_env = pd.concat([df_env, df_env2210])
    df_env = df_env[df_env["year"].isin(info.Y)]

    # update rule if not baseline
    if context.SDG != "baseline":
        eflow_rule = update_rule(
            E_FLOW_RULES[0],
            extra={"year_act": df_env["year"], "time": df_env["time"], "value": df_env["value"]},
            broadcast_updates={"node_loc": "B" + df_env["Region"].astype(str)},
            update_value_fn=lambda r: np.where(
                r["value"] >= 0.7 * dmd_df["value"], 0.7 * dmd_df["value"], r["value"]
            ),
        )
        results["bound_activity_lo"] = apply_supply_dsl_transformations([eflow_rule])

    return results