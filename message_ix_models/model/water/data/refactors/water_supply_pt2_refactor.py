from __future__ import annotations
import pandas as pd
from message_ix import make_df, Scenario
from message_ix_models.util import broadcast, package_data_path
from message_ix_models.model.water.data.utils import map_yv_ya_lt, map_basin_region_wat

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