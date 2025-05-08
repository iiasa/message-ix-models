"""Prepare data for adding techs related to water distribution,
treatment in urban & rural"""

from collections import defaultdict
from typing import Any

import pandas as pd

from message_ix_models import Context
from message_ix_models.model.water.data.infrastructure_rules import (
    CAP_RULES,
    DESALINATION_BOUND_LO_RULES,
    DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES,
    DESALINATION_HISTORICAL_CAPACITY_RULES,
    DESALINATION_INPUT_RULES2,
    DESALINATION_INV_COST_RULES,
    DESALINATION_OUTPUT_RULES,
    DESALINATION_OUTPUT_RULES2,
    FIX_COST_DESALINATION_RULES,
    FIX_COST_RULES,
    INPUT_DATAFRAME_STAGE1,
    INPUT_DATAFRAME_STAGE2,
    INV_COST_RULES,
    OUTPUT_RULES,
    TL_DESALINATION_RULES,
    TL_RULES,
    VAR_COST_DESALINATION_RULES,
    VAR_COST_RULES,
)
from message_ix_models.model.water.dsl_engine import build_standard
from message_ix_models.model.water.utils import safe_concat
from message_ix_models.util import make_matched_dfs, minimum_version, package_data_path


@minimum_version("python 3.10")
def start_creating_input_dataframe(
    sdg: str,
    df_node: pd.DataFrame,
    df_non_elec: pd.DataFrame,
    df_dist: pd.DataFrame,
    year_wat: tuple,
    first_year: int,
    sub_time,
) -> pd.DataFrame:
    """Creates an input pd.DataFrame and adds some data to it."""
    inp_df = [pd.DataFrame([])]

    args = {
        "node_loc": df_node["node"],
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
    }
    for rule in INPUT_DATAFRAME_STAGE1.get_rule():
        match (rule["condition"], sdg):
            # non elec commodities excecuted by default
            case "default", _:
                for index, rows in df_non_elec.iterrows():
                    current_args = args.copy()
                    current_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    inp_df.append(build_standard(r=rule, base_args=current_args))

            case "baseline_main", "baseline":
                # baseline case
                for index, rows in df_dist.iterrows():
                    current_args = args.copy()
                    current_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    inp_df.append(build_standard(r=rule, base_args=current_args))
            case "baseline_additional", "baseline":
                # baseline case additional
                # takes the final row from df_dist as input
                current_args = args.copy()
                rows = df_dist.iloc[-1]
                current_args["rule_dfs"] = rows
                current_args["lt"] = rows["technical_lifetime_mid"]
                inp_df.append(build_standard(r=rule, base_args=current_args))
            # non baseline case
            case "!baseline", _ if sdg != "baseline":
                for index, rows in df_dist.iterrows():
                    current_args = args.copy()
                    current_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    inp_df.append(build_standard(r=rule, base_args=current_args))
                    return safe_concat(inp_df)  # Terminates in the non-baseline case

    return safe_concat(inp_df)


@minimum_version("python 3.10")
def prepare_input_dataframe(
    context: "Context",
    sub_time,
    year_wat: tuple,
    first_year: int,
    df_node: pd.DataFrame,
    techs: list[str],
    df_elec: pd.DataFrame,
) -> defaultdict[Any, list]:
    """Creates an input pd.DataFrame and adds some data to it."""
    result_dc = defaultdict(list)

    args = {
        "lt": 1,
        "node_loc": df_node["node"],
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
    }
    for _, rows in df_elec.iterrows():
        dfs = {"rows": rows, "df_node": df_node}
        args["rule_dfs"] = dfs
        is_tech = rows["tec"] in techs
        # INPUT_DATAFRAME_STAGE2.change_unit("GWh/km3")
        for rule in INPUT_DATAFRAME_STAGE2.get_rule():
            match (context.SDG, rule["condition"], is_tech):
                case _, "!baseline", True if context.SDG != "baseline":
                    inp = build_standard(r=rule, base_args=args)
                    result_dc["input"].append(inp)
                case "baseline", "baseline_p1" | "baseline_p2", True:
                    inp = build_standard(r=rule, base_args=args)
                    result_dc["input"].append(inp)
                case _, "non_tech", False:
                    inp = build_standard(r=rule, base_args=args)
                    result_dc["input"].append(inp)
    return result_dc


def add_infrastructure_techs(context: "Context") -> dict[str, pd.DataFrame]:
    """Process water distribution data for a scenario instance.

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
    # Reference to the water configuration
    info = context["water build info"]

    # define an empty dictionary
    results = {}
    sub_time = context.time
    # load the scenario from context
    scen = context.get_scenario()

    year_wat = (2010, 2015, *info.Y)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    # Reading water distribution mapping from csv
    path = package_data_path("water", "infrastructure", "water_distribution.xlsx")
    df = pd.read_excel(path)

    techs = [
        "urban_t_d",
        "urban_unconnected",
        "industry_unconnected",
        "rural_t_d",
        "rural_unconnected",
    ]

    df_non_elec = df[df["incmd"] != "electr"].reset_index()
    df_dist = df_non_elec[df_non_elec["tec"].isin(techs)]
    df_non_elec = df_non_elec[~df_non_elec["tec"].isin(techs)]
    df_elec = df[df["incmd"] == "electr"].reset_index()

    inp_df = start_creating_input_dataframe(
        sdg=context.SDG,
        df_node=df_node,
        df_non_elec=df_non_elec,
        df_dist=df_dist,
        year_wat=year_wat,
        first_year=first_year,
        sub_time=sub_time,
    )

    result_dc = prepare_input_dataframe(
        context=context,
        sub_time=sub_time,
        year_wat=year_wat,
        first_year=first_year,
        df_node=df_node,
        techs=techs,
        df_elec=df_elec,
    )

    results_new = {par_name: safe_concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = safe_concat([inp_df, results_new["input"]])
    results["input"] = inp_df

    # add output dataframe
    df_out = df[~df["outcmd"].isna()]
    df_out_dist = df_out[df_out["tec"].isin(techs)]
    df_out = df_out[~df_out["tec"].isin(techs)]

    args_out = {
        "node_loc": df_node["node"],
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
    }

    out_df = _calculate_infra_output(context, df_out, df_out_dist, args_out)
    results["output"] = out_df

    # Filtering df for capacity factors
    df_cap = df.dropna(subset=["capacity_factor_mid"])

    # Adding capacity factor dataframe
    args_cap = {
        "node_loc": df_node["node"],
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
    }
    cap_df = _calculate_infra_cap_factor(df_cap, args_cap)
    results["capacity_factor"] = cap_df

    # Filtering df for capacity factors
    df_tl = df.dropna(subset=["technical_lifetime_mid"])
    extra_args = {"year_vtg": year_wat}
    tl_list = [pd.DataFrame([])]
    for rule in TL_RULES.get_rule():
        current_args = args_out.copy()
        current_args["rule_dfs"] = df_tl
        tl_list.append(
            build_standard(r=rule, base_args=current_args, extra_args=extra_args)
        )
    tl = safe_concat(tl_list)
    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=1)
    results["construction_time"] = cons_time["construction_time"]

    # Investment costs
    df_inv = df.dropna(subset=["investment_mid"])
    extra_args = {"year_vtg": year_wat}
    # INV_COST_RULES.change_unit("USD/km3")
    for rule in INV_COST_RULES.get_rule():
        current_args = args_out.copy()
        current_args["rule_dfs"] = df_inv
        # Prepare dataframe for investments
        inv_cost = build_standard(r=rule, base_args=current_args, extra_args=extra_args)
        inv_cost = inv_cost[~inv_cost["technology"].isin(techs)]
    results["inv_cost"] = inv_cost

    # Fixed costs
    # Prepare data frame for fix_cost
    fix_cost_list = [pd.DataFrame([])]
    # FIX_COST_RULES.change_unit("USD/km3")
    for rule in FIX_COST_RULES.get_rule():
        for index, rows in df_inv.iterrows():
            current_args = args_out.copy()
            current_args["rule_dfs"] = df_inv
            current_args["lt"] = rows["technical_lifetime_mid"]
            fix_cost_list.append(build_standard(r=rule, base_args=current_args))
    fix_cost = safe_concat(fix_cost_list)
    fix_cost = fix_cost[~fix_cost["technology"].isin(techs)]

    results["fix_cost"] = fix_cost

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]
    var_cost_list = [pd.DataFrame([])]

    var_cost = _add_var_cost(
        context,
        df_var,
        df_var_dist,
        df_node,
        year_wat,
        first_year,
        sub_time,
        var_cost_list,
        args_out,
    )

    results["var_cost"] = var_cost

    return results


def _calculate_infra_output(
    context: "Context",
    df_out: pd.DataFrame,
    df_out_dist: pd.DataFrame,
    args: dict,
) -> pd.DataFrame:
    """Calculate output parameters for infrastructure techs."""
    out_df_list = [pd.DataFrame([])]
    for rule in OUTPUT_RULES.get_rule():
        match (context.SDG, rule["condition"]):
            case (_, "default"):
                for index, rows in df_out.iterrows():
                    current_args = args.copy()
                    current_args["rule_dfs"] = rows
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    out_df_list.append(build_standard(r=rule, base_args=current_args))
            case (_, "!baseline") if context.SDG != "baseline":
                current_args = args.copy()
                current_args["rule_dfs"] = df_out_dist
                # Use lifetime from df_out as it seems to be the intended fallback
                current_args["lt"] = df_out.iloc[-1]["technical_lifetime_mid"]
                out_df_list.append(build_standard(r=rule, base_args=current_args))
            case ("baseline", "baseline_p1" | "baseline_p2"):
                current_args = args.copy()
                current_args["rule_dfs"] = df_out_dist
                # Use lifetime from df_out as it seems to be the intended fallback
                current_args["lt"] = df_out.iloc[-1]["technical_lifetime_mid"]
                out_df_list.append(build_standard(r=rule, base_args=current_args))
    return safe_concat(out_df_list)


def _calculate_infra_cap_factor(
    df_cap: pd.DataFrame,
    args: dict,
) -> pd.DataFrame:
    """Calculate capacity factor for infrastructure techs."""
    cap_list = [pd.DataFrame([])]
    for rule in CAP_RULES.get_rule():
        for index, rows in df_cap.iterrows():
            current_args = args.copy()
            current_args["rule_dfs"] = rows
            current_args["lt"] = rows["technical_lifetime_mid"]
            cap_list.append(build_standard(r=rule, base_args=current_args))
    return safe_concat(cap_list)


def _add_var_cost(
    context: "Context",
    df_var: pd.DataFrame,
    df_var_dist: pd.DataFrame,
    df_node: pd.DataFrame,
    year_wat: tuple,
    first_year: int,
    sub_time: tuple,
    var_cost_list: list[pd.DataFrame],
    args: dict,
) -> pd.DataFrame:
    sdg = context.SDG

    rules_baseline_dist = []  # collecting the two rules for
    # baseline_dist_p1 and baseline_dist_p2
    # Handle non-baseline case
    rows = pd.Series()  # dummy series
    # VAR_COST_RULES.change_unit("USD/km3")
    for rule in VAR_COST_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        match (sdg, rule["condition"]):
            case (_, "!baseline") if sdg != "baseline":
                for index, rows in df_var.iterrows():
                    # Variable cost
                    current_args = args.copy()
                    current_args["rule_dfs"] = rows
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    var_cost_list.append(build_standard(r=rule, base_args=current_args))
            case (_, "!baseline_dist") if sdg != "baseline":
                for index, rows in df_var_dist.iterrows():
                    current_args = args.copy()
                    current_args["rule_dfs"] = rows
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    var_cost_list.append(build_standard(r=rule, base_args=current_args))
            case ("baseline", "baseline_main"):
                for index, rows in df_var.iterrows():
                    dfs = {"rows": rows, "df_var": df_var}
                    current_args = args.copy()
                    current_args["rule_dfs"] = dfs
                    current_args["lt"] = rows["technical_lifetime_mid"]
                    var_cost_list.append(build_standard(r=rule, base_args=current_args))
            case ("baseline", "baseline_dist_p1" | "baseline_dist_p2"):
                # collecting both rules because they are implemented in the same
                # function
                rules_baseline_dist.append(rule)
                if len(rules_baseline_dist) == 2:
                    rule = rules_baseline_dist[0]
                    rule_alt = rules_baseline_dist[1]
                    # Apply both p1 and p2 rules for each row in df_var_dist
                    for index, rows in df_var_dist.iterrows():
                        current_args = args.copy()
                        current_args["rule_dfs"] = rows
                        current_args["lt"] = rows["technical_lifetime_mid"]
                        var_cost_list.append(
                            build_standard(r=rule, base_args=current_args)
                        )
                        current_args_alt = args.copy()
                        current_args_alt["rule_dfs"] = rows
                        current_args_alt["lt"] = rows["technical_lifetime_mid"]
                        var_cost_list.append(
                            build_standard(r=rule_alt, base_args=current_args_alt)
                        )

    var_cost = safe_concat(var_cost_list)
    return var_cost


# Helper Functions to reduce Desalination Complexity from 17
def _calculate_desal_output(
    df_desal, df_node, series_sub_time, first_year, year_wat, lt
):
    """Calculate desalination output parameters."""
    out_df = [pd.DataFrame([])]
    # DESALINATION_OUTPUT_RULES.change_unit("km3/year")
    for rule in DESALINATION_OUTPUT_RULES.get_rule():
        output_args = {
            "rule_dfs": df_desal,
            "node_loc": df_node["node"],
            "sub_time": series_sub_time,
            "first_year": first_year,
            "year_wat": year_wat,
            "lt": lt,
        }
        out_df.append(build_standard(r=rule, base_args=output_args))
    return safe_concat(out_df)


def _calculate_desal_tl(df_desal, df_node, year_wat):
    """Calculate desalination technical lifetime parameters."""
    extra_args_tl = {"year_vtg": year_wat}
    tl = [pd.DataFrame([])]
    for rule in TL_DESALINATION_RULES.get_rule():
        tl_args = {
            "rule_dfs": df_desal,
            "node_loc": df_node["node"],
        }
        tl.append(build_standard(r=rule, base_args=tl_args, extra_args=extra_args_tl))
    return safe_concat(tl)


def _calculate_desal_hist_cap(df_hist, df_node):
    """Calculate desalination historical capacity parameters."""
    # DESALINATION_HISTORICAL_CAPACITY_RULES.change_unit("km3/year")
    for rule in DESALINATION_HISTORICAL_CAPACITY_RULES.get_rule():
        hist_cap_args = {
            "rule_dfs": df_hist,
            "node_loc": df_node["node"],
        }
        df_hist_cap = build_standard(r=rule, base_args=hist_cap_args)

    # Divide the historical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    df_hist_cap["value"] = df_hist_cap["value"] / 5
    return df_hist_cap


def _calculate_desal_bound_up(df_proj, df_node):
    """Calculate desalination upper bound capacity parameters."""
    # DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES.change_unit("km3/year")
    for rule in DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES.get_rule():
        bound_up_args = {
            "rule_dfs": df_proj,
            "node_loc": df_node["node"],
        }
        bound_up = build_standard(r=rule, base_args=bound_up_args)

    # Making negative values zero
    bound_up["value"] = bound_up["value"].clip(lower=0)
    # Bound should start from 2025
    bound_up = bound_up[bound_up["year_act"] > 2020]
    return bound_up


def _calculate_desal_inv_cost(df_desal, df_node, year_wat):
    """Calculate desalination investment cost parameters."""
    inv_cost_list = [pd.DataFrame([])]
    # DESALINATION_INV_COST_RULES.change_unit("USD/km3")
    for rule in DESALINATION_INV_COST_RULES.get_rule():
        inv_cost_args = {
            "rule_dfs": df_desal,
            "node_loc": df_node["node"],
        }
        extra_args = {"year_vtg": year_wat}
        inv_cost_list.append(
            build_standard(r=rule, base_args=inv_cost_args, extra_args=extra_args)
        )
    return safe_concat(inv_cost_list)


def _calculate_desal_fix_var_cost(
    df_desal, df_node, series_sub_time, first_year, year_wat
):
    """Calculate desalination fixed and variable cost parameters."""
    fix_cost_list = [pd.DataFrame([])]
    var_cost_list = [pd.DataFrame([])]
    for index, rows in df_desal.iterrows():
        lt = rows["lifetime_mid"]
        node_loc = df_node["node"]

        # Fixed costs
        # FIX_COST_DESALINATION_RULES.change_unit("USD/km3")
        for rule in FIX_COST_DESALINATION_RULES.get_rule():
            fix_cost_args = {
                "rule_dfs": rows,
                "lt": lt,
                "node_loc": node_loc,
                "first_year": first_year,
                "year_wat": year_wat,
            }
            fix_cost_list.append(build_standard(r=rule, base_args=fix_cost_args))

        # Variable cost
        # VAR_COST_DESALINATION_RULES.change_unit("USD/km3")
        for rule in VAR_COST_DESALINATION_RULES.get_rule():
            var_cost_args = {
                "rule_dfs": rows,
                "lt": lt,
                "node_loc": node_loc,
                "sub_time": series_sub_time,
                "first_year": first_year,
                "year_wat": year_wat,
            }
            var_cost_list.append(build_standard(r=rule, base_args=var_cost_args))

    fix_cost = safe_concat(fix_cost_list)
    var_cost = safe_concat(var_cost_list)
    return fix_cost, var_cost


def _calculate_desal_input_output(
    df_desal, df_node, series_sub_time, first_year, year_wat, out_df_initial
):
    """Calculate desalination input and output parameters based on conditions."""
    inp_df_list = []
    out_df_list = [out_df_initial]

    input2_base_args = {
        "sub_time": series_sub_time,
        "first_year": first_year,
        "year_wat": year_wat,
    }

    # Pre-calculate heat dataframe to avoid recalculation in loop
    df_heat = df_desal[df_desal["heat_input_mid"] > 0]
    rule_output_tech = DESALINATION_OUTPUT_RULES2.get_rule()[0]

    for rule in DESALINATION_INPUT_RULES2.get_rule():
        match rule["condition"]:
            case "electricity":
                current_input_list = []
                for index, rows in df_desal.iterrows():
                    elec_args = input2_base_args.copy()
                    elec_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    elec_args["lt"] = rows["lifetime_mid"]
                    inp = build_standard(r=rule, base_args=elec_args)
                    current_input_list.append(inp)
                inp_df_list.append(safe_concat(current_input_list))

            case "heat":
                current_input_list = []
                for index, rows in df_heat.iterrows():
                    heat_args = input2_base_args.copy()
                    heat_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    heat_args["lt"] = rows["lifetime_mid"]
                    inp = build_standard(r=rule, base_args=heat_args)
                    current_input_list.append(inp)
                inp_df_list.append(safe_concat(current_input_list))

            case "technology":
                current_input_list = []
                current_output_list = []
                for index, rows in df_desal.iterrows():
                    lt = rows["lifetime_mid"]
                    tech_args = input2_base_args.copy()
                    tech_args["rule_dfs"] = {"rows": rows, "df_node": df_node}
                    tech_args["lt"] = lt
                    tech_args["node_loc"] = df_node["node"]
                    current_input_list.append(
                        build_standard(r=rule, base_args=tech_args)
                    )

                    output_args = {
                        "rule_dfs": rows,
                        "lt": lt,
                        "node_loc": df_node["node"],
                        "sub_time": series_sub_time,
                        "first_year": first_year,
                        "year_wat": year_wat,
                    }
                    current_output_list.append(
                        build_standard(r=rule_output_tech, base_args=output_args)
                    )
                inp_df_list.append(safe_concat(current_input_list))
                out_df_list.append(safe_concat(current_output_list))

    inp_df = safe_concat(inp_df_list)
    inp_df = inp_df.dropna()

    out_df = safe_concat(out_df_list)
    out_df = out_df.dropna()

    return inp_df, out_df


def _calculate_desal_bound_lo(df_hist, series_sub_time, year_wat):
    """Calculate desalination lower bound activity parameters."""
    df_bound = df_hist[df_hist["year"] == 2015]
    # DESALINATION_BOUND_LO_RULES.change_unit("km3/year")
    for rule in DESALINATION_BOUND_LO_RULES.get_rule():
        bound_lo_args = {
            "rule_dfs": df_bound,
            "sub_time": series_sub_time,
        }
        extra_args_new = {"year_act": year_wat}
        bound_lo = build_standard(
            r=rule, base_args=bound_lo_args, extra_args=extra_args_new
        )

    bound_lo = bound_lo[bound_lo["year_act"] <= 2030]

    # Divide the historical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    bound_lo["value"] = bound_lo["value"] / 5
    return bound_lo


def add_desalination(context: "Context") -> dict[str, pd.DataFrame]:
    """Add desalination infrastructure
    Two types of desalination are considered;
    1. Membrane
    2. Distillation

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010."""
    # define an empty dictionary
    results = {}
    info = context["water build info"]
    year_wat = (2010, 2015, *info.Y)

    # Paths to data files
    path_desal_excel = package_data_path("water", "infrastructure", "desalination.xlsx")
    path_hist_csv = package_data_path(
        "water",
        "infrastructure",
        f"historical_capacity_desalination_km3_year_{context.regions}.csv",
    )
    path_proj_csv = package_data_path(
        "water",
        "infrastructure",
        f"projected_desalination_potential_km3_year_{context.regions}.csv",
    )
    path_basins_csv = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )

    # Reading dataframes
    df_desal = pd.read_excel(path_desal_excel)
    df_hist = pd.read_csv(path_hist_csv)
    df_proj = pd.read_csv(path_proj_csv)

    # Filter and process projected data
    df_proj = df_proj[df_proj["rcp"] == f"{context.RCP}"]
    df_proj = df_proj[~df_proj["year"].isin([2065, 2075])]  # Drop specific years
    df_proj.reset_index(inplace=True, drop=True)
    df_proj = df_proj[df_proj["year"].isin(info.Y)]

    # Reading basin delineation data
    df_node = pd.read_csv(path_basins_csv)
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    # first activity year for all water technologies is 2020
    first_year = context.get_scenario().firstmodelyear

    series_sub_time = pd.Series(context.time)

    # Calculate output
    # Default lifetime (lt=20) seems arbitrary here, may need context?
    out_df = _calculate_desal_output(
        df_desal, df_node, series_sub_time, first_year, year_wat, lt=20
    )

    # Calculate technical lifetime
    tl = _calculate_desal_tl(df_desal, df_node, year_wat)
    results["technical_lifetime"] = tl

    # Calculate historical capacity
    df_hist_cap = _calculate_desal_hist_cap(df_hist, df_node)
    results["historical_new_capacity"] = df_hist_cap

    # Calculate upper bound capacity
    bound_up = _calculate_desal_bound_up(df_proj, df_node)
    results["bound_total_capacity_up"] = bound_up

    # Calculate investment costs
    inv_cost = _calculate_desal_inv_cost(df_desal, df_node, year_wat)
    results["inv_cost"] = inv_cost

    # Calculate fixed and variable costs
    fix_cost, var_cost = _calculate_desal_fix_var_cost(
        df_desal, df_node, series_sub_time, first_year, year_wat
    )
    results["fix_cost"] = fix_cost
    results["var_cost"] = var_cost

    cons_time = make_matched_dfs(tl, construction_time=3)
    results["construction_time"] = cons_time["construction_time"]

    # Calculate input and updated output parameters
    inp_df, out_df_updated = _calculate_desal_input_output(
        df_desal, df_node, series_sub_time, first_year, year_wat, out_df
    )
    results["input"] = inp_df
    results["output"] = out_df_updated

    # Calculate lower activity bound
    bound_lo = _calculate_desal_bound_lo(df_hist, series_sub_time, year_wat)
    results["bound_activity_lo"] = bound_lo

    return results
