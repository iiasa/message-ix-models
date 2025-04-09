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
from message_ix_models.model.water.data.infrastructure_utils import standard_operation
from message_ix_models.util import (
    make_matched_dfs,
    package_data_path,
)


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
    skip_kwargs = ["condition", "pipe"]
    args = {
        "skip_kwargs": skip_kwargs,
        "node_loc": df_node,
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
        "node_loc_arg": "node",
        "lt_arg": "technical_lifetime_mid",
    }
    for rule in INPUT_DATAFRAME_STAGE1.get_rule():
        args_rule = {**args, **rule["pipe"]}
        match (rule["condition"], sdg):
            # non elec commodities excecuted by default
            case "default", _ :
                for index, rows in df_non_elec.iterrows():
                    dfs = {"rows": rows, "df_node": df_node}
                    inp_df.append(
                        standard_operation(rule=rule, rule_dfs=dfs,
                        default_df_key="rows", lt = rows, **args_rule)
                    ),


            case "baseline_main", "baseline":
                # baseline case
                for index, rows in df_dist.iterrows():
                    dfs = {"rows": rows, "df_node": df_node}
                    inp_df.append(
                        standard_operation(rule=rule, rule_dfs=dfs,
                        default_df_key="rows", lt = rows, **args_rule)
                    )
            case "baseline_additional", "baseline":
                # baseline case additional
                #takes the final row from df_dist as input
                inp_df.append(
                standard_operation(rule=rule, rule_dfs=df_dist.iloc[-1],
                lt = df_dist.iloc[-1], **args_rule)
                )
            # non baseline case
            case "!baseline", _ if sdg != "baseline":
                for index, rows in df_dist.iterrows():
                    dfs = {"rows": rows, "df_node": df_node}
                    inp_df.append(
                    standard_operation(rule=rule, rule_dfs=dfs, default_df_key= "rows",
                    lt = rows, **args_rule)
                    )
                    return pd.concat(inp_df) # Terminates in the non-baseline case

    return pd.concat(inp_df)


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
    skip_kwargs = ["condition", "pipe"]
    args = {
        "skip_kwargs": skip_kwargs,
        "node_loc": df_node,
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": sub_time,
        "node_loc_arg": "node",
    }
    for _, rows in df_elec.iterrows():
        dfs = {"rows": rows, "df_node": df_node}
        is_tech = rows["tec"] in techs
        for rule in INPUT_DATAFRAME_STAGE2.get_rule():
            match (context.SDG, rule["condition"], is_tech):
                case _, "!baseline", True if context.SDG != "baseline":
                    args_rule = {**args, **rule["pipe"]}
                    inp = standard_operation(rule=rule, rule_dfs=dfs,
                    default_df_key="rows", lt = 1, **args_rule)
                    result_dc["input"].append(inp)
                case "baseline", "baseline_p1" | "baseline_p2", True:
                    args_rule = {**args, **rule["pipe"]}
                    inp = standard_operation(rule=rule, rule_dfs=dfs,
                    default_df_key="rows", lt = 1, **args_rule)
                    result_dc["input"].append(inp)
                case _, "non_tech", False:
                    args_rule = {**args, **rule["pipe"]}
                    inp = standard_operation(rule=rule, rule_dfs=dfs,
                    default_df_key="rows", lt = 1, **args_rule)
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

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = pd.concat([inp_df, results_new["input"]])
    results["input"] = inp_df

    # add output dataframe
    df_out = df[~df["outcmd"].isna()]
    df_out_dist = df_out[df_out["tec"].isin(techs)]
    df_out = df_out[~df_out["tec"].isin(techs)]

    out_df_list = [pd.DataFrame([])]
    skip_kwargs = ["condition", "pipe"]

    args= {
    "skip_kwargs": skip_kwargs,
    "node_loc": df_node,
    "year_wat": year_wat,
    "first_year": first_year,
    "sub_time": sub_time,
    "lt_arg": "technical_lifetime_mid",
    "node_loc_arg": "node",
    }

    for rule in OUTPUT_RULES.get_rule():
        args ={**args, **rule["pipe"]}
        match (context.SDG, rule["condition"]):
            case(_, "default"):
                for index, rows in df_out.iterrows():
                    out_df_list.append(
                    standard_operation(rule=rule, rule_dfs=rows, lt=rows, **args))
            case (_, "!baseline") if context.SDG != "baseline":
                out_df_list.append(
                standard_operation(rule=rule, rule_dfs=df_out_dist, lt=rows, **args))
            case ("baseline", "baseline_p1" | "baseline_p2"):
                out_df_list.append(
                standard_operation(rule=rule, rule_dfs=df_out_dist, lt=rows, **args))

    out_df = pd.concat(out_df_list)

    results["output"] = out_df

    # Filtering df for capacity factors
    df_cap = df.dropna(subset=["capacity_factor_mid"])
    cap_list = [pd.DataFrame([])]

    # Adding capacity factor dataframe
    for rule in CAP_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        for index, rows in df_cap.iterrows():
            cap_list.append(standard_operation(rule=rule, rule_dfs=rows, lt=rows, **args))
    cap_df = pd.concat(cap_list)
    results["capacity_factor"] = cap_df

    # Filtering df for capacity factors
    df_tl = df.dropna(subset=["technical_lifetime_mid"])
    extra_args = {"year_vtg":year_wat}
    for rule in TL_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        tl = standard_operation(rule=rule, rule_dfs=df_tl, extra_args= extra_args, **args)
    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=1)
    results["construction_time"] = cons_time["construction_time"]

    # Investment costs
    df_inv = df.dropna(subset=["investment_mid"])
    extra_args = {"year_vtg" : year_wat}
    for rule in INV_COST_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        # Prepare dataframe for investments
        inv_cost = standard_operation(rule= rule, rule_dfs=df_inv,
        extra_args= extra_args, **args)
        inv_cost = inv_cost[~inv_cost["technology"].isin(techs)]
    results["inv_cost"] = inv_cost

    # Fixed costs
    # Prepare data frame for fix_cost
    fix_cost_list = [pd.DataFrame([])]
    for rule in FIX_COST_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        for index, rows in df_inv.iterrows():
            fix_cost_list.append(standard_operation(rule=rule, rule_dfs= df_inv,
            lt=rows, **args))
    fix_cost = pd.concat(fix_cost_list)
    fix_cost = fix_cost[~fix_cost["technology"].isin(techs)]

    results["fix_cost"] = fix_cost

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]
    var_cost_list = [pd.DataFrame([])]

    var_cost = _add_var_cost(context, df_var, df_var_dist, df_node,
    year_wat, first_year, sub_time, var_cost_list, args)

    results["var_cost"] = var_cost

    return results


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

    rules_baseline_dist= [] # collecting the two rules for baseline_dist_p1 and baseline_dist_p2
        # Handle non-baseline case
    rows = pd.Series() # dummy series
    for rule in VAR_COST_RULES.get_rule():
        args = {**args, **rule["pipe"]}
        match (sdg, rule["condition"]):
            case (_, "!baseline") if sdg != "baseline":
                for index, rows in df_var.iterrows():
                    # Variable cost
                    var_cost_list.append(
                        standard_operation(rule=rule, rule_dfs=rows,
                        default_df_key="rows", lt=rows, **args))
            case (_, "!baseline_dist") if sdg != "baseline":
                for index, rows in df_var_dist.iterrows():
                    var_cost_list.append(
                        standard_operation(rule=rule, rule_dfs=rows,
                        default_df_key="rows", lt=rows, **args))
            case ("baseline", "baseline_main"):
                for index, rows in df_var.iterrows():
                    dfs = {"rows": rows, "df_var": df_var}
                    var_cost_list.append(
                    standard_operation(rule=rule, rule_dfs=dfs, default_df_key="rows",
                    lt=rows, **args))
            case ("baseline", "baseline_dist_p1" | "baseline_dist_p2"):
                #collecting both rules because they are implemented in the same function
                rules_baseline_dist.append(rule)
                if len(rules_baseline_dist) == 2:
                    rule = rules_baseline_dist[0]
                    rule_alt = rules_baseline_dist[1]
                    # Apply both p1 and p2 rules for each row in df_var_dist
                    for index, rows in df_var_dist.iterrows():
                        var_cost_list.append(
                        standard_operation(rule=rule, rule_dfs=rows, lt=rows, **args))
                        var_cost_list.append(
                        standard_operation(rule=rule_alt, rule_dfs=rows, lt=rows, **args))

    var_cost = pd.concat(var_cost_list)
    return var_cost

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
        ``context["water build info"]``, plus the additional year 2010.
    """
    # define an empty dictionary
    results = {}
    sub_time = context.time
    # Reference to the water configuration
    info = context["water build info"]

    # load the scenario from context
    scen = context.get_scenario()

    year_wat = (2010, 2015, *info.Y)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    # Reading water distribution mapping from csv
    path = package_data_path("water", "infrastructure", "desalination.xlsx")
    path2 = package_data_path(
        "water",
        "infrastructure",
        f"historical_capacity_desalination_km3_year_{context.regions}.csv",
    )
    path3 = package_data_path(
        "water",
        "infrastructure",
        f"projected_desalination_potential_km3_year_{context.regions}.csv",
    )
    # Reading dataframes
    df_desal = pd.read_excel(path)
    df_hist = pd.read_csv(path2)
    df_proj = pd.read_csv(path3)
    df_proj = df_proj[df_proj["rcp"] == f"{context.RCP}"]
    df_proj = df_proj[~(df_proj["year"] == 2065) & ~(df_proj["year"] == 2075)]
    df_proj.reset_index(inplace=True, drop=True)
    df_proj = df_proj[df_proj["year"].isin(info.Y)]

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

    skip_kwargs = ["condition", "pipe"]

    args = {
        "skip_kwargs": skip_kwargs,
        "node_loc": df_node,
        "year_wat": year_wat,
        "first_year": first_year,
        "sub_time": pd.Series(sub_time),
        "node_loc_arg": "node",
        "lt_arg": "lifetime_mid",
    }

    for rule in DESALINATION_OUTPUT_RULES.get_rule():
        args_rule = {**args, **rule["pipe"]}
        out_df = standard_operation(rule=rule, rule_dfs=df_desal, lt =20, **args_rule)
    # output dataframe linking to desal tech types

    extra_args_tl = {"year_vtg": year_wat}
    tl = [pd.DataFrame([])]
    for rule in  TL_DESALINATION_RULES.get_rule() :
        args_rule = {**args, **rule["pipe"]}
        tl.append(standard_operation(rule=rule, rule_dfs=df_desal,
        extra_args=extra_args_tl, **args_rule))


    tl = pd.concat(tl)
    results["technical_lifetime"] = tl
    for rule in DESALINATION_HISTORICAL_CAPACITY_RULES.get_rule():
        args_rule = {**args, **rule["pipe"]}
        df_hist_cap = standard_operation(rule=rule, rule_dfs=df_hist, **args_rule)

    # Divide the historical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    df_hist_cap["value"] = df_hist_cap["value"] / 5

    results["historical_new_capacity"] = df_hist_cap
    # Desalination potentials are added as an upper bound
    # to limit the salinewater extraction
    for rule in DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES.get_rule():
        args_rule = {**args, **rule["pipe"]}
        bound_up = standard_operation(rule=rule, rule_dfs=df_proj, **args_rule)

    # Making negative values zero
    bound_up["value"].clip(lower=0)
    # Bound should start from 2025
    bound_up = bound_up[bound_up["year_act"] > 2020]

    results["bound_total_capacity_up"] = bound_up

    # Investment costs
    for rule in DESALINATION_INV_COST_RULES.get_rule():
        args_rule = {**args, **rule["pipe"]}
        extra_args = {"year_vtg": year_wat}
        inv_cost = standard_operation(rule=rule, rule_dfs=df_desal,
        extra_args=extra_args, **args_rule)
    results["inv_cost"] = inv_cost

    fix_cost_list = [pd.DataFrame([])]
    var_cost_list = [pd.DataFrame([])]
    for index, rows in df_desal.iterrows():
        # Fixed costs
        # Prepare dataframe for fix_cost
        for rule in FIX_COST_DESALINATION_RULES.get_rule():
            args_rule = {**args, **rule["pipe"]}
            fix_cost_list.append(standard_operation(rule=rule, rule_dfs=rows,
            lt = rows, **args_rule))

        # Variable cost
        for rule in VAR_COST_DESALINATION_RULES.get_rule():
            if rule["condition"] != "SKIP":
                args_rule = {**args, **rule["pipe"]}
                var_cost_list.append(standard_operation(rule=rule,
                rule_dfs=rows, lt = rows, **args_rule))

    results["var_cost"] = pd.concat(var_cost_list)
    results["fix_cost"] = pd.concat(fix_cost_list)

    cons_time = make_matched_dfs(tl, construction_time=3)
    results["construction_time"] = cons_time["construction_time"]

    skip_kwargs = ["condition", "pipe"]
    result_dc = defaultdict(list)
    for rule in DESALINATION_INPUT_RULES2.get_rule():
        match rule["condition"]:
            case "electricity":
                for index, rows in df_desal.iterrows():
                    args_rule = {**args, **rule["pipe"]}
                    rule_dfs = {"rows": rows, "df_node": df_node}
                    inp = standard_operation(
                        rule=rule, rule_dfs=rule_dfs,
                        default_df_key="rows", lt = rows, **args_rule)

                    result_dc["input"].append(inp)
                    results_new = {
                        par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

                    inp_df = results_new["input"]

            # Adding input dataframe
            case "heat":
                df_heat = df_desal[df_desal["heat_input_mid"] > 0]

                result_dc = defaultdict(list)
                for index, rows in df_heat.iterrows():
                    args_rule = {**args, **rule["pipe"]}
                    rule_dfs = {"rows": rows, "df_node": df_node}
                    inp = standard_operation(rule=rule, rule_dfs=rule_dfs,
                    default_df_key="rows", lt = rows, **args_rule)

                    result_dc["input"].append(inp)

                    results_new = {
                        par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}
                    inp_df_list =[]
                    inp_df_list.append(inp_df)
                    inp_df_list.append(results_new["input"])

                    out_df_list = []
                    out_df_list.append(out_df)
            # Adding input dataframe
            case "technology":

                rule_output = DESALINATION_OUTPUT_RULES2.get_rule()[0]

                for index, rows in df_desal.iterrows():
                    args_rule = {**args, **rule["pipe"]}
                    inp_df_list.append(standard_operation(rule=rule, rule_dfs=rows,
                    lt = rows, **args_rule))
                    inp_df = pd.concat(inp_df_list)
                    inp_df.dropna()

                    results["input"] = inp_df
                    args_rule = {**args, **rule_output["pipe"]}
                    out_df_list.append(
                    standard_operation(rule=rule_output, rule_dfs=rows,
                    lt = rows, **args_rule))
                    out_df = pd.concat(out_df_list)
                    out_df.dropna()

                    results["output"] = out_df

    # putting a lower bound on desalination tecs based on hist capacities
    df_bound = df_hist[df_hist["year"] == 2015]

    for rule in DESALINATION_BOUND_LO_RULES.get_rule():
        args_rule = {**args, **rule["pipe"]}
        extra_args_new = {"year_act": year_wat}
        bound_lo = standard_operation(rule=rule, rule_dfs=df_bound,
        extra_args=extra_args_new, **args_rule)

    bound_lo = bound_lo[bound_lo["year_act"] <= 2030]

    # Divide the histroical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    bound_lo["value"] = bound_lo["value"] / 5

    results["bound_activity_lo"] = bound_lo

    return results

