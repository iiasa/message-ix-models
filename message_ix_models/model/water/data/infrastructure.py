"""Prepare data for adding techs related to water distribution,
 treatment in urban & rural"""

from collections import defaultdict

import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    private_data_path,
    same_node,
    same_time,
)


def add_infrastructure_techs(context):  # noqa: C901
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
    # TODO reduce complexity of this function from 18 to 15 or less
    # Reference to the water configuration
    info = context["water build info"]

    # define an empty dictionary
    results = {}
    sub_time = context.time
    # load the scenario from context
    scen = context.get_scenario()

    year_wat = [2010, 2015]
    year_wat.extend(info.Y)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = private_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    if context.type_reg == "country":
        df_node["region"] = context.map_ISO_c[context.regions]
    else:
        df_node["region"] = f"{context.regions}_" + df_node["REGION"].astype(str)

    # Reading water distribution mapping from csv
    path = private_data_path("water", "infrastructure", "water_distribution.xlsx")
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

    inp_df = pd.DataFrame([])

    # Input Dataframe for non elec commodities
    for index, rows in df_non_elec.iterrows():
        inp_df = pd.concat(
            [
                inp_df,
                (
                    make_df(
                        "input",
                        technology=rows["tec"],
                        value=rows["value_mid"],
                        unit="-",
                        level=rows["inlvl"],
                        commodity=rows["incmd"],
                        mode="M1",
                        node_loc=df_node["node"],
                    )
                    .pipe(
                        broadcast,
                        map_yv_ya_lt(
                            year_wat, rows["technical_lifetime_mid"], first_year
                        ),
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                ),
            ]
        )

    if context.SDG:
        for index, rows in df_dist.iterrows():
            inp_df = pd.concat(
                [
                    inp_df,
                    (
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_high"],
                            unit="-",
                            level=rows["inlvl"],
                            commodity=rows["incmd"],
                            mode="Mf",
                        )
                        .pipe(
                            broadcast,
                            map_yv_ya_lt(
                                year_wat, rows["technical_lifetime_mid"], first_year
                            ),
                            node_loc=df_node["node"],
                            time=sub_time,
                        )
                        .pipe(same_node)
                        .pipe(same_time)
                    ),
                ]
            )
    else:
        for index, rows in df_dist.iterrows():
            inp_df = pd.concat(
                [
                    inp_df,
                    (
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_mid"],
                            unit="-",
                            level=rows["inlvl"],
                            commodity=rows["incmd"],
                            mode="M1",
                        )
                        .pipe(
                            broadcast,
                            map_yv_ya_lt(
                                year_wat, rows["technical_lifetime_mid"], first_year
                            ),
                            node_loc=df_node["node"],
                            time=sub_time,
                        )
                        .pipe(same_node)
                        .pipe(same_time)
                    ),
                ]
            )

            inp_df = inp_df.append(
                (
                    make_df(
                        "input",
                        technology=rows["tec"],
                        value=rows["value_high"],
                        unit="-",
                        level=rows["inlvl"],
                        commodity=rows["incmd"],
                        mode="Mf",
                    )
                    .pipe(
                        broadcast,
                        map_yv_ya_lt(
                            year_wat, rows["technical_lifetime_mid"], first_year
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                )
            )
    result_dc = defaultdict(list)

    for index, rows in df_elec.iterrows():
        if rows["tec"] in techs:
            if context.SDG:
                inp = make_df(
                    "input",
                    technology=rows["tec"],
                    value=rows["value_high"],
                    unit="-",
                    level="final",
                    commodity="electr",
                    mode="Mf",
                    time_origin="year",
                    node_loc=df_node["node"],
                    node_origin=df_node["region"],
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(
                        year_wat,
                        # 1 because elec commodities don't have technical lifetime
                        1,
                        first_year,
                    ),
                    time=sub_time,
                )

                result_dc["input"].append(inp)
            else:
                inp = make_df(
                    "input",
                    technology=rows["tec"],
                    value=rows["value_high"],
                    unit="-",
                    level="final",
                    commodity="electr",
                    mode="Mf",
                    time_origin="year",
                    node_loc=df_node["node"],
                    node_origin=df_node["region"],
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(
                        year_wat,
                        # 1 because elec commodities don't have technical lifetime
                        1,
                        first_year,
                    ),
                    time=sub_time,
                )

                inp = inp.append(
                    make_df(
                        "input",
                        technology=rows["tec"],
                        value=rows["value_mid"],
                        unit="-",
                        level="final",
                        commodity="electr",
                        mode="M1",
                        time_origin="year",
                        node_loc=df_node["node"],
                        node_origin=df_node["region"],
                    ).pipe(
                        broadcast,
                        # 1 because elec commodities don't have technical lifetime
                        map_yv_ya_lt(year_wat, 1, first_year),
                        time=sub_time,
                    )
                )

                result_dc["input"].append(inp)
        else:
            inp = make_df(
                "input",
                technology=rows["tec"],
                value=rows["value_mid"],
                unit="-",
                level="final",
                commodity="electr",
                mode="M1",
                time_origin="year",
                node_loc=df_node["node"],
                node_origin=df_node["region"],
            ).pipe(
                broadcast,
                map_yv_ya_lt(year_wat, 1, first_year),
                time=sub_time,
            )

            result_dc["input"].append(inp)

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = inp_df.append(results_new["input"])
    # inp_df.dropna(inplace = True)
    results["input"] = inp_df

    # add output dataframe
    df_out = df[~df["outcmd"].isna()]
    df_out_dist = df_out[df_out["tec"].isin(techs)]
    df_out = df_out[~df_out["tec"].isin(techs)]

    out_df = pd.DataFrame([])
    for index, rows in df_out.iterrows():
        out_df = out_df.append(
            (
                make_df(
                    "output",
                    technology=rows["tec"],
                    value=rows["out_value_mid"],
                    unit="-",
                    level=rows["outlvl"],
                    commodity=rows["outcmd"],
                    mode="M1",
                )
                .pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
                .pipe(same_node)
                .pipe(same_time)
            )
        )

    if context.SDG:
        out_df = out_df.append(
            make_df(
                "output",
                technology=df_out_dist["tec"],
                value=df_out_dist["out_value_mid"],
                unit="-",
                level=df_out_dist["outlvl"],
                commodity=df_out_dist["outcmd"],
                mode="Mf",
            )
            .pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                node_loc=df_node["node"],
                time=sub_time,
            )
            .pipe(same_node)
            .pipe(same_time)
        )
    else:
        out_df = out_df.append(
            make_df(
                "output",
                technology=df_out_dist["tec"],
                value=df_out_dist["out_value_mid"],
                unit="-",
                level=df_out_dist["outlvl"],
                commodity=df_out_dist["outcmd"],
                mode="M1",
            )
            .pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                node_loc=df_node["node"],
                time=sub_time,
            )
            .pipe(same_node)
            .pipe(same_time)
        )
        out_df = out_df.append(
            make_df(
                "output",
                technology=df_out_dist["tec"],
                value=df_out_dist["out_value_mid"],
                unit="-",
                level=df_out_dist["outlvl"],
                commodity=df_out_dist["outcmd"],
                mode="Mf",
            )
            .pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                node_loc=df_node["node"],
                time=sub_time,
            )
            .pipe(same_node)
            .pipe(same_time)
        )

    results["output"] = out_df

    # Filtering df for capacity factors
    df_cap = df.dropna(subset=["capacity_factor_mid"])
    cap_df = pd.DataFrame([])
    # Adding capacity factor dataframe
    for index, rows in df_cap.iterrows():
        cap_df = cap_df.append(
            make_df(
                "capacity_factor",
                technology=rows["tec"],
                value=rows["capacity_factor_mid"],
                unit="%",
            )
            .pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                node_loc=df_node["node"],
                time=sub_time,
            )
            .pipe(same_node)
        )

    results["capacity_factor"] = cap_df

    # Filtering df for capacity factors
    df_tl = df.dropna(subset=["technical_lifetime_mid"])

    tl = (
        make_df(
            "technical_lifetime",
            technology=df_tl["tec"],
            value=df_tl["technical_lifetime_mid"],
            unit="y",
        )
        .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
        .pipe(same_node)
    )

    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=1)
    results["construction_time"] = cons_time["construction_time"]

    # Investment costs
    df_inv = df.dropna(subset=["investment_mid"])

    # Prepare dataframe for investments
    inv_cost = make_df(
        "inv_cost",
        technology=df_inv["tec"],
        value=df_inv["investment_mid"],
        unit="USD/km3",
    ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs)]
    results["inv_cost"] = inv_cost

    # Fixed costs
    # Prepare data frame for fix_cost
    fix_cost = pd.DataFrame([])
    var_cost = pd.DataFrame([])

    for index, rows in df_inv.iterrows():
        fix_cost = fix_cost.append(
            make_df(
                "fix_cost",
                technology=df_inv["tec"],
                value=df_inv["fix_cost_mid"],
                unit="USD/km3",
            ).pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                node_loc=df_node["node"],
            )
        )

        fix_cost = fix_cost[~fix_cost["technology"].isin(techs)]

        results["fix_cost"] = fix_cost

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    if context.SDG:
        for index, rows in df_var.iterrows():
            # Variable cost
            var_cost = var_cost.append(
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=rows["var_cost_mid"],
                    unit="USD/km3",
                    mode="M1",
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
            )

        # Variable cost for distribution technologies
        for index, rows in df_var_dist.iterrows():
            var_cost = var_cost.append(
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=rows["var_cost_high"],
                    unit="USD/km3",
                    mode="Mf",
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
            )
        results["var_cost"] = var_cost
    else:
        # Variable cost
        for index, rows in df_var.iterrows():
            var_cost = var_cost.append(
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=df_var["var_cost_mid"],
                    unit="USD/km3",
                    mode="M1",
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
            )

        for index, rows in df_var_dist.iterrows():
            var_cost = var_cost.append(
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=rows["var_cost_mid"],
                    unit="USD/km3",
                    mode="M1",
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
            )

            var_cost = var_cost.append(
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=rows["var_cost_high"],
                    unit="USD/km3",
                    mode="Mf",
                ).pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["technical_lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
            )
        results["var_cost"] = var_cost

    return results


def add_desalination(context):
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

    year_wat = [2010, 2015]
    year_wat.extend(info.Y)

    # first activity year for all water technologies is 2020
    first_year = scen.firstmodelyear

    # Reading water distribution mapping from csv
    path = private_data_path("water", "infrastructure", "desalination.xlsx")
    path2 = private_data_path(
        "water",
        "infrastructure",
        f"historical_capacity_desalination_km3_year_{context.regions}.csv",
    )
    path3 = private_data_path(
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
    PATH = private_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    if context.type_reg == "country":
        df_node["region"] = context.map_ISO_c[context.regions]
    else:
        df_node["region"] = f"{context.regions}_" + df_node["REGION"].astype(str)
    # output dataframe linking to desal tech types
    out_df = (
        make_df(
            "output",
            technology="extract_salinewater_basin",
            value=1,
            unit="km3/year",
            level="water_avail_basin",
            commodity="salinewater_basin",
            mode="M1",
        )
        .pipe(
            broadcast,
            map_yv_ya_lt(year_wat, 20, first_year),
            node_loc=df_node["node"],
            time=sub_time,
        )
        .pipe(same_node)
        .pipe(same_time)
    )

    tl = (
        make_df(
            "technical_lifetime",
            technology="extract_salinewater_basin",
            value=20,
            unit="y",
        )
        .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
        .pipe(same_node)
    )

    # Historical capacity of desalination technologies
    df_hist_cap = make_df(
        "historical_new_capacity",
        node_loc="B" + df_hist["BCU_name"],
        technology=df_hist["tec_type"],
        year_vtg=df_hist["year"],
        value=df_hist["cap_km3_year"],
        unit="km3/year",
    )
    # Divide the historical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    df_hist_cap["value"] = df_hist_cap["value"] / 5

    results["historical_new_capacity"] = df_hist_cap

    # Desalination potentials are added as an upper bound
    # to limit the salinewater extraction
    bound_up = make_df(
        "bound_total_capacity_up",
        node_loc="B" + df_proj["BCU_name"],
        technology="extract_salinewater_basin",
        year_act=df_proj["year"],
        value=df_proj["cap_km3_year"],
        unit="km3/year",
    )
    # Making negative values zero
    bound_up["value"].clip(lower=0, inplace=True)
    # Bound should start from 2025
    bound_up = bound_up[bound_up["year_act"] > 2020]

    results["bound_total_capacity_up"] = bound_up
    # Investment costs
    inv_cost = make_df(
        "inv_cost",
        technology=df_desal["tec"],
        value=df_desal["inv_cost_mid"],
        unit="USD/km3",
    ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])

    results["inv_cost"] = inv_cost

    fix_cost = pd.DataFrame([])
    var_cost = pd.DataFrame([])
    for index, rows in df_desal.iterrows():
        # Fixed costs
        # Prepare dataframe for fix_cost
        fix_cost = fix_cost.append(
            make_df(
                "fix_cost",
                technology=rows["tec"],
                value=rows["fix_cost_mid"],
                unit="USD/km3",
            ).pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
                node_loc=df_node["node"],
            )
        )

        results["fix_cost"] = fix_cost

        # Variable cost
        var_cost = var_cost.append(
            make_df(
                "var_cost",
                technology=rows["tec"],
                value=rows["var_cost_mid"],
                unit="USD/km3",
                mode="M1",
            ).pipe(
                broadcast,
                map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
                node_loc=df_node["node"],
                time=sub_time,
            )
        )

    # Dummy  Variable cost for salinewater extrqction
    # var_cost = var_cost.append(
    #     make_df(
    #     "var_cost",
    #     technology='extract_salinewater_basin',
    #     value= 100,
    #     unit="USD/km3",
    #     mode="M1",
    #     time="year",
    # ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat, node_loc=df_node["node"])
    # )

    results["var_cost"] = var_cost

    tl = tl.append(
        (
            make_df(
                "technical_lifetime",
                technology=df_desal["tec"],
                value=df_desal["lifetime_mid"],
                unit="y",
            )
            .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
            .pipe(same_node)
        )
    )

    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=3)
    results["construction_time"] = cons_time["construction_time"]

    from collections import defaultdict

    result_dc = defaultdict(list)

    for index, rows in df_desal.iterrows():
        inp = make_df(
            "input",
            technology=rows["tec"],
            value=rows["electricity_input_mid"],
            unit="-",
            level="final",
            commodity="electr",
            mode="M1",
            time_origin="year",
            node_loc=df_node["node"],
            node_origin=df_node["region"],
        ).pipe(
            broadcast,
            map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
            time=sub_time,
        )

        result_dc["input"].append(inp)

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = results_new["input"]

    # Adding input dataframe
    df_heat = df_desal[df_desal["heat_input_mid"] > 0]

    result_dc = defaultdict(list)

    for index, rows in df_heat.iterrows():
        inp = make_df(
            "input",
            technology=rows["tec"],
            value=rows["heat_input_mid"],
            unit="-",
            level="final",
            commodity="d_heat",
            mode="M1",
            time_origin="year",
            node_loc=df_node["node"],
            node_origin=df_node["region"],
        ).pipe(
            broadcast,
            map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
            time=sub_time,
        )

        result_dc["input"].append(inp)

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = inp_df.append(results_new["input"])

    # Adding input dataframe
    for index, rows in df_desal.iterrows():
        inp_df = inp_df.append(
            (
                make_df(
                    "input",
                    technology=rows["tec"],
                    value=1,
                    unit="-",
                    level=rows["inlvl"],
                    commodity=rows["incmd"],
                    mode="M1",
                )
                .pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
                .pipe(same_node)
                .pipe(same_time)
            )
        )

        inp_df.dropna(inplace=True)

        results["input"] = inp_df

        out_df = out_df.append(
            (
                make_df(
                    "output",
                    technology=rows["tec"],
                    value=1,
                    unit="-",
                    level=rows["outlvl"],
                    commodity=rows["outcmd"],
                    mode="M1",
                )
                .pipe(
                    broadcast,
                    map_yv_ya_lt(year_wat, rows["lifetime_mid"], first_year),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
                .pipe(same_node)
                .pipe(same_time)
            )
        )

        results["output"] = out_df

    # putting a lower bound on desalination tecs based on hist capacities
    df_bound = df_hist[df_hist["year"] == 2015]
    bound_lo = make_df(
        "bound_activity_lo",
        node_loc="B" + df_bound["BCU_name"],
        technology=df_bound["tec_type"],
        mode="M1",
        value=df_bound["cap_km3_year"],
        unit="km3/year",
    ).pipe(
        broadcast,
        year_act=year_wat,
        time=sub_time,
    )

    bound_lo = bound_lo[bound_lo["year_act"] <= 2030]
    # Divide the histroical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    bound_lo["value"] = bound_lo["value"] / 5

    results["bound_activity_lo"] = bound_lo

    return results
