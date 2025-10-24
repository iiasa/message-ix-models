"""Prepare data for adding techs related to water distribution,
treatment in urban & rural"""

from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.water.utils import (
    ANNUAL_CAPACITY_FACTOR,
    KM3_TO_MCM,
    USD_M3DAY_TO_USD_MCM,
    GWa_KM3_TO_GWa_MCM,
    get_vintage_and_active_years,
    kWh_m3_TO_GWa_MCM,
)
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    package_data_path,
    same_node,
    same_time,
)


def is_dummy_technology(df_row) -> bool:
    """Check if a technology is a dummy (has zero investment, fix, and var costs).

    Parameters
    ----------
    df_row : pandas.Series
        Row from the infrastructure CSV containing cost data

    Returns
    -------
    bool
        True if technology has zero costs (is dummy), False otherwise
    """
    return (
        df_row.get("investment_mid", 0) == 0.0
        and df_row.get("fix_cost_mid", 0) == 0.0
        and df_row.get("var_cost_mid", 0) == 0.0
    )


def start_creating_input_dataframe(
    sdg: str,
    df_node: pd.DataFrame,
    df_non_elec: pd.DataFrame,
    df_dist: pd.DataFrame,
    scenario_info: ScenarioInfo,
    sub_time,
) -> pd.DataFrame:
    """Creates an input pd.DataFrame and adds some data to it."""
    inp_df = pd.DataFrame([])
    # Input Dataframe for non elec commodities
    for index, rows in df_non_elec.iterrows():
        # Check if this is a dummy technology
        use_same_year = is_dummy_technology(rows)

        inp_df = pd.concat(
            [
                inp_df,
                (
                    make_df(
                        "input",
                        technology=rows["tec"],
                        value=rows["value_high"],
                        unit="MCM",
                        # MCM as all non elec technology have water as input
                        level=rows["inlvl"],
                        commodity=rows["incmd"],
                        mode="M1",
                        node_loc=df_node["node"],
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                ),
            ]
        )
    if sdg != "baseline":
        for index, rows in df_dist.iterrows():
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            inp_df = pd.concat(
                [
                    inp_df,
                    (
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_mid"],
                            unit="MCM",
                            level=rows["inlvl"],
                            commodity=rows["incmd"],
                            mode="Mf",
                        )
                        .pipe(
                            broadcast,
                            get_vintage_and_active_years(
                                scenario_info,
                                rows["technical_lifetime_mid"],
                                same_year_only=use_same_year,
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
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            # Add M1 mode input
            inp_df = pd.concat(
                [
                    inp_df,
                    (
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_high"],
                            unit="MCM",
                            level=rows["inlvl"],
                            commodity=rows["incmd"],
                            mode="M1",
                        )
                        .pipe(
                            broadcast,
                            get_vintage_and_active_years(
                                scenario_info,
                                rows["technical_lifetime_mid"],
                                same_year_only=use_same_year,
                            ),
                            node_loc=df_node["node"],
                            time=sub_time,
                        )
                        .pipe(same_node)
                        .pipe(same_time)
                    ),
                ]
            )
            # Add Mf mode input for baseline to match Mf output mode
            inp_df = pd.concat(
                [
                    inp_df,
                    (
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_mid"],
                            unit="MCM",
                            level=rows["inlvl"],
                            commodity=rows["incmd"],
                            mode="Mf",
                        )
                        .pipe(
                            broadcast,
                            get_vintage_and_active_years(
                                scenario_info,
                                rows["technical_lifetime_mid"],
                                same_year_only=use_same_year,
                            ),
                            node_loc=df_node["node"],
                            time=sub_time,
                        )
                        .pipe(same_node)
                        .pipe(same_time)
                    ),
                ]
            )

    return inp_df


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
    sub_time = pd.Series(context.time)
    # load the scenario from context
    scen = context.get_scenario()

    # Create ScenarioInfo object for get_vintage_and_active_years
    scenario_info = ScenarioInfo(scen)

    year_wat = (*range(2010, info.Y[0] + 1, 5), *info.Y)

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
    path = package_data_path("water", "infrastructure", "water_distribution.csv")
    df = pd.read_csv(path)

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
        scenario_info=scenario_info,
        sub_time=sub_time,
    )

    result_dc = prepare_input_dataframe(
        context=context,
        sub_time=sub_time,
        scenario_info=scenario_info,
        df_node=df_node,
        techs=techs,
        df_elec=df_elec,
    )

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = pd.concat([inp_df, results_new["input"]])

    # add output dataframe
    df_out = df[~df["outcmd"].isna()]
    df_out_dist = df_out[df_out["tec"].isin(techs)]
    df_out = df_out[~df_out["tec"].isin(techs)]

    out_df = pd.DataFrame([])
    for index, rows in df_out.iterrows():
        # Check if this is a dummy technology
        use_same_year = is_dummy_technology(rows)

        out_df = pd.concat(
            [
                out_df,
                (
                    make_df(
                        "output",
                        technology=rows["tec"],
                        value=rows["out_value_mid"],
                        unit="MCM",
                        level=rows["outlvl"],
                        commodity=rows["outcmd"],
                        mode="M1",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                ),
            ]
        )

    if context.SDG != "baseline":
        for index, dist_rows in df_out_dist.iterrows():
            # Check if this is a dummy distribution technology
            use_same_year_dist = is_dummy_technology(dist_rows)

            out_df = pd.concat(
                [
                    out_df,
                    make_df(
                        "output",
                        technology=dist_rows["tec"],
                        value=dist_rows["out_value_mid"],
                        unit="MCM",
                        level=dist_rows["outlvl"],
                        commodity=dist_rows["outcmd"],
                        mode="Mf",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            dist_rows["technical_lifetime_mid"],
                            same_year_only=use_same_year_dist,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time),
                ]
            )
    else:
        for index, dist_rows in df_out_dist.iterrows():
            # Check if this is a dummy distribution technology
            use_same_year_dist = is_dummy_technology(dist_rows)

            # Add M1 mode output
            out_df = pd.concat(
                [
                    out_df,
                    make_df(
                        "output",
                        technology=dist_rows["tec"],
                        value=dist_rows["out_value_high"],
                        unit="MCM",
                        level=dist_rows["outlvl"],
                        commodity=dist_rows["outcmd"],
                        mode="M1",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            dist_rows["technical_lifetime_mid"],
                            same_year_only=use_same_year_dist,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time),
                ]
            )
            # Add Mf mode output
            out_df = pd.concat(
                [
                    out_df,
                    make_df(
                        "output",
                        technology=dist_rows["tec"],
                        value=dist_rows["out_value_mid"],
                        unit="MCM",
                        level=dist_rows["outlvl"],
                        commodity=dist_rows["outcmd"],
                        mode="Mf",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            dist_rows["technical_lifetime_mid"],
                            same_year_only=use_same_year_dist,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    )
                    .pipe(same_node)
                    .pipe(same_time),
                ]
            )

    results["output"] = out_df

    # Filtering df for capacity factors
    df_cap = df.dropna(subset=["capacity_factor_mid"])
    cap_df = pd.DataFrame([])
    # Adding capacity factor dataframe
    for index, rows in df_cap.iterrows():
        # Check if this is a dummy technology
        use_same_year = is_dummy_technology(rows)

        cap_df = pd.concat(
            [
                cap_df,
                make_df(
                    "capacity_factor",
                    technology=rows["tec"],
                    value=rows["capacity_factor_mid"],
                    unit="%",
                )
                .pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        rows["technical_lifetime_mid"],
                        same_year_only=use_same_year,
                    ),
                    node_loc=df_node["node"],
                    time=sub_time,
                )
                .pipe(same_node),
            ]
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
    # The csv doesn't mention it but the units are
    # likely USD/(m^3* day) which we convert to USD/(MCM*year)
    inv_cost = make_df(
        "inv_cost",
        technology=df_inv["tec"],
        value=df_inv["investment_mid"] * USD_M3DAY_TO_USD_MCM,
        unit="USD/MCM",
    ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs)]
    results["inv_cost"] = inv_cost

    # Fixed costs
    # Prepare data frame for fix_cost
    fix_cost = pd.DataFrame([])
    var_cost = pd.DataFrame([])

    for index, rows in df_inv.iterrows():
        # Check if this is a dummy technology
        use_same_year = is_dummy_technology(rows)

        fix_cost = pd.concat(
            [
                fix_cost,
                make_df(
                    "fix_cost",
                    technology=df_inv["tec"],
                    value=df_inv["fix_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                    unit="USD/MCM",
                ).pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        rows["technical_lifetime_mid"],
                        same_year_only=use_same_year,
                    ),
                    node_loc=df_node["node"],
                ),
            ]
        )

    fix_cost = fix_cost[~fix_cost["technology"].isin(techs)]

    results["fix_cost"] = fix_cost

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    df_var = df_inv[~df_inv["tec"].isin(techs)]
    df_var_dist = df_inv[df_inv["tec"].isin(techs)]

    if context.SDG != "baseline":
        for index, rows in df_var.iterrows():
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            # Variable cost
            var_cost = pd.concat(
                [
                    var_cost,
                    make_df(
                        "var_cost",
                        technology=rows["tec"],
                        value=rows["var_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                        unit="USD/MCM",
                        mode="M1",
                    ).pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    ),
                ]
            )

        # Variable cost for distribution technologies
        for index, rows in df_var_dist.iterrows():
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            var_cost = pd.concat(
                [
                    var_cost,
                    make_df(
                        "var_cost",
                        technology=rows["tec"],
                        value=rows["var_cost_high"] * USD_M3DAY_TO_USD_MCM,
                        unit="USD/MCM",
                        mode="Mf",
                    ).pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    ),
                ]
            )
        results["var_cost"] = var_cost
    else:
        # Variable cost
        for index, rows in df_var.iterrows():
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            var_cost = pd.concat(
                [
                    var_cost,
                    make_df(
                        "var_cost",
                        technology=rows["tec"],
                        value=df_var["var_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                        unit="USD/MCM",
                        mode="M1",
                    ).pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    ),
                ]
            )

        for index, rows in df_var_dist.iterrows():
            # Check if this is a dummy technology
            use_same_year = is_dummy_technology(rows)

            var_cost = pd.concat(
                [
                    var_cost,
                    make_df(
                        "var_cost",
                        technology=rows["tec"],
                        value=rows["var_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                        unit="USD/MCM",
                        mode="M1",
                    ).pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    ),
                ]
            )

            var_cost = pd.concat(
                [
                    var_cost,
                    make_df(
                        "var_cost",
                        technology=rows["tec"],
                        value=rows["var_cost_high"] * USD_M3DAY_TO_USD_MCM,
                        unit="USD/MCM",
                        mode="Mf",
                    ).pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["technical_lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=sub_time,
                    ),
                ]
            )
        results["var_cost"] = var_cost

    # Add the input dataframe to results
    results["input"] = inp_df

    # Remove duplicates from all DataFrames in results
    for key, df in results.items():
        results[key] = df.dropna().drop_duplicates().reset_index(drop=True)
    return results


def prepare_input_dataframe(
    context: "Context",
    sub_time,
    scenario_info: ScenarioInfo,
    df_node: pd.DataFrame,
    techs: list[str],
    df_elec: pd.DataFrame,
) -> defaultdict[Any, list]:
    result_dc = defaultdict(list)
    # Unit 1 KWh/m^3 = 10^3 GWh/Km^3 = 1 GWh/MCM,
    # Parkinson et al.
    # which is the only explanation as to how the model solved.
    for _, rows in df_elec.iterrows():
        if rows["tec"] in techs:
            # Check if this is a dummy technology (for distribution techs)
            use_same_year = is_dummy_technology(rows)

            if context.SDG != "baseline":
                inp = make_df(
                    "input",
                    technology=rows["tec"],
                    value=rows["value_mid"] * kWh_m3_TO_GWa_MCM,
                    unit="GWa/MCM",
                    level="final",
                    commodity="electr",
                    mode="Mf",
                    time_origin="year",
                    node_loc=df_node["node"],
                    node_origin=df_node["region"],
                ).pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        # 1 because elec commodities don't have technical lifetime
                        1,
                        same_year_only=use_same_year,
                    ),
                    time=sub_time,
                )

                result_dc["input"].append(inp)
            else:
                inp = make_df(
                    "input",
                    technology=rows["tec"],
                    value=rows["value_mid"] * kWh_m3_TO_GWa_MCM,
                    unit="GWa/MCM",
                    level="final",
                    commodity="electr",
                    mode="Mf",
                    time_origin="year",
                    node_loc=df_node["node"],
                    node_origin=df_node["region"],
                ).pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        # 1 because elec commodities don't have technical lifetime
                        1,
                        same_year_only=use_same_year,
                    ),
                    time=sub_time,
                )

                inp = pd.concat(
                    [
                        inp,
                        make_df(
                            "input",
                            technology=rows["tec"],
                            value=rows["value_high"] * kWh_m3_TO_GWa_MCM,
                            unit="GWa/MCM",
                            level="final",
                            commodity="electr",
                            mode="M1",
                            time_origin="year",
                            node_loc=df_node["node"],
                            node_origin=df_node["region"],
                        ).pipe(
                            broadcast,
                            # 1 because elec commodities don't have technical lifetime
                            get_vintage_and_active_years(
                                scenario_info, 1, same_year_only=use_same_year
                            ),
                            time=sub_time,
                        ),
                    ]
                )

                result_dc["input"].append(inp)
        else:
            # Check if this is a dummy technology (for non-distribution techs)
            use_same_year = is_dummy_technology(rows)

            inp = make_df(
                "input",
                technology=rows["tec"],
                value=rows["value_high"] * kWh_m3_TO_GWa_MCM,
                unit="GWa/MCM",
                level="final",
                commodity="electr",
                mode="M1",
                time_origin="year",
                node_loc=df_node["node"],
                node_origin=df_node["region"],
            ).pipe(
                broadcast,
                get_vintage_and_active_years(
                    scenario_info, 1, same_year_only=use_same_year
                ),
                time=sub_time,
            )

            result_dc["input"].append(inp)
    return result_dc


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
    sub_time = pd.Series(context.time)
    # Reference to the water configuration
    info = context["water build info"]

    # load the scenario from context
    scen = context.get_scenario()
    firstyear = scen.firstmodelyear
    # Create ScenarioInfo object for get_vintage_and_active_years
    scenario_info = ScenarioInfo(scen)
    year_wat = (*range(2010, info.Y[0] + 1, 5), *info.Y)

    # Reading water distribution mapping from csv
    path = package_data_path("water", "infrastructure", "desalination.csv")
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
    df_desal = pd.read_csv(path)
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
    # output dataframe linking to desal tech types
    out_df = (
        make_df(
            "output",
            technology="extract_salinewater_basin",
            value=1,
            unit="MCM/year",
            level="water_avail_basin",
            commodity="salinewater_basin",
            mode="M1",
        )
        .pipe(
            broadcast,
            get_vintage_and_active_years(scenario_info, 20),
            node_loc=df_node["node"],
            time=pd.Series(sub_time),
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
        value=df_hist["cap_km3_year"] * KM3_TO_MCM / ANNUAL_CAPACITY_FACTOR,
        unit="MCM/year",
    )

    results["historical_new_capacity"] = df_hist_cap

    # Desalination potentials are added as an upper bound
    # to limit the salinewater extraction
    bound_up = make_df(
        "bound_total_capacity_up",
        node_loc="B" + df_proj["BCU_name"],
        technology="extract_salinewater_basin",
        year_act=df_proj["year"],
        value=df_proj["cap_km3_year"] * KM3_TO_MCM,
        unit="MCM/year",
    )
    # Making negative values zero
    bound_up["value"] = bound_up["value"].clip(lower=0)
    # Bound should start from 2025
    bound_up = bound_up[bound_up["year_act"] >= firstyear]

    results["bound_total_capacity_up"] = bound_up
    # Investment costs
    inv_cost = make_df(
        "inv_cost",
        technology=df_desal["tec"],
        value=df_desal["inv_cost_mid"] * USD_M3DAY_TO_USD_MCM,
        unit="USD/MCM",
    ).pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])

    results["inv_cost"] = inv_cost

    fix_cost = pd.DataFrame([])
    var_cost = pd.DataFrame([])
    for index, rows in df_desal.iterrows():
        # Check if this is a dummy technology (desalination techs have real costs)
        use_same_year = is_dummy_technology(rows)

        # Fixed costs
        # Prepare dataframe for fix_cost
        fix_cost = pd.concat(
            [
                fix_cost,
                make_df(
                    "fix_cost",
                    technology=rows["tec"],
                    value=rows["fix_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                    unit="USD/MCM",
                ).pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        rows["lifetime_mid"],
                        same_year_only=use_same_year,
                    ),
                    node_loc=df_node["node"],
                ),
            ]
        )

        # Variable cost
        var_cost = pd.concat(
            [
                var_cost,
                make_df(
                    "var_cost",
                    technology=rows["tec"],
                    value=rows["var_cost_mid"] * USD_M3DAY_TO_USD_MCM,
                    unit="USD/MCM",
                    mode="M1",
                ).pipe(
                    broadcast,
                    get_vintage_and_active_years(
                        scenario_info,
                        rows["lifetime_mid"],
                        same_year_only=use_same_year,
                    ),
                    node_loc=df_node["node"],
                    time=pd.Series(sub_time),
                ),
            ]
        )

    # Dummy  Variable cost for salinewater extrqction
    # var_cost = var_cost.append(
    #     make_df(
    #     "var_cost",
    #     technology='extract_salinewater_basin',
    #     value= 100 * GWa_KM3_TO_GWa_MCM,
    #     unit="USD/MCM",
    #     mode="M1",
    #     time="year",
    # ).pipe(broadcast, year_vtg=year_wat, year_act=year_wat, node_loc=df_node["node"])
    # )

    results["fix_cost"] = fix_cost
    results["var_cost"] = var_cost

    tl = pd.concat(
        [
            tl,
            (
                make_df(
                    "technical_lifetime",
                    technology=df_desal["tec"],
                    value=df_desal["lifetime_mid"],
                    unit="y",
                )
                .pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"])
                .pipe(same_node)
            ),
        ]
    )

    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=3)
    results["construction_time"] = cons_time["construction_time"]

    from collections import defaultdict

    result_dc = defaultdict(list)

    for index, rows in df_desal.iterrows():
        # Check if this is a dummy technology (desalination techs have real costs)
        use_same_year = is_dummy_technology(rows)

        inp = make_df(
            "input",
            technology=rows["tec"],
            value=rows["electricity_input_mid"] * GWa_KM3_TO_GWa_MCM,
            unit="GWa/MCM",
            level="final",
            commodity="electr",
            mode="M1",
            time_origin="year",
            node_loc=df_node["node"],
            node_origin=df_node["region"],
        ).pipe(
            broadcast,
            get_vintage_and_active_years(
                scenario_info, rows["lifetime_mid"], same_year_only=use_same_year
            ),
            time=pd.Series(sub_time),
        )

        result_dc["input"].append(inp)

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = results_new["input"]

    # Adding input dataframe
    df_heat = df_desal[df_desal["heat_input_mid"] > 0]

    result_dc = defaultdict(list)

    for index, rows in df_heat.iterrows():
        # Check if this is a dummy technology (desalination techs have real costs)
        use_same_year = is_dummy_technology(rows)

        inp = make_df(
            "input",
            technology=rows["tec"],
            value=rows["heat_input_mid"] * GWa_KM3_TO_GWa_MCM,
            unit="GWa/MCM",
            level="final",
            commodity="d_heat",
            mode="M1",
            time_origin="year",
            node_loc=df_node["node"],
            node_origin=df_node["region"],
        ).pipe(
            broadcast,
            get_vintage_and_active_years(
                scenario_info, rows["lifetime_mid"], same_year_only=use_same_year
            ),
            time=pd.Series(sub_time),
        )

        result_dc["input"].append(inp)

    results_new = {par_name: pd.concat(dfs) for par_name, dfs in result_dc.items()}

    inp_df = pd.concat([inp_df, results_new["input"]])

    # Adding input dataframe
    for index, rows in df_desal.iterrows():
        # Check if this is a dummy technology (desalination techs have real costs)
        use_same_year = is_dummy_technology(rows)

        inp_df = pd.concat(
            [
                inp_df,
                (
                    make_df(
                        "input",
                        technology=rows["tec"],
                        value=1,
                        unit="MCM",
                        level=rows["inlvl"],
                        commodity=rows["incmd"],
                        mode="M1",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=pd.Series(sub_time),
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                ),
            ]
        )

        inp_df.dropna(inplace=True)

        results["input"] = inp_df

        out_df = pd.concat(
            [
                out_df,
                (
                    make_df(
                        "output",
                        technology=rows["tec"],
                        value=1,
                        unit="MCM",
                        level=rows["outlvl"],
                        commodity=rows["outcmd"],
                        mode="M1",
                    )
                    .pipe(
                        broadcast,
                        get_vintage_and_active_years(
                            scenario_info,
                            rows["lifetime_mid"],
                            same_year_only=use_same_year,
                        ),
                        node_loc=df_node["node"],
                        time=pd.Series(sub_time),
                    )
                    .pipe(same_node)
                    .pipe(same_time)
                ),
            ]
        )

        results["output"] = out_df

    # putting a lower bound on desalination tecs based on hist capacities
    df_bound = df_hist[df_hist["year"] == 2025]  # firstyear dataabsent
    bound_lo = make_df(
        "bound_activity_lo",
        node_loc="B" + df_bound["BCU_name"],
        technology=df_bound["tec_type"],
        mode="M1",
        value=df_bound["cap_km3_year"] * KM3_TO_MCM,
        unit="MCM/year",
    ).pipe(
        broadcast,
        year_act=year_wat,
        time=pd.Series(sub_time),
    )

    bound_lo = bound_lo[bound_lo["year_act"] <= firstyear + 15]
    # Divide the histroical capacity by 5 since the existing data is summed over
    # 5 years and model needs per year
    bound_lo["value"] = bound_lo["value"] / 5

    # Clip activity bounds to not exceed capacity bounds
    bound_lo = bound_lo.merge(
        bound_up[["node_loc", "year_act", "value"]],
        on=["node_loc", "year_act"],
        how="left",
        suffixes=("", "_cap"),
    )
    bound_lo["value"] = np.minimum(
        bound_lo["value"], bound_lo["value_cap"].fillna(np.inf)
    )
    bound_lo = bound_lo.drop("value_cap", axis=1)

    results["bound_activity_lo"] = bound_lo

    # # Add soft constraints for desalination bound_activity_lo
    # # Parameters for soft constraints
    # relaxation_factor = 10  # Effectively allow complete relaxation at penalty
    # penalty_multiplier = 1.0  # 100% of levelized cost as penalty for violations
    #
    # # Create soft_activity_lo parameter using the same bound_lo data
    # soft_lo = bound_lo.copy()
    # soft_lo["value"] = relaxation_factor
    # soft_lo["unit"] = "-"
    # results["soft_activity_lo"] = soft_lo
    #
    # # Create penalty cost parameter
    # penalty_lo = bound_lo.copy()
    # penalty_lo["value"] = penalty_multiplier
    # penalty_lo["unit"] = "-"
    # results["level_cost_activity_soft_lo"] = penalty_lo
    #
    # Remove duplicates from all DataFrames in results
    for key, df in results.items():
        results[key] = df.dropna().drop_duplicates().reset_index(drop=True)
    return results
