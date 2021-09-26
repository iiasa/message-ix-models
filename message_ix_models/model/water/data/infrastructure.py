"""Prepare data for adding techs related to water distribution, treatment in urban & rural"""

import pandas as pd
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    private_data_path,
    same_node,
)

from .demands import add_sectoral_demands


def add_infrastructure_techs(context):
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

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = private_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = "R11_" + df_node["REGION"].astype(str)

    # Reading water distribution mapping from csv
    path = private_data_path("water", "water_dist", "water_distribution.xlsx")
    df = pd.read_excel(path)

    df_non_elec = df[df["incmd"] != "electr"].reset_index()
    df_elec = df[df["incmd"] == "electr"].reset_index()

    # Adding input dataframe
    inp_df = (
        make_df(
            "input",
            technology=df_non_elec["tec"],
            value=df_non_elec["value_mid"],
            unit="-",
            level=df_non_elec["inlvl"],
            commodity=df_non_elec["incmd"],
            mode="M1",
            time="year",
            time_origin="year",
        )
        .pipe(broadcast, year_act=info.Y, year_vtg=info.Y, node_loc=df_node["node"])
        .pipe(same_node)
    )

    for index, rows in df_elec.iterrows():

        inp_df = inp_df.append(
            (
                make_df(
                    "input",
                    technology=rows["tec"],
                    value=rows["value_mid"],
                    unit="-",
                    level="final",
                    commodity="electr",
                    mode="M1",
                    time="year",
                    time_origin="year",
                    node_loc=df_node["node"],
                    node_origin=df_node["region"],
                ).pipe(
                    broadcast,
                    year_act=info.Y,
                    year_vtg=info.Y,
                )
            )
        )

        results["input"] = inp_df

    # add output dataframe
    df_out = df[~df["outcmd"].isna()]
    out_df = (
        make_df(
            "output",
            technology=df_out["tec"],
            value=df_out["out_value_mid"],
            unit="-",
            level=df_out["outlvl"],
            commodity=df_out["outcmd"],
            mode="M1",
            time="year",
            time_dest="year",
        )
        .pipe(broadcast, year_act=info.Y, year_vtg=info.Y, node_loc=df_node["node"])
        .pipe(same_node)
    )

    results["output"] = out_df

    # Filtering df for capacity factors
    df_cap = df.dropna(subset=["capacity_factor_mid"])

    # Adding input dataframe
    cap_df = (
        make_df(
            "capacity_factor",
            technology=df_cap["tec"],
            value=df_cap["capacity_factor_mid"],
            unit="%",
            time="year",
        )
        .pipe(broadcast, year_act=info.Y, year_vtg=info.Y, node_loc=df_node["node"])
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
        .pipe(broadcast, year_vtg=info.Y, node_loc=df_node["node"])
        .pipe(same_node)
    )

    results["technical_lifetime"] = tl

    cons_time = make_matched_dfs(tl, construction_time=1)
    results["construction_time"] = cons_time["construction_time"]

    # Investment costs
    df_inv = df.dropna(subset=["investment_mid"])

    # Prepare dataframe for investments
    # TODO finalize units
    inv_cost = make_df(
        "inv_cost", technology=df_inv["tec"], value=df_inv["investment_mid"], unit="-"
    ).pipe(broadcast, year_vtg=info.Y, node_loc=df_node["node"])

    results["inv_cost"] = inv_cost

    # Fixed costs
    # Prepare dataframe for fix_cost
    # TODO update units
    fix_cost = make_df(
        "fix_cost", technology=df_inv["tec"], value=df_inv["fix_cost_mid"], unit="-"
    ).pipe(broadcast, year_vtg=info.Y, year_act=info.Y, node_loc=df_node["node"])

    results["fix_cost"] = fix_cost

    # Variable cost
    var_cost = make_df(
        "var_cost",
        technology=df_inv["tec"],
        value=df_inv["var_cost_mid"],
        unit="-",
        mode="M1",
        time="year",
    ).pipe(broadcast, year_vtg=info.Y, year_act=info.Y, node_loc=df_node["node"])

    results["var_cost"] = var_cost

    return results
