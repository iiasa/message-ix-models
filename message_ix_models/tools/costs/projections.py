from itertools import product

import numpy as np
import pandas as pd

from message_ix_models.tools.costs.config import (
    BASE_YEAR,
    FIRST_MODEL_YEAR,
    LAST_MODEL_YEAR,
)
from message_ix_models.tools.costs.gdp import calculate_gdp_adjusted_region_cost_ratios
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)

# from message_ix_models.tools.costs.splines import (
#     get_final_inv_and_fom_costs,
#     project_all_inv_costs,
# )
from message_ix_models.tools.costs.weo import get_weo_region_differentiated_costs


def create_projections_learning(in_node, in_ref_region, in_base_year, in_scenario):
    print("Selected scenario: " + in_scenario)
    print(
        "For the learning method, only the SSP scenario(s) itself needs to be specified. \
        No scenario version (previous vs. updated) is needed."
    )

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            sel_scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        else:
            sel_scen = in_scenario.upper()

    # Repeating to avoid linting error
    sel_scen = sel_scen

    df_region_diff = get_weo_region_differentiated_costs(
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @sel_scen")

    df_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
        )
        .reindex(
            [
                "scenario",
                "message_technology",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
    )

    return df_costs


def create_projections_gdp(
    in_node, in_ref_region, in_base_year, in_scenario, in_scenario_version
):
    # Print selection of scenario version and scenario
    print("Selected scenario: " + in_scenario)
    print("Selected scenario version: " + in_scenario_version)

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            sel_scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        else:
            sel_scen = in_scenario.upper()

    # If no scenario version is specified, do not filter for scenario version
    # If it specified, then filter as below:
    if in_scenario_version is not None:
        if in_scenario_version == "all":
            sel_scen_vers = ["Review (2023)", "Previous (2013)"]
        elif in_scenario_version == "updated":
            sel_scen_vers = ["Review (2023)"]
        elif in_scenario_version == "original":
            sel_scen_vers = ["Previous (2013)"]

    # Repeating to avoid linting error
    sel_scen = sel_scen
    sel_scen_vers = sel_scen_vers

    df_region_diff = get_weo_region_differentiated_costs(
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    df_adj_cost_ratios = calculate_gdp_adjusted_region_cost_ratios(
        df_region_diff,
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @sel_scen")
        df_adj_cost_ratios = df_adj_cost_ratios.query(
            "scenario_version == @sel_scen_vers and scenario == @sel_scen"
        )

    df_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .merge(
            df_adj_cost_ratios, on=["scenario", "message_technology", "region", "year"]
        )
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio_adj,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
    )

    return df_costs


def create_projections_converge(
    in_node, in_ref_region, in_base_year, in_scenario, in_convergence_year
):
    print("Selected scenario: " + in_scenario)
    print("Selected convergence year: " + str(in_convergence_year))
    print(
        "For the convergence method, only the SSP scenario(s) itself needs to be specified. \
        No scenario version (previous vs. updated) is needed."
    )

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            sel_scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        else:
            sel_scen = in_scenario.upper()

    # Repeating to avoid linting error
    sel_scen = sel_scen

    df_region_diff = get_weo_region_differentiated_costs(
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @sel_scen")

    df_pre_costs = df_region_diff.merge(
        df_ref_reg_learning, on="message_technology"
    ).assign(
        inv_cost_converge=lambda x: np.where(
            x.year <= FIRST_MODEL_YEAR,
            x.reg_cost_base_year,
            np.where(
                x.year < in_convergence_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                x.inv_cost_ref_region_learning,
            ),
        ),
    )

    df_splines = apply_splines_to_convergence(
        df_pre_costs,
        column_name="inv_cost_converge",
        input_convergence_year=in_convergence_year,
    )

    df_costs = (
        df_pre_costs.merge(
            df_splines,
            on=["scenario", "message_technology", "region", "year"],
            how="outer",
        )
        .rename(columns={"inv_cost_splines": "inv_cost"})
        .assign(fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio)
        .reindex(
            [
                "scenario",
                "message_technology",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
    )

    return df_costs


def create_cost_projections(
    sel_node: str = "r12",
    sel_ref_region=None,
    sel_base_year: int = BASE_YEAR,
    sel_method: str = "gdp",
    sel_scenario_version="updated",
    sel_scenario="all",
    sel_convergence_year: int = 2050,
):
    """Get investment and fixed cost projections

    Parameters
    ----------
    sel_node : str, optional
        Spatial resolution, by default "r12". Options are "r11", "r12", and "r20"
    sel_ref_region : str, optional
        Reference region, by default R12_NAM for R12, R11_NAM for R11, and R20_NAM for R20
    sel_base_year : int, optional
        Base year, by default BASE_YEAR specified in the config file
    sel_method : str, optional
        Method to use, by default "gdp". Options are "learning", "gdp", and "convergence"
    sel_scenario_version : str, optional
        Scenario version, by default "updated". Options are "updated" and "original"
    sel_scenario : str, optional
        Scenario, by default "all"
    sel_convergence_year : int, optional
        Year to converge costs to, by default 2050

    Returns
    -------
    pandas.DataFrame
        Dataframe containing cost projections
    """
    # Change node selection to upper case
    node_up = sel_node.upper()

    # Check if node selection is valid
    if node_up not in ["R11", "R12", "R20"]:
        return "Please select a valid spatial resolution: R11, R12, or R20"
    else:
        # Set default values for input arguments
        # If specified node is R11, then use R11_NAM as the reference region
        # If specified node is R12, then use R12_NAM as the reference region
        # If specified node is R20, then use R20_NAM as the reference region
        # However, if a reference region is specified, then use that instead
        if sel_ref_region is None:
            if node_up == "R11":
                sel_ref_region = "R11_NAM"
            if node_up == "R12":
                sel_ref_region = "R12_NAM"
            if node_up == "R20":
                sel_ref_region = "R20_NAM"
        elif sel_ref_region is not None:
            sel_ref_region = sel_ref_region.upper()

        # Print final selection of regions, reference regions, and base year
        print("Selected node: " + node_up)
        print("Selected reference region: " + sel_ref_region)
        print("Selected base year: " + str(sel_base_year))

        print("Selected method: " + sel_method)

        # If method is learning, then use the learning method
        if sel_method == "learning":
            df_costs = create_projections_learning(
                in_node=node_up,
                in_ref_region=sel_ref_region,
                in_base_year=sel_base_year,
                in_scenario=sel_scenario,
            )

        # If method is GDP, then use the GDP method
        if sel_method == "gdp":
            df_costs = create_projections_gdp(
                in_node=node_up,
                in_ref_region=sel_ref_region,
                in_base_year=sel_base_year,
                in_scenario=sel_scenario,
                in_scenario_version=sel_scenario_version,
            )

        # If method is convergence, then use the convergence method
        if sel_method == "convergence":
            df_costs = create_projections_converge(
                in_node=node_up,
                in_ref_region=sel_ref_region,
                in_base_year=sel_base_year,
                in_scenario=sel_scenario,
                in_convergence_year=sel_convergence_year,
            )

        return df_costs


# Function to get cost projections based on the following inputs:
# - Spatial resolution
# - Reference region
# - Base year
# - Scenario version (review or updated)
# - SSP scenario
# - Method (learning only, GDP adjusted, or convergence via spline projections)
# - Convergence year (if applicable)
# - Format (message or IAMC)
# def get_cost_projections(
#     sel_node: str = "r12",
#     sel_ref_region=None,
#     sel_base_year: int = BASE_YEAR,
#     sel_scenario_version="updated",
#     sel_scenario="all",
#     sel_method: str = "convergence",
#     sel_convergence_year: int = 2050,
#     sel_format: str = "message",
# ):
#     # Change node selection to upper case
#     node_up = sel_node.upper()

#     # Check if node selection is valid
#     if node_up not in ["R11", "R12", "R20"]:
#         return "Please select a valid spatial resolution: R11, R12, or R20"
#     else:
#         # Set default values for input arguments
#         # If specified node is R11, then use R11_NAM as the reference region
#         # If specified node is R12, then use R12_NAM as the reference region
#         # If specified node is R20, then use R20_NAM as the reference region
#         # However, if a reference region is specified, then use that instead
#         if sel_ref_region is None:
#             if node_up == "R11":
#                 sel_ref_region = "R11_NAM"
#             if node_up == "R12":
#                 sel_ref_region = "R12_NAM"
#             if node_up == "R20":
#                 sel_ref_region = "R20_NAM"
#         elif sel_ref_region is not None:
#             sel_ref_region = sel_ref_region.upper()

#         # Print final selection of regions, reference regions, and base year
#         print("Selected node: " + node_up)
#         print("Selected reference region: " + sel_ref_region)
#         print("Selected base year: " + str(sel_base_year))

#         print("Selected method: " + sel_method)

#         # Print final selection of scenario version and scenario
#         print("Selected scenario version: " + sel_scenario_version)
#         print("Selected scenario: " + sel_scenario)

#         df_region_diff = get_weo_region_differentiated_costs(
#             input_node=sel_node,
#             input_ref_region=sel_ref_region,
#             input_base_year=sel_base_year,
#         )

#         df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
#             df_region_diff,
#             input_node=sel_node,
#             input_ref_region=sel_ref_region,
#             input_base_year=sel_base_year,
#         )

#         df_adj_cost_ratios = calculate_gdp_adjusted_region_cost_ratios(
#             df_region_diff,
#             input_node=sel_node,
#             input_ref_region=sel_ref_region,
#             input_base_year=sel_base_year,
#         )

#         df_all_inv = project_all_inv_costs(
#             df_region_diff,
#             df_ref_reg_learning,
#             df_adj_cost_ratios,
#             input_convergence_year=sel_convergence_year,
#             input_scenario_version=sel_scenario_version,
#             input_scenario=sel_scenario,
#         )

#         df_inv_fom = get_final_inv_and_fom_costs(df_all_inv, input_method=sel_method)

#         return df_inv_fom


def create_message_inputs(df_proj: pd.DataFrame):
    """Create inputs for MESSAGE

    Parameters
    ----------
    df_proj : pd.DataFrame
        Dataframe containing cost projections, output of :func:`get_cost_projections`

    Returns
    -------
    """

    HORIZON_START = 1960
    HORIZON_END = 2110

    # For investment costs, for each region-technology pair, repeat the cost up until base year and then use the projected values up until 2100
    # For years up until the horizon end, repeat the 2100 value
    un_vers = df_proj.scenario_version.unique()
    un_scen = df_proj.scenario.unique()
    un_tech = df_proj.message_technology.unique()
    un_reg = df_proj.region.unique()

    l_inv = []
    l_fix = []
    for h, i, j, k in product(un_vers, un_scen, un_tech, un_reg):
        print(h, i, j, k)

        def smaller_than(sequence, value):
            return [item for item in sequence if item < value]

        def larger_than(sequence, value):
            return [item for item in sequence if item > value]

        seq_years = list(range(HORIZON_START, HORIZON_END + 5, 5))
        hist_years = smaller_than(seq_years, BASE_YEAR - 5)
        fut_years = larger_than(seq_years, LAST_MODEL_YEAR)

        tech = df_proj.query(
            "scenario_version == @h and scenario == @i and message_technology == @j \
                    and region == @k"
        )

        # For years up until the base year, repeat the 2020 value
        l_hist = []
        for year in hist_years:
            df = tech.query("year == 2020").assign(year=year)
            l_hist.append(df)

        # For years after the final model year, repeat the 2100 value
        l_fut = []
        for year in fut_years:
            df = tech.query("year == 2100").assign(year=year)
            l_fut.append(df)

        # Combine all dataframes
        costs_hist = pd.concat(l_hist)
        costs_fut = pd.concat(l_fut)
        costs_tot = costs_hist._append([tech, costs_fut]).reset_index(drop=1)

        # For investment costs, assign year as year_vtg and use value as inv_cost
        tech_inv = costs_tot.assign(
            year_vtg=lambda x: x.year,
            value=lambda x: x.inv_cost,
            unit="USD/kWa",
            technology=lambda x: x.message_technology,
            node_loc=lambda x: x.region,
        ).reindex(
            [
                "scenario_version",
                "scenario",
                "node_loc",
                "technology",
                "year_vtg",
                "value",
                "unit",
            ],
            axis=1,
        )

        l_fom_updated = []
        for y in seq_years:
            fom = (
                costs_tot.query("year >= @y")
                .reindex(
                    [
                        "scenario_version",
                        "scenario",
                        "message_technology",
                        "region",
                        "year",
                        "fix_cost",
                    ],
                    axis=1,
                )
                .assign(year_vtg=y)
            )

            if y <= 2020:
                init_val = fom.query("year == 2020").fix_cost.values[0]
            elif y > 2020:
                init_val = fom.query("year == @y").fix_cost.values[0]

            # Calculate value every year if val decreases by 0.5% every year
            d = pd.DataFrame(data={"year": range(y, 2111)}).assign(
                val=lambda x: init_val * (1 - 0.0025) ** (x.year - y),
            )

            fom_updated = (
                fom.merge(d, on="year", how="left")
                .assign(
                    value=lambda x: np.where(x.year <= 2020, x.fix_cost, x.val),
                    year_act=lambda x: x.year,
                    unit="USD/kWa",
                    technology=lambda x: x.message_technology,
                    node_loc=lambda x: x.region,
                )
                .reindex(
                    [
                        "scenario_version",
                        "scenario",
                        "node_loc",
                        "technology",
                        "year_vtg",
                        "year_act",
                        "value",
                        "unit",
                    ],
                    axis=1,
                )
            )

            l_fom_updated.append(fom_updated)

        tech_fom = pd.concat(l_fom_updated).reset_index(drop=1)

        l_inv.append(tech_inv)
        l_fix.append(tech_fom)

    msg_inv = pd.concat(l_inv).reset_index(drop=1)
    msg_fom = pd.concat(l_fix).reset_index(drop=1)

    return msg_inv, msg_fom


# # Function to get cost projections based on method specified
# # (learning only, GDP adjusted, or convergence via spline projections)
# def get_cost_projections(
#     cost_type: str = "inv_cost",
#     scenario: str = "ssp2",
#     version: str = "review",
#     format: str = "message",
#     use_gdp: bool = False,
#     converge_costs: bool = True,
#     convergence_year: int = 2050,
# ):
#     """Get cost projections based on method specified

#     Parameters
#     ----------
#     cost_type : str, optional
#         Type of cost to project, by default "inv_cost"
#     scenario : str, optional
#         SSP scenario, by default "ssp2"
#     format : str, optional
#         Format of output, by default "message"
#     use_gdp : bool, optional
#         Whether to use GDP projections, by default False
#     converge_costs : bool, optional
#         Whether to converge costs, by default True
#     convergence_year : int, optional
#         Year to converge costs to, by default 2050

#     Returns
#     -------
#     pandas.DataFrame

#     Columns depend on the format specified:
#     - message: scenario, node_loc, technology, year_vtg, value, unit
#     - iamc: Scenario, Region, Variable, 2020, 2025, ..., 2100
#     """
#     df_weo = get_weo_data()
#     df_nam_orig_message = get_cost_assumption_data()
#     df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)
#     df_fom_inv_ratios = calculate_fom_to_inv_cost_ratios(df_weo)

#     df_region_diff = get_region_differentiated_costs(
#         df_weo, df_nam_orig_message, df_tech_cost_ratios
#     )

#     df_learning_rates = get_cost_reduction_data()
#     df_technology_first_year = get_technology_first_year_data()

#     df_gdp = get_gdp_data()
#     df_linreg = linearly_regress_tech_cost_vs_gdp_ratios(df_gdp, df_tech_cost_ratios)

#     df_adj_cost_ratios = calculate_adjusted_region_cost_ratios(df_gdp, df_linreg)
#     df_nam_learning = project_NAM_inv_costs_using_learning_rates(
#         df_region_diff, df_learning_rates, df_technology_first_year
#     )

#     df_adj_inv = project_adjusted_inv_costs(
#         df_nam_learning,
#         df_adj_cost_ratios,
#         df_region_diff,
#         convergence_year_flag=convergence_year,
#     )

#     df_poly_reg = apply_polynominal_regression(
#         df_adj_inv, convergence_year_flag=convergence_year
#     )

#     df_spline_projections = apply_splines_projection(
#         df_region_diff, df_technology_first_year, df_poly_reg, df_adj_inv
#     )

#     df_inv_fom = project_final_inv_and_fom_costs(
#         df_spline_projections,
#         df_fom_inv_ratios,
#         use_gdp_flag=use_gdp,
#         converge_costs_flag=converge_costs,
#     )

#     df_message = (
#         df_inv_fom.loc[(df_spline_projections.scenario == scenario.upper())]
#         .assign(
#             node_loc=lambda x: "R11_" + x.r11_region,
#             technology=lambda x: x.message_technology,
#             year_vtg=lambda x: x.year,
#             value=lambda x: x[cost_type],
#             unit="USD/kW",
#         )
#         .reindex(
#             ["scenario", "node_loc", "technology", "year_vtg", "value", "unit"],
# axis=1
#         )
#         .reset_index(drop=1)
#     )

#     df_iamc = (
#         df_inv_fom.reindex(
#             ["scenario", "message_technology", "r11_region", "year", cost_type],
#             axis=1,
#         )
#         .melt(
#             id_vars=[
#                 "scenario",
#                 "message_technology",
#                 "r11_region",
#                 "year",
#             ],
#             var_name="cost_type",
#             value_name="cost_value",
#         )
#         .assign(
#             Variable=lambda x: np.where(
#                 x.cost_type == "inv_cost",
#                 "Capital Cost|Electricity|" + x.message_technology,
#                 "OM Cost|Electricity|" + x.message_technology,
#             )
#         )
#         .rename(
#             columns={"scenario": "Scenario", "year": "Year", "r11_region": "Region"}
#         )
#         .drop(columns=["message_technology"])
#         .pivot(
#             index=["Scenario", "Region", "Variable"],
#             columns="Year",
#             values="cost_value",
#         )
#         .reset_index()
#         .rename_axis(None, axis=1)
#     )

#     if format == "message":
#         return df_message
#     elif format == "iamc":
#         return df_iamc
