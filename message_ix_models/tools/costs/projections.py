from message_ix_models.tools.costs.config import BASE_YEAR
from message_ix_models.tools.costs.gdp import calculate_gdp_adjusted_region_cost_ratios
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.splines import (
    get_final_inv_and_fom_costs,
    project_all_inv_costs,
)
from message_ix_models.tools.costs.weo import get_weo_region_differentiated_costs

df_reg_diff = get_weo_region_differentiated_costs(
    input_node="r12", input_ref_region="R12_NAM", input_base_year=2021
)

df_adj_cost_ratios = calculate_gdp_adjusted_region_cost_ratios(
    df_reg_diff, input_node="r12", input_ref_region="R12_NAM", input_base_year=2021
)


# Function to get cost projections based on the following inputs:
# - Spatial resolution
# - Reference region
# - Base year
# - Scenario version (review or updated)
# - SSP scenario
# - Method (learning only, GDP adjusted, or convergence via spline projections)
# - Convergence year (if applicable)
# - Format (message or IAMC)
def get_cost_projections(
    sel_node: str = "r12",
    sel_ref_region=None,
    sel_base_year: int = BASE_YEAR,
    sel_scenario_version="updated",
    sel_scenario="all",
    sel_method: str = "convergence",
    sel_convergence_year: int = 2050,
    sel_format: str = "message",
):
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

        # Print final selection of scenario version and scenario
        print("Selected scenario version: " + sel_scenario_version)
        print("Selected scenario: " + sel_scenario)

        df_region_diff = get_weo_region_differentiated_costs(
            input_node=sel_node,
            input_ref_region=sel_ref_region,
            input_base_year=sel_base_year,
        )

        df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
            df_region_diff,
            input_node=sel_node,
            input_ref_region=sel_ref_region,
            input_base_year=sel_base_year,
        )

        df_adj_cost_ratios = calculate_gdp_adjusted_region_cost_ratios(
            df_region_diff,
            input_node=sel_node,
            input_ref_region=sel_ref_region,
            input_base_year=sel_base_year,
        )

        df_all_inv = project_all_inv_costs(
            df_region_diff,
            df_ref_reg_learning,
            df_adj_cost_ratios,
            input_convergence_year=sel_convergence_year,
            input_scenario_version=sel_scenario_version,
            input_scenario=sel_scenario,
        )

        df_inv_fom = get_final_inv_and_fom_costs(df_all_inv, input_method=sel_method)

        return df_inv_fom


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
