import numpy as np

from message_ix_models.tools.costs.gdp import (
    calculate_adjusted_region_cost_ratios,
    get_gdp_data,
    linearly_regress_tech_cost_vs_gdp_ratios,
)
from message_ix_models.tools.costs.learning import (
    get_cost_reduction_data,
    get_technology_first_year_data,
    project_NAM_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.splines import (
    apply_polynominal_regression,
    apply_splines_projection,
    project_adjusted_inv_costs,
    project_final_inv_and_fom_costs,
)
from message_ix_models.tools.costs.weo import (
    calculate_fom_to_inv_cost_ratios,
    calculate_region_cost_ratios,
    get_cost_assumption_data,
    get_region_differentiated_costs,
    get_weo_data,
)


# Function to get cost projections based on method specified
# (learning only, GDP adjusted, or convergence via spline projections)
def get_cost_projections(
    cost_type: str = "inv_cost",
    scenario: str = "ssp2",
    format: str = "message",
    use_gdp: bool = False,
    converge_costs: bool = True,
    convergence_year: int = 2050,
):
    """Get cost projections based on method specified

    Parameters
    ----------
    cost_type : str, optional
        Type of cost to project, by default "inv_cost"
    scenario : str, optional
        SSP scenario, by default "ssp2"
    format : str, optional
        Format of output, by default "message"
    use_gdp : bool, optional
        Whether to use GDP projections, by default False
    converge_costs : bool, optional
        Whether to converge costs, by default True
    convergence_year : int, optional
        Year to converge costs to, by default 2050

    Returns
    -------
    pandas.DataFrame

    Columns depend on the format specified:
    - message: scenario, node_loc, technology, year_vtg, value, unit
    - iamc: Scenario, Region, Variable, 2020, 2025, ..., 2100
    """
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)
    df_fom_inv_ratios = calculate_fom_to_inv_cost_ratios(df_weo)

    df_region_diff = get_region_differentiated_costs(
        df_weo, df_nam_orig_message, df_tech_cost_ratios
    )

    df_learning_rates = get_cost_reduction_data()
    df_technology_first_year = get_technology_first_year_data()

    df_gdp = get_gdp_data()
    df_linreg = linearly_regress_tech_cost_vs_gdp_ratios(df_gdp, df_tech_cost_ratios)

    df_adj_cost_ratios = calculate_adjusted_region_cost_ratios(df_gdp, df_linreg)
    df_nam_learning = project_NAM_inv_costs_using_learning_rates(
        df_region_diff, df_learning_rates, df_technology_first_year
    )

    df_adj_inv = project_adjusted_inv_costs(
        df_nam_learning,
        df_adj_cost_ratios,
        df_region_diff,
        convergence_year_flag=convergence_year,
    )

    df_poly_reg = apply_polynominal_regression(
        df_adj_inv, convergence_year_flag=convergence_year
    )

    df_spline_projections = apply_splines_projection(
        df_region_diff, df_technology_first_year, df_poly_reg, df_adj_inv
    )

    df_inv_fom = project_final_inv_and_fom_costs(
        df_spline_projections,
        df_fom_inv_ratios,
        use_gdp_flag=use_gdp,
        converge_costs_flag=converge_costs,
    )

    df_message = (
        df_inv_fom.loc[(df_spline_projections.scenario == scenario.upper())]
        .assign(
            node_loc=lambda x: "R11_" + x.r11_region,
            technology=lambda x: x.message_technology,
            year_vtg=lambda x: x.year,
            value=lambda x: x[cost_type],
            unit="USD/kW",
        )
        .reindex(
            ["scenario", "node_loc", "technology", "year_vtg", "value", "unit"], axis=1
        )
        .reset_index(drop=1)
    )

    df_iamc = (
        df_inv_fom.reindex(
            ["scenario", "message_technology", "r11_region", "year", cost_type],
            axis=1,
        )
        .melt(
            id_vars=[
                "scenario",
                "message_technology",
                "r11_region",
                "year",
            ],
            var_name="cost_type",
            value_name="cost_value",
        )
        .assign(
            Variable=lambda x: np.where(
                x.cost_type == "inv_cost",
                "Capital Cost|Electricity|" + x.message_technology,
                "OM Cost|Electricity|" + x.message_technology,
            )
        )
        .rename(
            columns={"scenario": "Scenario", "year": "Year", "r11_region": "Region"}
        )
        .drop(columns=["message_technology"])
        .pivot(
            index=["Scenario", "Region", "Variable"],
            columns="Year",
            values="cost_value",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    if format == "message":
        return df_message
    elif format == "iamc":
        return df_iamc


def get_all_costs(
    use_gdp: bool = False,
    converge_costs: bool = True,
    convergence_year: int = 2050,
):
    """Get all costs

    Parameters
    ----------
    use_gdp : bool, optional
        Whether to use GDP projections, by default False
    converge_costs : bool, optional
        Whether to converge costs, by default True
    convergence_year : int, optional
        Year to converge costs to, by default 2050

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario: SSP1, SSP2, or SSP3
        - message_technology: MESSAGEix technology name
        - r11_region: R11 region
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed cost

    """
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)
    df_fom_inv_ratios = calculate_fom_to_inv_cost_ratios(df_weo)

    df_region_diff = get_region_differentiated_costs(
        df_weo, df_nam_orig_message, df_tech_cost_ratios
    )

    df_learning_rates = get_cost_reduction_data()
    df_technology_first_year = get_technology_first_year_data()

    df_gdp = get_gdp_data()
    df_linreg = linearly_regress_tech_cost_vs_gdp_ratios(df_gdp, df_tech_cost_ratios)

    df_adj_cost_ratios = calculate_adjusted_region_cost_ratios(df_gdp, df_linreg)
    df_nam_learning = project_NAM_inv_costs_using_learning_rates(
        df_region_diff, df_learning_rates, df_technology_first_year
    )

    df_reg_learning = project_adjusted_inv_costs(
        df_nam_learning,
        df_adj_cost_ratios,
        df_region_diff,
        convergence_year_flag=convergence_year,
    )

    df_poly_reg = apply_polynominal_regression(df_reg_learning)

    df_spline_projections = apply_splines_projection(
        df_region_diff, df_technology_first_year, df_poly_reg, df_reg_learning
    )

    df_inv_fom = project_final_inv_and_fom_costs(
        df_spline_projections,
        df_fom_inv_ratios,
        use_gdp_flag=use_gdp,
        converge_costs_flag=converge_costs,
    )

    return df_inv_fom
