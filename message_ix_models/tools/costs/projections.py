import numpy as np

from message_ix_models.tools.costs.gdp import (
    get_gdp_data,
    linearly_regress_tech_cost_vs_gdp_ratios,
)
from message_ix_models.tools.costs.learning import get_cost_reduction_data
from message_ix_models.tools.costs.splines import (
    apply_polynominal_regression,
    get_technology_first_year_data,
    project_capital_costs_using_learning_rates,
    project_costs_using_splines,
)
from message_ix_models.tools.costs.weo import (
    calculate_fom_to_inv_cost_ratios,
    calculate_region_cost_ratios,
    get_cost_assumption_data,
    get_region_differentiated_costs,
    get_weo_data,
)


def create_cost_inputs(cost_type, ssp_scenario="ssp2", format="message"):
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)
    df_fom_inv_ratios = calculate_fom_to_inv_cost_ratios(df_weo)

    df_region_diff = get_region_differentiated_costs(
        df_weo, df_nam_orig_message, df_tech_cost_ratios
    )

    df_learning_rates = get_cost_reduction_data()
    df_technology_first_year = get_technology_first_year_data()
    df_learning_projections = project_capital_costs_using_learning_rates(
        df_learning_rates, df_region_diff, df_technology_first_year
    )
    df_poly_reg = apply_polynominal_regression(df_learning_projections)
    df_spline_projections = project_costs_using_splines(
        df_region_diff,
        df_technology_first_year,
        df_poly_reg,
        df_learning_projections,
        df_fom_inv_ratios,
    )

    df_message = (
        df_spline_projections.loc[
            (df_spline_projections.ssp_scenario == ssp_scenario.upper())
        ]
        .assign(
            node_loc=lambda x: "R11_" + x.r11_region,
            technology=lambda x: x.message_technology,
            year_vtg=lambda x: x.year,
            value=lambda x: x[cost_type],
            unit="USD/kW",
        )
        .reindex(["node_loc", "technology", "year_vtg", "value", "unit"], axis=1)
        .reset_index(drop=1)
    )

    df_iamc = (
        df_spline_projections.reindex(
            ["ssp_scenario", "message_technology", "r11_region", "year", cost_type],
            axis=1,
        )
        .melt(
            id_vars=[
                "ssp_scenario",
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
            columns={"ssp_scenario": "Scenario", "year": "Year", "r11_region": "Region"}
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
