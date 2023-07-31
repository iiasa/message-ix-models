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


# Test projection of adjusted investment costs
def test_project_adjusted_inv_costs():
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)

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

    res = project_adjusted_inv_costs(
        df_nam_learning,
        df_adj_cost_ratios,
        df_region_diff,
        convergence_year_flag=2060,
    )

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "scenario",
                    "message_technology",
                    "weo_technology",
                    "r11_region",
                    "year",
                    "inv_cost_learning_only",
                    "inv_cost_gdp_adj",
                    "inv_cost_converge",
                ]
            ).any()
        )
        is True
    )

    # Check that the maximum year is 2100
    assert res.year.max() == 2100


# Test application of polynomial regression
def test_apply_polynominal_regression():
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)

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
        convergence_year_flag=2060,
    )

    res = apply_polynominal_regression(df_adj_inv, convergence_year_flag=2060)

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "scenario",
                    "message_technology",
                    "r11_region",
                    "beta_1",
                    "beta_2",
                    "beta_3",
                    "intercept",
                ]
            ).any()
        )
        is True
    )


# Test projections using spline regression results
def test_apply_splines_projection():
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)

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
        convergence_year_flag=2060,
    )

    df_poly_reg = apply_polynominal_regression(df_adj_inv, convergence_year_flag=2060)

    res = apply_splines_projection(
        df_region_diff, df_technology_first_year, df_poly_reg, df_adj_inv
    )

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "scenario",
                    "message_technology",
                    "r11_region",
                    "year",
                    "inv_cost_learning_only",
                    "inv_cost_gdp_adj",
                    "inv_cost_converge",
                    "inv_cost_splines",
                ]
            ).any()
        )
        is True
    )

    # Check that the maximum year is 2100
    assert res.year.max() == 2100


# Test function to get final investment and fixed costs
def test_project_final_inv_and_fom_costs():
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
        convergence_year_flag=2060,
    )

    df_poly_reg = apply_polynominal_regression(df_adj_inv, convergence_year_flag=2060)

    df_spline_projections = apply_splines_projection(
        df_region_diff, df_technology_first_year, df_poly_reg, df_adj_inv
    )

    res = project_final_inv_and_fom_costs(
        df_spline_projections,
        df_fom_inv_ratios,
        use_gdp_flag=False,
        converge_costs_flag=True,
    )

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "scenario",
                    "message_technology",
                    "r11_region",
                    "year",
                    "inv_cost",
                    "fix_cost",
                ]
            ).any()
        )
        is True
    )

    # Check that the maximum year is 2100
    assert res.year.max() == 2100

    # Check that all fix costs are less than investment costs
    assert bool((res.fix_cost / res.inv_cost).max() < 1)
