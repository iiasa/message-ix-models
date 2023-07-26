from message_ix_models.tools.costs.learning import (
    get_cost_reduction_data,
    get_technology_first_year_data,
    project_NAM_capital_costs_using_learning_rates,
)
from message_ix_models.tools.costs.weo import (
    calculate_region_cost_ratios,
    get_cost_assumption_data,
    get_region_differentiated_costs,
    get_weo_data,
)


# Test function to get first year data for technologies
def test_get_technology_first_year_data():
    res = get_technology_first_year_data()

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "message_technology",
                    "first_year_original",
                    "first_technology_year",
                ]
            ).any()
        )
        is True
    )

    # Check that the final adjusted first year is equal to or greater than 2020
    assert res.first_technology_year.min() > 0


def test_get_cost_reduction_data():
    res = get_cost_reduction_data()

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "message_technology",
                    "technology_type",
                    "scenario",
                    "cost_reduction",
                ]
            ).any()
        )
        is True
    )

    # Check that the max cost reduction is less than 1
    assert res.cost_reduction.max() < 1


# Test function to project investment costs in NAM region using learning rates
def test_project_NAM_capital_costs_using_learning_rates():
    df_weo = get_weo_data()
    df_nam_orig_message = get_cost_assumption_data()
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo)

    df_region_diff = get_region_differentiated_costs(
        df_weo, df_nam_orig_message, df_tech_cost_ratios
    )

    df_learning_rates = get_cost_reduction_data()
    df_technology_first_year = get_technology_first_year_data()

    res = project_NAM_capital_costs_using_learning_rates(
        df_region_diff, df_learning_rates, df_technology_first_year
    )

    # Check that the appropriate columns are present
    assert (
        bool(
            res.columns.isin(
                [
                    "scenario",
                    "message_technology",
                    "weo_technology",
                    "year",
                    "inv_cost_learning_NAM",
                ]
            ).any()
        )
        is True
    )

    # Check that coal_ppl inv_cost_learning_NAM is greater than 0
    assert (
        res.loc[res.message_technology == "coal_ppl", "inv_cost_learning_NAM"].min() > 0
    )
