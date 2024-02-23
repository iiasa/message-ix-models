from typing import Literal

import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.learning import (
    get_cost_reduction_data,
    get_technology_learning_scenarios_data,
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)


@pytest.mark.parametrize(
    "module, t_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"}),
        ("materials", {"biomass_NH3", "MTO_petro", "furnace_foil_steel"}),
    ),
)
def test_get_cost_reduction_data(module: str, t_exp) -> None:
    # The function runs without error
    result = get_cost_reduction_data(module)

    # Expected MESSAGEix-GLOBIOM technologies are present in the data
    assert t_exp <= set(result.message_technology.unique())

    # Values of the "cost reduction" columns are between 0 and 1
    stats = result.cost_reduction.describe()
    assert 0 <= stats["min"] and stats["max"] <= 1


@pytest.mark.parametrize("module", ("energy", "materials"))
def test_get_technology_learning_scenarios_data(module: str) -> None:
    # The function runs without error
    result = get_technology_learning_scenarios_data(Config.base_year, module=module)

    # All first technology years are equal to or greater than the default base year
    assert Config.base_year <= result.first_technology_year.min()

    # Data for LED and SSP1-5 scenarios are present
    assert {"SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"} <= set(
        result.scenario.unique()
    )


@pytest.mark.parametrize(
    "module, t_exp, t_excluded",
    (
        ("energy", {"coal_ppl", "gas_cc", "gas_ppl", "solar_pv_ppl"}, {"biomass_NH3"}),
        ("materials", {"biomass_NH3", "MTO_petro", "furnace_foil_steel"}, set()),
    ),
)
def test_project_ref_region_inv_costs_using_learning_rates(
    module: Literal["energy", "materials"], t_exp, t_excluded
) -> None:
    # Set up
    config = Config(module=module)
    reg_diff = apply_regional_differentiation(config)

    # The function runs without error
    result = project_ref_region_inv_costs_using_learning_rates(reg_diff, config)

    # Expected technologies are present
    t = set(result.message_technology.unique())
    assert t_exp <= t

    # Excluded technologies are *not* present
    assert set() == (t_excluded & t)

    # The first technology year is equal to or greater than the default base year
    assert Config.base_year <= result.first_technology_year.min()
