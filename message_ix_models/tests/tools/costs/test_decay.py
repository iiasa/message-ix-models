from typing import Literal

import pandas as pd
import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.decay import (
    _get_module_cost_reduction,
    _get_module_scenarios_reduction,
    get_technology_reduction_scenarios_data,
    project_ref_region_inv_costs_using_reduction_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
    get_raw_technology_mapping,
    subset_module_map,
)


@pytest.mark.parametrize(
    "module, t_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}),
        ("materials", {"biomass_NH3", "MTO_petro", "furnace_foil_steel"}),
        ("cooling", {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"}),
    ),
)
def test_get_module_scenarios_reduction(
    module: Literal["energy", "materials", "cooling"], t_exp: set[str]
) -> None:
    tech_map = energy_map = get_raw_technology_mapping("energy")

    # if module is not energy, run subset_module_map
    if module != "energy":
        module_map = get_raw_technology_mapping(module)
        module_sub = subset_module_map(module_map)

        # Remove energy technologies that exist in module mapping
        energy_map = energy_map.query(
            "message_technology not in @module_sub.message_technology"
        )

        tech_map = pd.concat([energy_map, module_sub], ignore_index=True)

    result = _get_module_scenarios_reduction(module, energy_map, tech_map)

    # Expected MESSAGEix-GLOBIOM technologies are present in the data
    assert t_exp <= set(result.message_technology.unique())


@pytest.mark.parametrize(
    "module, t_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}),
        ("materials", {"biomass_NH3", "MTO_petro", "furnace_foil_steel"}),
        ("cooling", {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"}),
    ),
)
def test_get_module_cost_reduction(
    module: Literal["energy", "materials", "cooling"], t_exp: set[str]
) -> None:
    tech_map = energy_map = get_raw_technology_mapping("energy")

    # if module is not energy, run subset_module_map
    if module != "energy":
        module_map = get_raw_technology_mapping(module)
        module_sub = subset_module_map(module_map)

        # Remove energy technologies that exist in module mapping
        energy_map = energy_map.query(
            "message_technology not in @module_sub.message_technology"
        )

        tech_map = pd.concat([energy_map, module_sub], ignore_index=True)

    # The function runs without error
    result = _get_module_cost_reduction(module, energy_map, tech_map)

    # Expected MESSAGEix-GLOBIOM technologies are present in the data
    assert t_exp <= set(result.message_technology.unique())


@pytest.mark.parametrize("module", ("energy", "materials", "cooling"))
def test_get_technology_reduction_scenarios_data(
    module: Literal["energy", "materials", "cooling"],
) -> None:
    config = Config()
    # The function runs without error
    result = get_technology_reduction_scenarios_data(config.y0, module=module)

    # All first technology years are equal to or greater than
    # the default first model year
    assert config.y0 <= result.first_technology_year.min()

    # Data for LED and SSP1-5 scenarios are present
    assert {"SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"} <= set(
        result.scenario.unique()
    )


@pytest.mark.parametrize(
    "module, t_exp, t_excluded",
    (
        (
            "energy",
            {"coal_ppl", "gas_cc", "gas_ppl", "solar_res1"},
            {"biomass_NH3"},
        ),
        ("materials", {"biomass_NH3", "MTO_petro", "furnace_foil_steel"}, set()),
        ("cooling", {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"}, set()),
    ),
)
def test_project_ref_region_inv_costs_using_reduction_rates(
    module: Literal["energy", "materials", "cooling"],
    t_exp: set[str],
    t_excluded: set[str],
) -> None:
    # Set up
    config = Config(module=module)
    reg_diff = apply_regional_differentiation(config)

    # The function runs without error
    result = project_ref_region_inv_costs_using_reduction_rates(reg_diff, config)

    # Expected technologies are present
    t = set(result.message_technology.unique())
    assert t_exp <= t

    # Excluded technologies are *not* present
    assert set() == (t_excluded & t)

    # The first technology year is equal to or greater than the default first model year
    assert config.y0 <= result.first_technology_year.min()
