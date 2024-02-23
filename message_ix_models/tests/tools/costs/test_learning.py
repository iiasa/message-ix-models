from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.learning import (
    get_cost_reduction_data,
    get_technology_learning_scenarios_data,
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)


def test_get_cost_reduction_data() -> None:
    # Assert that the energy module is present
    cost_red_energy = get_cost_reduction_data(module="energy")

    # Assert that the materials module is present
    cost_red_materials = get_cost_reduction_data(module="materials")

    # Assert that certain energy technologies are present in the energy module
    energy_techs = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]
    assert (
        bool(
            all(i in cost_red_energy.message_technology.unique() for i in energy_techs)
        )
        is True
    )

    # Assert that certain materials technologies are present in the materials module
    materials_techs = ["biomass_NH3", "MTO_petro", "furnace_foil_steel"]
    assert (
        bool(
            all(
                i in cost_red_materials.message_technology.unique()
                for i in materials_techs
            )
        )
        is True
    )

    # Assert that the cost reduction values are between 0 and 1
    assert cost_red_energy.cost_reduction.min() >= 0
    assert cost_red_energy.cost_reduction.max() <= 1

    assert cost_red_materials.cost_reduction.min() >= 0
    assert cost_red_materials.cost_reduction.max() <= 1


def test_get_technology_learning_scenarios_data() -> None:
    energy = get_technology_learning_scenarios_data(base_year=2021, module="energy")
    materials = get_technology_learning_scenarios_data(
        base_year=2021, module="materials"
    )

    # Check that all first technology years are equal to or greater than 2021
    assert energy.first_technology_year.min() >= 2021
    assert materials.first_technology_year.min() >= 2021

    # Check that LED and SSP1-5 are present in each module
    scens = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    assert bool(all(i in energy.scenario.unique() for i in scens)) is True
    assert bool(all(i in materials.scenario.unique() for i in scens)) is True


def test_project_ref_region_inv_costs_using_learning_rates() -> None:
    # TODO Parametrize this test
    c0 = Config(base_year=2021)
    r12_energy_reg_diff = apply_regional_differentiation(c0)

    c1 = Config(base_year=2021, module="materials")
    r12_materials_reg_diff = apply_regional_differentiation(c1)

    r12_energy_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r12_energy_reg_diff, config=c0
    )

    r12_materials_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r12_materials_reg_diff, config=c1
    )

    a = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]
    b = ["biomass_NH3"]
    c = [
        "biomass_NH3",
        "MTO_petro",
        "furnace_foil_steel",
    ]

    # Check that only base technologies are present in the base module
    assert bool(all(i in r12_energy_res.message_technology.unique() for i in a)) is True
    assert (
        bool(all(i in r12_energy_res.message_technology.unique() for i in b)) is False
    )

    # Check that materials technologies are present in the materials module
    assert (
        bool(all(i in r12_materials_res.message_technology.unique() for i in c)) is True
    )

    # Assert that the first technology year is equal to or greater than 2021
    assert r12_energy_res.first_technology_year.min() >= 2021
    assert r12_materials_res.first_technology_year.min() >= 2021
