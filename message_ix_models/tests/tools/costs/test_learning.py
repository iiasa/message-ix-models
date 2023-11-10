from message_ix_models.tools.costs.learning import (
    get_cost_reduction_data,
    get_technology_learning_scenarios_data,
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    get_weo_region_differentiated_costs,
)


def test_get_cost_reduction_data():
    base = get_cost_reduction_data(input_module="base")
    mat = get_cost_reduction_data(input_module="materials")

    a = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]
    b = ["biomass_NH3"]
    c = [
        "coal_ppl",
        "gas_ppl",
        "gas_cc",
        "biomass_NH3",
        "biomass_NH3",
        "furnace_foil_steel",
    ]

    # Check that only base technologies are present in the base module
    assert bool(all(i in base.message_technology.unique() for i in a)) is True
    assert bool(all(i in base.message_technology.unique() for i in b)) is False

    # Check that base and materials technologies are present in the materials module
    assert bool(all(i in mat.message_technology.unique() for i in c)) is True

    # Check that the cost reduction values are between 0 and 1
    assert base.cost_reduction.min() >= 0
    assert base.cost_reduction.max() <= 1
    assert mat.cost_reduction.min() >= 0
    assert mat.cost_reduction.max() <= 1


def test_get_technology_learning_scenarios_data():
    base = get_technology_learning_scenarios_data(
        input_base_year=2021, input_module="base"
    )
    mat = get_technology_learning_scenarios_data(
        input_base_year=2021, input_module="materials"
    )

    # Check that all first technology years are equal to or greater than 2021
    assert base.first_technology_year.min() >= 2021
    assert mat.first_technology_year.min() >= 2021

    # Check that LED and SSP1-5 are present in each module
    scens = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    assert bool(all(i in base.scenario.unique() for i in scens)) is True
    assert bool(all(i in mat.scenario.unique() for i in scens)) is True


def test_project_ref_region_inv_costs_using_learning_rates():
    r11_base_reg_diff = get_weo_region_differentiated_costs(
        input_node="r11",
        input_ref_region="R11_NAM",
        input_base_year=2021,
        input_module="base",
    )

    r11_materials_reg_diff = get_weo_region_differentiated_costs(
        input_node="r11",
        input_ref_region="R11_NAM",
        input_base_year=2021,
        input_module="materials",
    )

    r12_base_reg_diff = get_weo_region_differentiated_costs(
        input_node="r12",
        input_ref_region="R12_NAM",
        input_base_year=2021,
        input_module="base",
    )

    r12_materials_reg_diff = get_weo_region_differentiated_costs(
        input_node="r12",
        input_ref_region="R12_NAM",
        input_base_year=2021,
        input_module="materials",
    )

    r11_base_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r11_base_reg_diff,
        input_node="r11",
        input_ref_region="R11_NAM",
        input_base_year=2021,
        input_module="base",
    )

    r11_materials_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r11_materials_reg_diff,
        input_node="r11",
        input_ref_region="R11_NAM",
        input_base_year=2021,
        input_module="materials",
    )

    r12_base_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r12_base_reg_diff,
        input_node="r12",
        input_ref_region="R12_NAM",
        input_base_year=2021,
        input_module="base",
    )

    r12_materials_res = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=r12_materials_reg_diff,
        input_node="r12",
        input_ref_region="R12_NAM",
        input_base_year=2021,
        input_module="materials",
    )

    a = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]
    b = ["biomass_NH3"]
    c = [
        "coal_ppl",
        "gas_ppl",
        "gas_cc",
        "biomass_NH3",
        "biomass_NH3",
        "furnace_foil_steel",
    ]

    # Check that only base technologies are present in the base module
    assert bool(all(i in r11_base_res.message_technology.unique() for i in a)) is True
    assert bool(all(i in r11_base_res.message_technology.unique() for i in b)) is False
    assert bool(all(i in r12_base_res.message_technology.unique() for i in a)) is True
    assert bool(all(i in r12_base_res.message_technology.unique() for i in b)) is False

    # Check that base and materials technologies are present in the materials module
    assert (
        bool(all(i in r11_materials_res.message_technology.unique() for i in c)) is True
    )
    assert (
        bool(all(i in r12_materials_res.message_technology.unique() for i in c)) is True
    )

    # Assert that the first technology year is equal to or greater than 2021
    assert r11_base_res.first_technology_year.min() >= 2021
    assert r11_materials_res.first_technology_year.min() >= 2021
    assert r12_base_res.first_technology_year.min() >= 2021
    assert r12_materials_res.first_technology_year.min() >= 2021
