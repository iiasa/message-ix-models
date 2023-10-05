from message_ix_models.tools.costs.weo import (
    get_technology_mapping,
    get_weo_data,
    get_weo_region_differentiated_costs,
)


def test_get_weo_data():
    result = get_weo_data()

    # Check that the minimum and maximum years are correct
    assert min(result.year) == "2021"
    assert max(result.year) == "2050"

    # Check that the regions are correct
    # (e.g., in the past, "Europe" changed to "European Union")
    assert all(
        [
            "European Union",
            "United States",
            "Japan",
            "Russia",
            "China",
            "India",
            "Middle East",
            "Africa",
            "Brazil",
        ]
        == result.weo_region.unique()
    )

    # Check one sample value
    assert (
        result.query(
            "weo_technology == 'steam_coal_subcritical' and \
                weo_region == 'United States' and \
                    year == '2021' and cost_type == 'inv_cost'"
        ).value.values[0]
        == 1296.0
    )


def test_get_technology_mapping():
    base = get_technology_mapping(input_module="base")
    mat = get_technology_mapping(input_module="materials")

    a = base.message_technology.unique()
    b = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]
    c = ["biomass_NH3"]
    d = mat.message_technology.unique()
    e = ["coal_ppl", "gas_ppl", "gas_cc", "biomass_NH3", "furnace_foil_steel"]

    # Assert that some main energy technologies are present in the base module
    assert bool(all(i in a for i in b)) is True

    # Assert that materials-specific technologies are not present in the base module
    assert bool(all(i in a for i in c)) is False

    # Assert that some materials-specific technologies are present
    # in the materials module
    assert bool(all(i in d for i in e)) is True


def test_get_weo_region_differentiated_costs():
    res = get_weo_region_differentiated_costs(
        input_node="r12",
        input_ref_region="R12_NAM",
        input_base_year=2021,
        input_module="base",
    )

    # Assert that all reference region cost ratios are equal to 1
    assert all(res.query("region == 'R12_NAM'").reg_cost_ratio.values == 1.0)

    # Assert that all cost values are greater than 0
    assert all(res.reg_cost_ratio.values > 0)
