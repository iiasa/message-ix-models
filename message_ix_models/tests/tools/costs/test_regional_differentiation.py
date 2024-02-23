from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.regional_differentiation import (
    adjust_technology_mapping,
    apply_regional_differentiation,
    get_intratec_data,
    get_raw_technology_mapping,
    get_weo_data,
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
            "weo_technology == 'steam_coal_subcritical'"
            "and weo_region == 'United States'"
            "and year == '2021'"
            "and cost_type == 'inv_cost'"
        ).value.values[0]
        == 1296.0
    )


def test_get_intratec_data():
    res = get_intratec_data()

    # Check that the regions of R12 are present
    assert all(
        [
            "NAM",
            "LAM",
            "WEU",
            "EEU",
            "FSU",
            "AFR",
            "MEA",
            "SAS",
            "RCPA",
            "PAS",
            "PAO",
            "CHN",
        ]
        == res.intratec_region.unique()
    )


def test_get_raw_technology_mapping():
    energy = get_raw_technology_mapping("energy")

    # Assert that certain energy technologies are present
    energy_tech = [
        "coal_ppl",
        "gas_ppl",
        "gas_cc",
        "solar_pv_ppl",
    ]
    assert (
        bool(all(i in energy.message_technology.unique() for i in energy_tech)) is True
    )

    materials = get_raw_technology_mapping("materials")

    # Assert that certain materials technologies are present
    materials_tech = ["biomass_NH3", "meth_h2", "furnace_foil_steel"]

    assert (
        bool(all(i in materials.message_technology.unique() for i in materials_tech))
        is True
    )

    # Assert that "energy" is one of the regional differentiation sources
    assert "energy" in materials.reg_diff_source.unique()


def test_adjust_technology_mapping():
    energy_raw = get_raw_technology_mapping("energy")
    energy_adj = adjust_technology_mapping("energy")

    # Assert that the output of raw and adjusted technology mapping are the same
    # for the energy module
    assert energy_raw.equals(energy_adj)

    # materials_raw = get_raw_technology_mapping("materials")
    materials_adj = adjust_technology_mapping("materials")

    # Assert that the "energy" regional differentiation source is no longer present
    # in the materials module
    assert "energy" not in materials_adj.reg_diff_source.unique()

    # Assert that the "weo" regional differentiation source is present
    # in the materials module
    assert "weo" in materials_adj.reg_diff_source.unique()


def test_apply_regional_differentiation():
    # Assert that the regional differentiation is applied correctly
    # for the energy module
    config = Config()
    energy_r12_nam = apply_regional_differentiation(config)

    # Assert that the regional differentiation is applied correctly
    # for the materials module
    config.module = "materials"
    materials_r12_nam = apply_regional_differentiation(config)

    # Assert that certain technologies are present in the energy module
    energy_tech = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"]

    assert (
        bool(all(i in energy_r12_nam.message_technology.unique() for i in energy_tech))
        is True
    )

    # Assert that certain technologies are present in the materials module
    materials_tech = ["biomass_NH3", "meth_h2", "furnace_foil_steel"]

    assert (
        bool(
            all(
                i in materials_r12_nam.message_technology.unique()
                for i in materials_tech
            )
        )
        is True
    )

    # For technologies whose reg_diff_source and reg_diff_technology are NaN,
    # assert that the reg_cost_ratio are 1 (i.e., no regional differentiation)
    assert (
        bool(
            all(
                materials_r12_nam.query(
                    "reg_diff_source.isna() and reg_diff_technology.isna()"
                ).reg_cost_ratio
                == 1
            )
        )
        is True
    )
