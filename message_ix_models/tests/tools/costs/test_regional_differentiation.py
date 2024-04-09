import numpy as np
import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.regional_differentiation import (
    adjust_technology_mapping,
    apply_regional_differentiation,
    get_intratec_data,
    get_raw_technology_mapping,
    get_weo_data,
)


def test_get_weo_data() -> None:
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
    assert np.isclose(
        1324.68,
        result.query(
            "weo_technology == 'steam_coal_subcritical'"
            "and weo_region == 'United States'"
            "and year == '2021'"
            "and cost_type == 'inv_cost'"
        )["value"].item(),
    )


def test_get_intratec_data() -> None:
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


@pytest.mark.parametrize(
    "module, t_exp, rds_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"}, {"weo"}),
        ("materials", {"biomass_NH3", "meth_h2", "furnace_foil_steel"}, {"energy"}),
    ),
)
def test_get_raw_technology_mapping(module, t_exp, rds_exp) -> None:
    # Function runs without error
    result = get_raw_technology_mapping(module)

    # Expected technologies are present
    assert t_exp <= set(result.message_technology.unique())

    # Expected values for regional differentiation sources
    assert rds_exp <= set(result.reg_diff_source.unique())


@pytest.mark.parametrize("module", ("energy", "materials"))
def test_adjust_technology_mapping(module) -> None:
    energy_raw = get_raw_technology_mapping("energy")

    # Function runs without error
    result = adjust_technology_mapping(module)

    # For module="energy", adjustment has no effect; output data are the same
    if module == "energy":
        assert energy_raw.equals(result)

    # The "energy" regional differentiation source is not present in the result data
    assert "energy" not in result.reg_diff_source.unique()

    # The "weo" regional differentiation source is present in the result data
    assert "weo" in result.reg_diff_source.unique()


@pytest.mark.parametrize(
    "module, t_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl"}),
        ("materials", {"biomass_NH3", "meth_h2", "furnace_foil_steel"}),
    ),
)
def test_apply_regional_differentiation(module, t_exp) -> None:
    """Regional differentiation is applied correctly for each `module`."""
    config = Config(module=module)

    # Function runs without error
    result = apply_regional_differentiation(config)

    # Assert that certain technologies are present in the energy module

    # Expected technologies are present
    assert t_exp <= set(result.message_technology.unique())

    # For technologies whose reg_diff_source and reg_diff_technology are NaN, the
    # reg_cost_ratio is 1 (i.e., no regional differentiation)
    assert (
        result.query(
            "reg_diff_source.isna() and reg_diff_technology.isna()"
        ).reg_cost_ratio
        == 1
    ).all()
