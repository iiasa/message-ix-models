import numpy as np
import pytest

from message_ix_models.tools.costs import MODULE, Config
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
    assert min(result.year) == "2022"
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
        1238.018,
        result.query(
            "weo_technology == 'steam_coal_subcritical'"
            "and weo_region == 'United States'"
            "and year == '2022'"
            "and cost_type == 'inv_cost'"
        )["value"].item(),
    )


def test_get_intratec_data() -> None:
    res = get_intratec_data()

    # Check that the regions of R12 are present
    assert all(
        [
            "R11_NAM",
            "R11_LAM",
            "R11_WEU",
            "R11_EEU",
            "R11_FSU",
            "R11_AFR",
            "R11_MEA",
            "R11_SAS",
            "R11_CPA",
            "R11_PAS",
            "R11_PAO",
        ]
        == res.node.unique()
    )


@pytest.mark.parametrize(
    "module, t_exp, rds_exp",
    (
        (MODULE.energy, {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}, {"weo"}),
        (
            MODULE.materials,
            {"biomass_NH3", "meth_h2", "furnace_foil_steel"},
            {"energy"},
        ),
        (
            MODULE.cooling,
            {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"},
            {"energy"},
        ),
    ),
)
def test_get_raw_technology_mapping(module: MODULE, t_exp, rds_exp) -> None:
    # Function runs without error
    result = get_raw_technology_mapping(module)

    # Expected technologies are present
    assert t_exp <= set(result.message_technology.unique())

    # Expected values for regional differentiation sources
    assert rds_exp <= set(result.reg_diff_source.unique())


@pytest.mark.parametrize("module", list(MODULE))
def test_adjust_technology_mapping(module: MODULE) -> None:
    energy_raw = get_raw_technology_mapping(MODULE.energy)

    # Function runs without error
    result = adjust_technology_mapping(module)

    # For module="energy", adjustment has no effect; output data are the same
    if module == MODULE.energy:
        assert energy_raw.equals(result)

    # The "energy" regional differentiation source is not present in the result data
    assert "energy" not in result.reg_diff_source.unique()

    # The "weo" regional differentiation source is present in the result data
    assert "weo" in result.reg_diff_source.unique()


@pytest.mark.parametrize(
    "module, t_exp",
    (
        (MODULE.energy, {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}),
        (MODULE.materials, {"biomass_NH3", "meth_h2", "furnace_foil_steel"}),
        (MODULE.cooling, {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"}),
    ),
)
def test_apply_regional_differentiation(module: MODULE, t_exp) -> None:
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
