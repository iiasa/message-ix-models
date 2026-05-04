"""Tests for model.buildings.impacts — building energy CID integration."""

import numpy as np
import pandas as pd

from message_ix_models.model.buildings.impacts import (
    _demand_to_final_energy_iamc,
    predict_building_ei,
    prepare_building_demand,
)

# ---------------------------------------------------------------------------
# predict_building_ei
# ---------------------------------------------------------------------------


def test_warming_increases_cooling_ei():
    gmt = np.array([1.0, 1.5, 2.0])
    ei_cool = predict_building_ei(gmt, "cool")
    mean_per_year = np.nanmean(ei_cool, axis=(0, 1, 2))
    assert mean_per_year[2] > mean_per_year[0]


def test_warming_decreases_heating_ei():
    gmt = np.array([1.0, 1.5, 2.0])
    ei_heat = predict_building_ei(gmt, "heat")
    mean_per_year = np.nanmean(ei_heat, axis=(0, 1, 2))
    assert mean_per_year[2] < mean_per_year[0]


# ---------------------------------------------------------------------------
# IAMC packaging helpers
# ---------------------------------------------------------------------------


def test_demand_to_final_energy_iamc_converts_gwa_to_ej_per_year():
    demand = pd.DataFrame(
        {
            "node": ["R12_AFR", "R12_AFR"],
            "year": [2030, 2035],
            "value": [1.0, 2.0],
        }
    )

    result = _demand_to_final_energy_iamc(
        demand, "Final Energy|Residential and Commercial|Cooling"
    )

    assert result["unit"].unique().tolist() == ["EJ/yr"]
    assert result["variable"].unique().tolist() == [
        "Final Energy|Residential and Commercial|Cooling"
    ]
    np.testing.assert_allclose(result["value"], [0.0315576, 0.0631152])


# ---------------------------------------------------------------------------
# prepare_building_demand — substitution arithmetic
# ---------------------------------------------------------------------------

# Minimal columns required by prepare_building_demand / _substitute_climate_component.
_RC_SPEC = pd.DataFrame(
    {"node": ["R12_AFR"], "year": [2050], "commodity": ["rc_spec"], "value": [100.0]}
)
_RC_THERM = pd.DataFrame(
    {"node": ["R12_AFR"], "year": [2050], "commodity": ["rc_therm"], "value": [200.0]}
)
_FRACTIONS = pd.DataFrame(
    {
        "node": ["R12_AFR"],
        "year": [2050],
        "frac_resid_cool": [0.2],
        "frac_comm_cool": [0.1],
        "frac_resid_heat": [0.3],
        "frac_comm_heat": [0.1],
    }
)


def test_prepare_building_demand_substitution_arithmetic():
    """new_value = old_value * (1 - frac_total) + cid_value."""
    cooling_cids = pd.DataFrame({"node": ["R12_AFR"], "year": [2050], "value": [15.0]})
    heating_cids = pd.DataFrame({"node": ["R12_AFR"], "year": [2050], "value": [30.0]})

    new_spec, new_therm = prepare_building_demand(
        _RC_SPEC.copy(),
        _RC_THERM.copy(),
        cooling_cids,
        heating_cids,
        fractions=_FRACTIONS,
    )

    # rc_spec: 100 * (1 - 0.3) + 15 = 85.0
    np.testing.assert_allclose(new_spec["value"].values, [85.0])
    # rc_therm: 200 * (1 - 0.4) + 30 = 150.0
    np.testing.assert_allclose(new_therm["value"].values, [150.0])


def test_prepare_building_demand_missing_cid_zerofills():
    """When CID has no rows for a (node, year), its contribution is zero."""
    empty_cids = pd.DataFrame(
        {"node": pd.Series(dtype=str), "year": pd.Series(dtype=int), "value": pd.Series(dtype=float)}
    )

    new_spec, new_therm = prepare_building_demand(
        _RC_SPEC.copy(),
        _RC_THERM.copy(),
        empty_cids,
        empty_cids,
        fractions=_FRACTIONS,
    )

    # rc_spec: 100 * (1 - 0.3) + 0 = 70.0
    np.testing.assert_allclose(new_spec["value"].values, [70.0])
    # rc_therm: 200 * (1 - 0.4) + 0 = 120.0
    np.testing.assert_allclose(new_therm["value"].values, [120.0])
