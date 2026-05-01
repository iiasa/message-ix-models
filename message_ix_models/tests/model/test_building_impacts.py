"""Tests for model.buildings.impacts — building energy CID integration."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from message_ix_models.model.buildings.impacts import (
    _demand_to_final_energy_iamc,
    compute_building_cids,
    load_sector_fractions,
    predict_building_ei,
)
from message_ix_models.tools.impacts import GmtArray

_RC_SPEC_BASELINE = Path("_staging/rc_spec_baseline.csv")
_RC_THERM_BASELINE = Path("_staging/rc_therm_baseline.csv")

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
# compute_building_cids end-to-end (requires staged baselines)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not (_RC_SPEC_BASELINE.exists() and _RC_THERM_BASELINE.exists()),
    reason=(
        "Requires SSP2 baseline rc_spec/rc_therm extracts at "
        "_staging/rc_spec_baseline.csv and _staging/rc_therm_baseline.csv; "
        "produced by SPARRCLE staging tooling, not packaged in the repo."
    ),
)
def test_theta_reproduces_calibrated_demand_at_gwl_1_1():
    """At GWL 1.1 (present-day), CID output must match beta * rc."""
    years = np.arange(2020, 2101)
    gmt = GmtArray(values=np.full(len(years), 1.1), years=years)

    cooling, heating = compute_building_cids(
        gmt,
        [2030, 2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100, 2110],
        reference_scenario="SSP2",
    )

    fractions = load_sector_fractions("SSP2")
    rc_spec = pd.read_csv(_RC_SPEC_BASELINE)[["node", "year", "value"]]
    rc_therm = pd.read_csv(_RC_THERM_BASELINE)[["node", "year", "value"]]

    for cid_df, scenario_df, frac_cols in (
        (cooling, rc_spec, ["frac_resid_cool", "frac_comm_cool"]),
        (heating, rc_therm, ["frac_resid_heat", "frac_comm_heat"]),
    ):
        calibrated = scenario_df.merge(
            fractions[["node", "year"] + frac_cols],
            on=["node", "year"],
            how="inner",
        )
        calibrated = calibrated.assign(
            calibrated=lambda df: df["value"] * df[frac_cols].sum(axis=1)
        )[["node", "year", "calibrated"]]

        merged = cid_df.merge(calibrated, on=["node", "year"], how="inner")
        np.testing.assert_allclose(
            merged["value"],
            merged["calibrated"],
            atol=1e-10,
            rtol=0,
        )
