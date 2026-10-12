"""Tests for :mod:`message_ix_models.model.buildings.impacts`."""

from collections.abc import Callable

import numpy as np
import pandas as pd
import pytest

from message_ix_models.model.buildings.impacts import (
    _apply_theta,
    _mfh_weighted_ei,
    compute_building_cids,
    load_correction_coefficients,
    load_floor_areas,
    load_sector_fractions,
    load_theta,
    predict_building_ei,
)
from message_ix_models.tools.impacts import GmtArray

#: Model periods of the R12 SSP scenarios.
MODEL_YEARS = [2020, 2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060]
MODEL_YEARS += [2070, 2080, 2090, 2100, 2110]

#: Annual years of a MAGICC GSAT trajectory.
GMT_YEARS = np.arange(2015, 2101)


def flat_gmt(level: float) -> GmtArray:
    """A single GSAT trajectory held at *level* °C."""
    return GmtArray(np.full(GMT_YEARS.size, level), GMT_YEARS)


@pytest.mark.parametrize(
    "loader, args, required_columns",
    [
        (load_theta, ("cool", "SSP1"), {"node", "year", "theta"}),
        (load_sector_fractions, ("SSP2",), {"node", "year"}),
        (load_floor_areas, ("resid",), {"region", "year", "floor_Mm2"}),
        (
            load_correction_coefficients,
            ("cool", "resid"),
            {"region", "arch", "urt", "year", "correction_coeff"},
        ),
    ],
)
def test_calibration_loaders(
    loader: Callable[..., pd.DataFrame], args: tuple, required_columns: set[str]
) -> None:
    df = loader(*args)
    assert required_columns <= set(df.columns)
    assert not df.empty


def test_warming_increases_cooling_ei() -> None:
    ei_cool = predict_building_ei(np.array([1.0, 1.5, 2.0]), "cool")
    mean_per_year = np.nanmean(ei_cool, axis=(0, 1, 2))
    assert mean_per_year[2] > mean_per_year[0]


def test_warming_decreases_heating_ei() -> None:
    ei_heat = predict_building_ei(np.array([1.0, 1.5, 2.0]), "heat")
    mean_per_year = np.nanmean(ei_heat, axis=(0, 1, 2))
    assert mean_per_year[2] < mean_per_year[0]


def test_building_ei_selects_gwl_window_response_percentile() -> None:
    gmt = np.array([1.5, 2.0])
    low = predict_building_ei(gmt, "cool", percentile=10)
    high = predict_building_ei(gmt, "cool", percentile=90)
    assert np.nanmean(high) > np.nanmean(low)


@pytest.mark.parametrize("ssp", ["SSP1", "SSP2", "SSP3"])
def test_compute_building_cids_covers_model_years(ssp: str) -> None:
    cooling, heating = compute_building_cids(flat_gmt(1.5), MODEL_YEARS, ssp)

    first = min(load_theta(m, ssp)["year"].min() for m in ("cool", "heat"))
    expected_years = [y for y in MODEL_YEARS if y >= first]
    for result in (cooling, heating):
        assert list(result.columns) == ["node", "year", "value"]
        assert result["node"].nunique() == 12
        assert result["node"].str.startswith("R12_").all()
        for _, group in result.groupby("node"):
            assert group["year"].tolist() == expected_years
        assert np.isfinite(result["value"]).all()
        assert (result["value"] > 0).all()

        # 2110 lies beyond the GMT trajectory and is carried from 2100
        by_year = result.set_index(["node", "year"])["value"].unstack()
        np.testing.assert_allclose(by_year[2110], by_year[2100])


def test_compute_building_cids_responds_to_warming() -> None:
    cool_lo, heat_lo = compute_building_cids(flat_gmt(1.2), MODEL_YEARS, "SSP2")
    cool_hi, heat_hi = compute_building_cids(flat_gmt(2.5), MODEL_YEARS, "SSP2")

    assert cool_hi["value"].sum() > cool_lo["value"].sum()
    assert heat_hi["value"].sum() < heat_lo["value"].sum()


def test_compute_building_cids_ensemble_of_identical_runs() -> None:
    """Reducing an ensemble of identical runs gives the single-run result."""
    single = flat_gmt(1.8)
    ensemble = GmtArray(np.tile(single.values, (5, 1)), GMT_YEARS)

    for a, b in zip(
        compute_building_cids(single, MODEL_YEARS, "SSP2"),
        compute_building_cids(ensemble, MODEL_YEARS, "SSP2"),
        strict=True,
    ):
        pd.testing.assert_frame_equal(a, b)


def test_compute_building_cids_rejects_other_regions() -> None:
    with pytest.raises(NotImplementedError, match="R12"):
        compute_building_cids(flat_gmt(1.5), MODEL_YEARS, "SSP2", regions="R11")


def test_compute_building_cids_requires_overlapping_years() -> None:
    gmt = GmtArray(np.full(10, 1.5), np.arange(1900, 1910))
    with pytest.raises(ValueError, match="No model years"):
        compute_building_cids(gmt, MODEL_YEARS, "SSP2")


def test_mfh_weighted_ei_weights_by_2020_floor_area() -> None:
    ei = pd.DataFrame(
        {
            "region": ["AFR"] * 3,
            "arch": ["mfh_s1", "mfh_s2", "sfh_s1"],
            "urt": ["urban"] * 3,
            "year": [2050] * 3,
            "ei": [10.0, 20.0, 1000.0],
        }
    )
    floor = pd.DataFrame(
        {
            "region": ["AFR"] * 4,
            "arch": ["mfh_s1", "mfh_s2", "sfh_s1", "mfh_s1"],
            "urt": ["urban"] * 4,
            "year": [2020, 2020, 2020, 2050],
            "floor_Mm2": [1.0, 3.0, 50.0, 100.0],
        }
    )

    result = _mfh_weighted_ei(ei, floor)

    # Single-family archetypes and non-2020 floor areas carry no weight
    assert result.to_dict("records") == [
        {"region": "AFR", "urt": "urban", "year": 2050, "ei": 17.5}
    ]


def test_mfh_weighted_ei_without_mfh_floor_area() -> None:
    ei = pd.DataFrame(
        {"region": ["AFR"], "arch": ["mfh_s1"], "urt": ["urban"], "year": [2050]}
    ).assign(ei=1.0)
    floor = pd.DataFrame(
        {
            "region": ["AFR"],
            "arch": ["sfh_s1"],
            "urt": ["urban"],
            "year": [2020],
            "floor_Mm2": [1.0],
        }
    )

    result = _mfh_weighted_ei(ei, floor)

    assert result.empty
    assert list(result.columns) == ["region", "urt", "year", "ei"]


def test_apply_theta_scales_by_calibration() -> None:
    theta = load_theta("cool", "SSP2").set_index(["node", "year"])["theta"]
    demand = pd.DataFrame({"node": ["R12_AFR", "R12_CHN"], "year": [2050, 2050]})
    demand = demand.assign(value=2.0)

    result = _apply_theta(demand, "cool", "SSP2")

    expected = [2.0 * theta[("R12_AFR", 2050)], 2.0 * theta[("R12_CHN", 2050)]]
    np.testing.assert_allclose(result["value"], expected)


def test_apply_theta_missing_node_raises() -> None:
    demand = pd.DataFrame({"node": ["R12_XYZ"], "year": [2050], "value": [1.0]})
    with pytest.raises(ValueError, match="Missing theta"):
        _apply_theta(demand, "heat", "SSP2")
