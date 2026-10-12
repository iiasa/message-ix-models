"""Tests for tools.impacts.rime -- RIME prediction engine.

Pure-function tests use synthetic data. Integration tests use
packaged RIME NetCDF datasets.
"""

import logging
from contextlib import AbstractContextManager
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from message_ix_models.tools.impacts import (
    check_emulator_linearity,
    clip_gmt,
    predict_rime,
    resolve_rime_variable,
)

# Fake per-run prediction: shape (n_spatial=3,) returned for each GMT scalar.
_FAKE_SPATIAL = np.ones(3)


def _mock_predict_from() -> AbstractContextManager:
    return patch(
        "message_ix_models.tools.impacts.rime._predict_from_gmt",
        return_value=_FAKE_SPATIAL,
    )


def _mock_linearity(max_deviation: float) -> AbstractContextManager:
    return patch(
        "message_ix_models.tools.impacts.rime.check_emulator_linearity",
        return_value={
            "max_deviation": max_deviation,
            "mean_deviation": max_deviation,
            "is_linear": max_deviation < 0.05,
        },
    )


def test_clip_annual_clipping() -> None:
    gmt = np.array([0.3, 0.5, 0.6, 1.0])
    result = clip_gmt(gmt, gmt_min=0.6, gmt_ceil=0.9)
    # Values below 0.6 clipped to [0.6, 0.9]
    assert result[0] >= 0.6
    assert result[0] <= 0.9
    assert result[1] >= 0.6
    assert result[1] <= 0.9
    # Value at 0.6 should be unchanged
    assert result[2] == 0.6
    assert result[3] == 1.0


def test_clip_seasonal_higher_threshold() -> None:
    gmt = np.array([0.7, 0.8, 1.0])
    result = clip_gmt(gmt, gmt_min=0.8, gmt_ceil=1.2)
    # 0.7 is below seasonal min of 0.8, clipped to [0.8, 1.2]
    assert result[0] >= 0.8
    assert result[0] <= 1.2
    # 0.8 is at boundary, unchanged
    assert result[1] == 0.8


def test_clip_2d_input() -> None:
    gmt = np.array([[0.3, 1.0, 2.0], [0.5, 1.5, 3.0]])
    result = clip_gmt(gmt)
    assert result.shape == (2, 3)
    assert result[0, 0] >= 0.6
    assert result[0, 1] == 1.0


def test_predict_linear_emulator_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Linear emulator (deviation <= 1%): no linearity warning emitted."""
    gmt_2d = np.ones((5, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.005):
        with caplog.at_level(
            logging.WARNING, logger="message_ix_models.tools.impacts.rime"
        ):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

    assert not any("non-linear" in r.message for r in caplog.records)


def test_predict_nonlinear_multi_run_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Non-linear emulator + n_runs > 1: log warning emitted, no raise."""
    gmt_2d = np.ones((5, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.08):
        with caplog.at_level(
            logging.WARNING, logger="message_ix_models.tools.impacts.rime"
        ):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

    assert any("non-linear" in r.message for r in caplog.records)


def test_predict_nonlinear_single_run_raises() -> None:
    """Non-linear emulator + n_runs == 1: ValueError."""
    gmt_2d = np.ones((1, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.08):
        with pytest.raises(ValueError, match="non-linear emulator"):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")


def test_predict_linear_single_run_no_raise() -> None:
    """Linear emulator + n_runs == 1: no error."""
    gmt_2d = np.ones((1, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.005):
        result = predict_rime(gmt_2d, "dummy.nc", "qtot_mean")
    assert result.shape == (3, 4)


def test_response_percentile_is_selected_inside_each_gwl_window() -> None:
    dataset = xr.Dataset(
        {
            "impact": ("gwl", [1.0, 2.0]),
            "impact_p10": ("gwl", [0.8, 1.6]),
            "impact_p50": ("gwl", [1.0, 2.0]),
            "impact_p90": ("gwl", [1.2, 2.4]),
        },
        coords={"gwl": [1.0, 2.0]},
    )
    with patch(
        "message_ix_models.tools.impacts.rime.open_rime_dataset",
        return_value=dataset,
    ):
        result = predict_rime(np.array([1.0, 2.0]), "dummy.nc", "impact", percentile=90)

    np.testing.assert_allclose(result, [1.2, 2.4])


def test_resolve_response_percentile_reports_available_values() -> None:
    dataset = xr.Dataset(
        {"impact_p10": ("gwl", [0.8]), "impact_p50": ("gwl", [1.0])},
        coords={"gwl": [1.0]},
    )
    with patch(
        "message_ix_models.tools.impacts.rime.open_rime_dataset",
        return_value=dataset,
    ):
        assert resolve_rime_variable("dummy.nc", "impact", "p50") == "impact_p50"
        with pytest.raises(ValueError, match="impact_p90.*impact_p10.*impact_p50"):
            resolve_rime_variable("dummy.nc", "impact", 90)


# Integration tests on packaged RIME data
_FILE = "r12_thermoelectric_gwl.nc"
_SEL = {"cooling": "wet"}


def test_predict_capacity_factor_1d() -> None:
    gmt = np.linspace(1.0, 2.5, 10)
    result = predict_rime(gmt, _FILE, "capacity_factor", sel=_SEL)
    assert result.shape == (12, 10)


def test_predict_capacity_factor_2d() -> None:
    rng = np.random.default_rng(42)
    gmt_2d = np.clip(rng.normal(1.5, 0.2, size=(5, 10)), 0.6, 7.4)
    result = predict_rime(gmt_2d, _FILE, "capacity_factor", sel=_SEL)
    assert result.shape == (12, 10)


def test_linearity_check_integration() -> None:
    result = check_emulator_linearity(
        _FILE, "capacity_factor", gmt_range=(1.0, 3.0), n_probe=5
    )
    assert "max_deviation" in result
    assert "is_linear" in result
    assert isinstance(result["is_linear"], bool)
