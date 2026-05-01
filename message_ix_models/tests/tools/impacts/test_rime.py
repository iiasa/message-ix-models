"""Tests for tools.impacts.rime -- RIME prediction engine.

Pure-function tests use synthetic data. Integration tests that need actual
RIME NetCDF datasets are marked with skipif.
"""

import logging
from unittest.mock import patch

import numpy as np
import pytest

from message_ix_models.tools.impacts import (
    check_emulator_linearity,
    clip_gmt,
    impacts_data_path,
    predict_rime,
)

# Fake per-run prediction: shape (n_spatial=3,) returned for each GMT scalar.
_FAKE_SPATIAL = np.ones(3)


def _mock_predict_from():
    return patch(
        "message_ix_models.tools.impacts.rime._predict_from_gmt",
        return_value=_FAKE_SPATIAL,
    )


def _mock_linearity(max_deviation: float):
    return patch(
        "message_ix_models.tools.impacts.rime.check_emulator_linearity",
        return_value={
            "max_deviation": max_deviation,
            "mean_deviation": max_deviation,
            "is_linear": max_deviation < 0.05,
        },
    )


def test_clip_annual_clipping():
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


def test_clip_seasonal_higher_threshold():
    gmt = np.array([0.7, 0.8, 1.0])
    result = clip_gmt(gmt, gmt_min=0.8, gmt_ceil=1.2)
    # 0.7 is below seasonal min of 0.8, clipped to [0.8, 1.2]
    assert result[0] >= 0.8
    assert result[0] <= 1.2
    # 0.8 is at boundary, unchanged
    assert result[1] == 0.8


def test_clip_2d_input():
    gmt = np.array([[0.3, 1.0, 2.0], [0.5, 1.5, 3.0]])
    result = clip_gmt(gmt)
    assert result.shape == (2, 3)
    assert result[0, 0] >= 0.6
    assert result[0, 1] == 1.0


def test_predict_linear_emulator_no_warning(caplog):
    """Linear emulator (deviation <= 1%): no linearity warning emitted."""
    gmt_2d = np.ones((5, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.005):
        with caplog.at_level(
            logging.WARNING, logger="message_ix_models.tools.impacts.rime"
        ):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

    assert not any("non-linear" in r.message for r in caplog.records)


def test_predict_nonlinear_multi_run_warns(caplog):
    """Non-linear emulator + n_runs > 1: log warning emitted, no raise."""
    gmt_2d = np.ones((5, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.08):
        with caplog.at_level(
            logging.WARNING, logger="message_ix_models.tools.impacts.rime"
        ):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

    assert any("non-linear" in r.message for r in caplog.records)


def test_predict_nonlinear_single_run_raises():
    """Non-linear emulator + n_runs == 1: ValueError."""
    gmt_2d = np.ones((1, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.08):
        with pytest.raises(ValueError, match="non-linear emulator"):
            predict_rime(gmt_2d, "dummy.nc", "qtot_mean")


def test_predict_linear_single_run_no_raise():
    """Linear emulator + n_runs == 1: no error."""
    gmt_2d = np.ones((1, 4)) * 1.5
    with _mock_predict_from(), _mock_linearity(0.005):
        result = predict_rime(gmt_2d, "dummy.nc", "qtot_mean")
    assert result.shape == (3, 4)


# Integration tests requiring RIME NetCDF files
_RIME_DIR = impacts_data_path("rime")
_HAS_RIME_DATA = (
    _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
).exists()

skip_no_rime = pytest.mark.skipif(
    not _HAS_RIME_DATA, reason="RIME NetCDF datasets not available"
)


@skip_no_rime
def test_predict_qtot_mean_1d():
    path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    gmt = np.linspace(1.0, 2.5, 10)
    result = predict_rime(gmt, path, "qtot_mean")
    assert result.shape == (157, 10)


@skip_no_rime
def test_predict_qtot_mean_2d():
    path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    rng = np.random.default_rng(42)
    gmt_2d = rng.normal(1.5, 0.2, size=(5, 10))
    gmt_2d = np.clip(gmt_2d, 0.6, 7.4)
    result = predict_rime(gmt_2d, path, "qtot_mean")
    assert result.shape == (157, 10)


@skip_no_rime
def test_linearity_check_integration():
    path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    result = check_emulator_linearity(
        path, "qtot_mean", gmt_range=(1.0, 3.0), n_probe=5
    )
    assert "max_deviation" in result
    assert "is_linear" in result
    assert isinstance(result["is_linear"], bool)
