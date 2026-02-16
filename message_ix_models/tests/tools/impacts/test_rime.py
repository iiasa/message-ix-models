"""Tests for tools.impacts.rime -- RIME prediction engine.

Pure-function tests use synthetic data. Integration tests that need actual
RIME NetCDF datasets are marked with skipif.
"""

import numpy as np
import pytest

from message_ix_models.tools.impacts.rime import (
    _RIME_DATASETS_DIR,
    _clip_gmt,
)


class TestClipGmt:
    def test_no_clipping_needed(self):
        gmt = np.array([1.0, 1.5, 2.0, 3.0])
        result = _clip_gmt(gmt, "annual")
        np.testing.assert_array_equal(result, gmt)

    def test_annual_clipping(self):
        gmt = np.array([0.3, 0.5, 0.6, 1.0])
        result = _clip_gmt(gmt, "annual")
        # Values below 0.6 clipped to [0.6, 0.9]
        assert result[0] >= 0.6
        assert result[0] <= 0.9
        assert result[1] >= 0.6
        assert result[1] <= 0.9
        # Value at 0.6 should be unchanged
        assert result[2] == 0.6
        assert result[3] == 1.0

    def test_seasonal_higher_threshold(self):
        gmt = np.array([0.7, 0.8, 1.0])
        result = _clip_gmt(gmt, "seasonal2step")
        # 0.7 is below seasonal min of 0.8, clipped to [0.8, 1.2]
        assert result[0] >= 0.8
        assert result[0] <= 1.2
        # 0.8 is at boundary, unchanged
        assert result[1] == 0.8

    def test_2d_input(self):
        gmt = np.array([[0.3, 1.0, 2.0], [0.5, 1.5, 3.0]])
        result = _clip_gmt(gmt, "annual")
        assert result.shape == (2, 3)
        assert result[0, 0] >= 0.6
        assert result[0, 1] == 1.0

    def test_reproducibility(self):
        gmt = np.array([0.3, 0.4, 0.5])
        r1 = _clip_gmt(gmt, "annual", seed=42)
        r2 = _clip_gmt(gmt, "annual", seed=42)
        np.testing.assert_array_equal(r1, r2)


# Integration tests requiring RIME NetCDF files
_HAS_RIME_DATA = (
    _RIME_DATASETS_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
).exists()


@pytest.mark.skipif(not _HAS_RIME_DATA, reason="RIME NetCDF datasets not available")
class TestPredictRimeIntegration:
    """predict_rime returns native emulator resolution (157 basins)."""

    def test_qtot_mean_1d(self):
        from message_ix_models.tools.impacts.rime import predict_rime

        gmt = np.linspace(1.0, 2.5, 10)
        result = predict_rime(gmt, "qtot_mean")
        assert result.shape == (157, 10)

    def test_qtot_mean_2d(self):
        from message_ix_models.tools.impacts.rime import predict_rime

        rng = np.random.default_rng(42)
        gmt_2d = rng.normal(1.5, 0.2, size=(5, 10))
        gmt_2d = np.clip(gmt_2d, 0.6, 7.4)
        result = predict_rime(gmt_2d, "qtot_mean")
        assert result.shape == (157, 10)

    def test_linearity_check(self):
        from message_ix_models.tools.impacts.rime import check_emulator_linearity

        result = check_emulator_linearity("qtot_mean", gmt_range=(1.0, 3.0), n_probe=5)
        assert "max_deviation" in result
        assert "is_linear" in result
        assert isinstance(result["is_linear"], bool)
