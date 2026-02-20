"""Tests for tools.impacts.rime -- RIME prediction engine.

Pure-function tests use synthetic data. Integration tests that need actual
RIME NetCDF datasets are marked with skipif.
"""

from unittest.mock import patch

import numpy as np
import pytest

from message_ix_models.tools.impacts import clip_gmt, impacts_data_path


class TestClipGmt:
    def test_no_clipping_needed(self):
        gmt = np.array([1.0, 1.5, 2.0, 3.0])
        result = clip_gmt(gmt)
        np.testing.assert_array_equal(result, gmt)

    def test_annual_clipping(self):
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

    def test_seasonal_higher_threshold(self):
        gmt = np.array([0.7, 0.8, 1.0])
        result = clip_gmt(gmt, gmt_min=0.8, gmt_ceil=1.2)
        # 0.7 is below seasonal min of 0.8, clipped to [0.8, 1.2]
        assert result[0] >= 0.8
        assert result[0] <= 1.2
        # 0.8 is at boundary, unchanged
        assert result[1] == 0.8

    def test_2d_input(self):
        gmt = np.array([[0.3, 1.0, 2.0], [0.5, 1.5, 3.0]])
        result = clip_gmt(gmt)
        assert result.shape == (2, 3)
        assert result[0, 0] >= 0.6
        assert result[0, 1] == 1.0

    def test_reproducibility(self):
        gmt = np.array([0.3, 0.4, 0.5])
        r1 = clip_gmt(gmt, seed=42)
        r2 = clip_gmt(gmt, seed=42)
        np.testing.assert_array_equal(r1, r2)


class TestPredictRimeLinearityGuard:
    """2D predict_rime warns or raises based on emulator linearity."""

    # Fake per-run prediction: shape (n_spatial=3,) returned for each GMT scalar.
    _FAKE_SPATIAL = np.ones(3)

    def _mock_predict_from(self):
        return patch(
            "message_ix_models.tools.impacts.rime._predict_from_gmt",
            return_value=self._FAKE_SPATIAL,
        )

    def _mock_linearity(self, max_deviation: float):
        return patch(
            "message_ix_models.tools.impacts.rime.check_emulator_linearity",
            return_value={
                "max_deviation": max_deviation,
                "mean_deviation": max_deviation,
                "is_linear": max_deviation < 0.05,
            },
        )

    def test_linear_emulator_no_warning(self, caplog):
        """Linear emulator (deviation <= 1%): no linearity warning emitted."""
        import logging

        from message_ix_models.tools.impacts.rime import predict_rime

        gmt_2d = np.ones((5, 4)) * 1.5
        with self._mock_predict_from(), self._mock_linearity(0.005):
            with caplog.at_level(
                logging.WARNING, logger="message_ix_models.tools.impacts.rime"
            ):
                predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

        assert not any("non-linear" in r.message for r in caplog.records)

    def test_nonlinear_multi_run_warns(self, caplog):
        """Non-linear emulator + n_runs > 1: log warning emitted, no raise."""
        import logging

        from message_ix_models.tools.impacts.rime import predict_rime

        gmt_2d = np.ones((5, 4)) * 1.5
        with self._mock_predict_from(), self._mock_linearity(0.08):
            with caplog.at_level(
                logging.WARNING, logger="message_ix_models.tools.impacts.rime"
            ):
                predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

        assert any("non-linear" in r.message for r in caplog.records)

    def test_nonlinear_single_run_raises(self):
        """Non-linear emulator + n_runs == 1: ValueError."""
        from message_ix_models.tools.impacts.rime import predict_rime

        gmt_2d = np.ones((1, 4)) * 1.5
        with self._mock_predict_from(), self._mock_linearity(0.08):
            with pytest.raises(ValueError, match="non-linear emulator"):
                predict_rime(gmt_2d, "dummy.nc", "qtot_mean")

    def test_linear_single_run_no_raise(self):
        """Linear emulator + n_runs == 1: no error."""
        from message_ix_models.tools.impacts.rime import predict_rime

        gmt_2d = np.ones((1, 4)) * 1.5
        with self._mock_predict_from(), self._mock_linearity(0.005):
            result = predict_rime(gmt_2d, "dummy.nc", "qtot_mean")
        assert result.shape == (3, 4)


# Integration tests requiring RIME NetCDF files
_RIME_DIR = impacts_data_path("rime")
_HAS_RIME_DATA = (
    _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
).exists()


@pytest.mark.skipif(not _HAS_RIME_DATA, reason="RIME NetCDF datasets not available")
class TestPredictRimeIntegration:
    """predict_rime returns native emulator resolution (157 basins)."""

    def test_qtot_mean_1d(self):
        from message_ix_models.tools.impacts import predict_rime

        path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
        gmt = np.linspace(1.0, 2.5, 10)
        result = predict_rime(gmt, path, "qtot_mean")
        assert result.shape == (157, 10)

    def test_qtot_mean_2d(self):
        from message_ix_models.tools.impacts import predict_rime

        path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
        rng = np.random.default_rng(42)
        gmt_2d = rng.normal(1.5, 0.2, size=(5, 10))
        gmt_2d = np.clip(gmt_2d, 0.6, 7.4)
        result = predict_rime(gmt_2d, path, "qtot_mean")
        assert result.shape == (157, 10)

    def test_linearity_check(self):
        from message_ix_models.tools.impacts import check_emulator_linearity

        path = _RIME_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
        result = check_emulator_linearity(
            path, "qtot_mean", gmt_range=(1.0, 3.0), n_probe=5
        )
        assert "max_deviation" in result
        assert "is_linear" in result
        assert isinstance(result["is_linear"], bool)
