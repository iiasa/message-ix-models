"""Tests for tools.impacts.risk — CVaR ensemble statistics.

predict_rime is mocked at the risk module import site so tests exercise
the CVaR reduction logic without requiring RIME NetCDF data.
"""

from unittest.mock import patch

import numpy as np
import pytest

from message_ix_models.tools.impacts.risk import cvar_coherent, cvar_pointwise

# Canonical ensemble shape: (n_runs=10, n_spatial=5, n_years=3)
_RNG = np.random.default_rng(42)
_ENSEMBLE = _RNG.normal(100, 20, size=(10, 5, 3))

_GMT = np.linspace(1.0, 2.5, 3)
_PATH = "dummy.nc"
_VAR = "qtot_mean"


def _mock_predict_rime(ensemble):
    """Return a mock for predict_rime that ignores inputs and yields *ensemble*."""
    return patch(
        "message_ix_models.tools.impacts.risk.predict_rime",
        return_value=ensemble,
    )


class TestCvarPointwise:
    def test_shape(self):
        with _mock_predict_rime(_ENSEMBLE):
            result = cvar_pointwise(_GMT, _PATH, _VAR, alpha=10)
        assert result.shape == (5, 3)

    def test_alpha_bounds(self):
        with _mock_predict_rime(_ENSEMBLE):
            with pytest.raises(ValueError, match="alpha must be between"):
                cvar_pointwise(_GMT, _PATH, _VAR, alpha=0)
            with pytest.raises(ValueError, match="alpha must be between"):
                cvar_pointwise(_GMT, _PATH, _VAR, alpha=100)

    def test_leq_expectation(self):
        """Lower-tail CVaR <= pointwise expectation at every cell."""
        with _mock_predict_rime(_ENSEMBLE):
            cvar_10 = cvar_pointwise(_GMT, _PATH, _VAR, alpha=10)
        expectation = np.mean(_ENSEMBLE, axis=0)
        assert np.all(cvar_10 <= expectation + 1e-10)

    def test_monotonicity(self):
        """CVaR_10 <= CVaR_50 at every cell (tighter tail is worse)."""
        with _mock_predict_rime(_ENSEMBLE):
            cvar_10 = cvar_pointwise(_GMT, _PATH, _VAR, alpha=10)
            cvar_50 = cvar_pointwise(_GMT, _PATH, _VAR, alpha=50)
        assert np.all(cvar_10 <= cvar_50 + 1e-10)

    def test_all_same(self):
        """Uniform ensemble: CVaR equals the constant value."""
        uniform = np.full((10, 5, 3), 7.0)
        with _mock_predict_rime(uniform):
            result = cvar_pointwise(_GMT, _PATH, _VAR, alpha=25)
        assert pytest.approx(result) == np.full((5, 3), 7.0)

    def test_known_values(self):
        """1-spatial, 1-year ensemble: worst 10% of 10 runs = bottom 1."""
        # runs sorted ascending: [10, 20, ..., 100]; worst 10% = [10], mean = 10
        values = np.arange(10, 101, 10, dtype=float).reshape(10, 1, 1)
        with _mock_predict_rime(values):
            result = cvar_pointwise(_GMT, _PATH, _VAR, alpha=10)
        assert pytest.approx(result[0, 0]) == 10.0


class TestCvarCoherent:
    def test_shape(self):
        with _mock_predict_rime(_ENSEMBLE):
            result = cvar_coherent(_GMT, _PATH, _VAR, alpha=10)
        assert result.shape == (5, 3)

    def test_alpha_bounds(self):
        with _mock_predict_rime(_ENSEMBLE):
            with pytest.raises(ValueError, match="alpha must be between"):
                cvar_coherent(_GMT, _PATH, _VAR, alpha=0)
            with pytest.raises(ValueError, match="alpha must be between"):
                cvar_coherent(_GMT, _PATH, _VAR, alpha=100)

    def test_global_mean_leq_expectation(self):
        """Coherent CVaR selects worst trajectories globally, so the
        global mean of the result is <= the global expectation."""
        with _mock_predict_rime(_ENSEMBLE):
            cvar_10 = cvar_coherent(_GMT, _PATH, _VAR, alpha=10)
        expectation = np.mean(_ENSEMBLE, axis=0)
        assert np.mean(cvar_10) <= np.mean(expectation) + 1e-10

    def test_all_same(self):
        """Uniform ensemble: CVaR equals the constant value."""
        uniform = np.full((10, 5, 3), 7.0)
        with _mock_predict_rime(uniform):
            result = cvar_coherent(_GMT, _PATH, _VAR, alpha=25)
        assert pytest.approx(result) == np.full((5, 3), 7.0)

    def test_selects_worst_trajectories(self):
        """Worst run (all values 0) must appear in coherent CVaR at alpha=10."""
        # Run 0: all zeros (worst globally). Runs 1-9: all ones.
        ensemble = np.ones((10, 5, 3))
        ensemble[0] = 0.0
        with _mock_predict_rime(ensemble):
            result = cvar_coherent(_GMT, _PATH, _VAR, alpha=10)
        # alpha=10 of 10 runs -> cutoff=1 -> selects run 0 -> result = 0
        assert pytest.approx(result) == np.zeros((5, 3))
