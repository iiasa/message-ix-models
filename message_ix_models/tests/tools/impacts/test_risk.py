"""Tests for tools.impacts.risk — CVaR ensemble statistics."""

import numpy as np
import pytest

from message_ix_models.tools.impacts.risk import (
    compute_cvar_single,
    cvar_coherent,
    cvar_pointwise,
)


class TestComputeCvarSingle:
    def test_basic(self):
        values = np.arange(1, 101, dtype=float)
        # Worst 10% of [1..100] = [1..10], mean = 5.5
        assert pytest.approx(compute_cvar_single(values, 10)) == 5.5

    def test_all_same(self):
        values = np.ones(50) * 3.0
        assert pytest.approx(compute_cvar_single(values, 50)) == 3.0

    def test_alpha_bounds(self):
        with pytest.raises(ValueError, match="alpha must be between"):
            compute_cvar_single(np.arange(10.0), 0)
        with pytest.raises(ValueError, match="alpha must be between"):
            compute_cvar_single(np.arange(10.0), 100)

    def test_single_element(self):
        assert pytest.approx(compute_cvar_single(np.array([5.0]), 50)) == 5.0


class TestCvarPointwise:
    @pytest.fixture
    def ensemble(self):
        return np.random.default_rng(42).normal(100, 20, size=(10, 5, 3))

    def test_shape(self, ensemble):
        result = cvar_pointwise(ensemble, 10)
        assert result.shape == (5, 3)

    def test_leq_expectation(self, ensemble):
        """Lower-tail CVaR <= expectation at every cell."""
        cvar_10 = cvar_pointwise(ensemble, 10)
        expectation = np.mean(ensemble, axis=0)
        assert np.all(cvar_10 <= expectation + 1e-10)

    def test_monotonicity(self, ensemble):
        """CVaR_10 <= CVaR_50 (more restrictive tail <= less restrictive)."""
        cvar_10 = cvar_pointwise(ensemble, 10)
        cvar_50 = cvar_pointwise(ensemble, 50)
        assert np.all(cvar_10 <= cvar_50 + 1e-10)

    def test_wrong_dimensions(self):
        with pytest.raises(ValueError, match="3D"):
            cvar_pointwise(np.zeros((10, 5)), 10)


class TestCvarCoherent:
    @pytest.fixture
    def ensemble(self):
        return np.random.default_rng(42).normal(100, 20, size=(10, 5, 3))

    def test_shape(self, ensemble):
        result = cvar_coherent(ensemble, 10)
        assert result.shape == (5, 3)

    def test_global_mean_leq_expectation(self, ensemble):
        """Coherent CVaR selects worst trajectories by global mean, so the
        *global* average of the result is <= the global expectation, even
        though individual cells can exceed it."""
        cvar_10 = cvar_coherent(ensemble, 10)
        expectation = np.mean(ensemble, axis=0)
        assert np.mean(cvar_10) <= np.mean(expectation) + 1e-10
