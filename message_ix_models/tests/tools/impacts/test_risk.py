"""Tests for tools.impacts.risk — CVaR ensemble statistics."""

import numpy as np
import pytest

from message_ix_models.tools.impacts.risk import (
    compute_cvar,
    compute_cvar_single,
    validate_cvar_monotonicity,
)


class TestComputeCvarSingle:
    def test_basic(self):
        values = np.arange(1, 101, dtype=float)
        cvar_10 = compute_cvar_single(values, 10)
        # Worst 10% of [1..100] = [1..10], mean = 5.5
        assert pytest.approx(cvar_10) == 5.5

    def test_all_same(self):
        values = np.ones(50) * 3.0
        assert pytest.approx(compute_cvar_single(values, 50)) == 3.0

    def test_alpha_bounds(self):
        values = np.arange(10, dtype=float)
        with pytest.raises(ValueError, match="alpha must be between"):
            compute_cvar_single(values, 0)
        with pytest.raises(ValueError, match="alpha must be between"):
            compute_cvar_single(values, 100)

    def test_single_element(self):
        values = np.array([5.0])
        # alpha=50 on 1 element: ceil(1*0.5) = 1, so mean of [5.0]
        assert pytest.approx(compute_cvar_single(values, 50)) == 5.0


class TestComputeCvar:
    @pytest.fixture
    def ensemble_data(self):
        """Synthetic (10 runs, 5 basins, 3 years) with known structure."""
        rng = np.random.default_rng(42)
        return rng.normal(loc=100, scale=20, size=(10, 5, 3))

    def test_pointwise_shape(self, ensemble_data):
        result = compute_cvar(ensemble_data, [10, 50], method="pointwise")
        assert "expectation" in result
        assert "cvar_10" in result
        assert "cvar_50" in result
        assert result["expectation"].shape == (5, 3)

    def test_coherent_shape(self, ensemble_data):
        result = compute_cvar(ensemble_data, [10, 50], method="coherent")
        assert result["expectation"].shape == (5, 3)

    def test_unknown_method(self, ensemble_data):
        with pytest.raises(ValueError, match="Unknown method"):
            compute_cvar(ensemble_data, [10], method="bogus")

    def test_wrong_dimensions(self):
        with pytest.raises(ValueError, match="must be 3D"):
            compute_cvar(np.zeros((10, 5)), [10])

    def test_custom_labels(self, ensemble_data):
        result = compute_cvar(
            ensemble_data,
            [10],
            basin_ids=["B1", "B2", "B3", "B4", "B5"],
            year_columns=[2020, 2030, 2040],
        )
        assert list(result["expectation"].index) == ["B1", "B2", "B3", "B4", "B5"]
        assert list(result["expectation"].columns) == [2020, 2030, 2040]

    def test_cvar_leq_expectation(self, ensemble_data):
        """CVaR (lower tail) should be <= expectation for all cells."""
        result = compute_cvar(ensemble_data, [10, 50], method="pointwise")
        exp = result["expectation"].values
        cvar_10 = result["cvar_10"].values
        cvar_50 = result["cvar_50"].values
        assert np.all(cvar_10 <= exp + 1e-10)
        assert np.all(cvar_50 <= exp + 1e-10)


class TestMonotonicity:
    def test_valid_results(self):
        rng = np.random.default_rng(42)
        data = rng.normal(100, 20, size=(100, 5, 3))
        result = compute_cvar(data, [10, 50, 90], method="pointwise")
        validation = validate_cvar_monotonicity(result, [10, 50, 90])
        assert validation["is_valid"]
        assert validation["total_violations"] == 0

    def test_detects_violations(self):
        """Construct deliberately invalid CVaR results."""
        import pandas as pd

        # cvar_10 > cvar_50: violation
        results = {
            "expectation": pd.DataFrame([[100.0]]),
            "cvar_10": pd.DataFrame([[80.0]]),
            "cvar_50": pd.DataFrame([[70.0]]),  # should be >= cvar_10
        }
        validation = validate_cvar_monotonicity(results, [10, 50])
        assert not validation["is_valid"]
        assert validation["total_violations"] > 0
