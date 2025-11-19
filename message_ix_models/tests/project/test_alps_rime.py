"""Tests for ALPS RIME uncertainty propagation."""

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from rimeX.stats import fit_dist

from message_ix_models.project.alps.rime import (
    expand_predictions_with_emulator_uncertainty,
    batch_rime_predictions_with_percentiles,
    compute_expectation,
    compute_rime_cvar,
)
from message_ix_models.util import package_data_path


class TestLognormalFitting:
    """Test rimeX.stats.fit_dist integration for RIME percentiles."""

    def test_right_skewed_distribution(self):
        """Test lognormal fit for right-skewed distribution."""
        # Right-skewed: (p90-p50) > (p50-p10)
        p10, p50, p90 = 100, 200, 500

        dist = fit_dist([p50, p10, p90], quantiles=[0.5, 0.05, 0.95], dist_name='lognorm')

        # Sample at test quantiles
        quantiles = np.array([0.05, 0.25, 0.50, 0.75, 0.95])
        samples = dist.ppf(quantiles)

        # Assertions
        assert np.all(samples >= 0), "All samples must be non-negative"
        assert samples[0] == pytest.approx(p10, rel=1e-6), "p05 should match p10"
        assert samples[2] == pytest.approx(p50, rel=1e-6), "p50 should match input"
        assert samples[4] == pytest.approx(p90, rel=1e-6), "p95 should match p90"
        assert np.all(np.diff(samples) > 0), "Samples must be monotonically increasing"

    def test_left_skewed_distribution(self):
        """Test lognormal fit for left-skewed distribution (problematic case)."""
        # Left-skewed: (p90-p50) < (p50-p10)
        # From actual RIME data that was producing negatives
        p10, p50, p90 = 5970.11, 6883.14, 7154.34

        dist = fit_dist([p50, p10, p90], quantiles=[0.5, 0.05, 0.95], dist_name='lognorm')

        # Sample at stratification quantiles
        quantiles = np.array([0.05, 0.25, 0.50, 0.75, 0.95])
        samples = dist.ppf(quantiles)

        # Critical assertions for left-skewed case
        assert np.all(samples >= 0), "All samples must be non-negative (left-skewed case)"
        assert samples[0] >= p10 * 0.99, f"p05 sample {samples[0]} should be >= p10 {p10}"
        assert samples[2] == pytest.approx(p50, rel=1e-4), "p50 should match input"
        assert samples[4] <= p90 * 1.01, f"p95 sample {samples[4]} should be <= p90 {p90}"
        assert np.all(np.diff(samples) > 0), "Samples must be monotonically increasing"

        # Check samples stay within reasonable range (not 100× larger)
        assert np.all(samples < p90 * 2), "Samples should not extrapolate wildly"

    def test_symmetric_distribution(self):
        """Test normal fit for perfectly symmetric distribution."""
        p10, p50, p90 = 100, 150, 200

        # Symmetric distribution should use 'norm' not 'lognorm'
        dist = fit_dist([p50, p10, p90], quantiles=[0.5, 0.05, 0.95], dist_name='norm')

        quantiles = np.array([0.05, 0.5, 0.95])
        samples = dist.ppf(quantiles)

        assert samples[1] == pytest.approx(p50, rel=1e-6), "Median should match input"

    def test_mildly_left_skewed_lognorm(self):
        """Test lognormal fit for mildly left-skewed distribution."""
        # Mildly left-skewed but still within lognormal range
        p10, p50, p90 = 100, 180, 220

        dist = fit_dist([p50, p10, p90], quantiles=[0.5, 0.05, 0.95], dist_name='lognorm')

        quantiles = np.array([0.05, 0.5, 0.95])
        samples = dist.ppf(quantiles)

        assert np.all(samples >= 0), "All samples must be non-negative"
        assert samples[1] == pytest.approx(p50, rel=1e-4), "Median should match input"

    def test_small_values_near_zero(self):
        """Test lognormal fit handles small values correctly."""
        p10, p50, p90 = 0.01, 0.05, 0.20

        dist = fit_dist([p50, p10, p90], quantiles=[0.5, 0.05, 0.95], dist_name='lognorm')

        quantiles = np.array([0.05, 0.5, 0.95])
        samples = dist.ppf(quantiles)

        assert np.all(samples >= 0), "Small values must remain non-negative"
        assert samples[1] == pytest.approx(p50, rel=1e-4), "Median should match"


class TestEmulatorUncertaintyExpansion:
    """Test expand_predictions_with_emulator_uncertainty function."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        """Create test data for all tests in this class."""
        # Simple test case: 3 basins, 5 years, 2 runs
        years = [2020, 2025, 2030, 2035, 2040]
        basins = [0, 1, 2]

        # Create synthetic percentile predictions
        # Run 0: right-skewed distributions
        self.p10_0 = pd.DataFrame([[100, 110, 120, 130, 140]] * 3,
                                  index=basins, columns=years)
        self.p50_0 = pd.DataFrame([[200, 210, 220, 230, 240]] * 3,
                                  index=basins, columns=years)
        self.p90_0 = pd.DataFrame([[500, 510, 520, 530, 540]] * 3,
                                  index=basins, columns=years)

        # Run 1: left-skewed distributions (like actual RIME data)
        self.p10_1 = pd.DataFrame([[5970, 6000, 6030, 6060, 6090]] * 3,
                                  index=basins, columns=years)
        self.p50_1 = pd.DataFrame([[6883, 6900, 6920, 6940, 6960]] * 3,
                                  index=basins, columns=years)
        self.p90_1 = pd.DataFrame([[7154, 7170, 7190, 7210, 7230]] * 3,
                                  index=basins, columns=years)

    def test_expansion_shape(self):
        """Test that expansion produces correct output shape."""
        predictions_p10 = {0: self.p10_0, 1: self.p10_1}
        predictions_p50 = {0: self.p50_0, 1: self.p50_1}
        predictions_p90 = {0: self.p90_0, 1: self.p90_1}

        run_ids = [0, 1]
        weights = np.array([0.5, 0.5])
        n_samples = 3  # K=3 for speed

        expanded_preds, expanded_ids, expanded_weights = expand_predictions_with_emulator_uncertainty(
            predictions_p10, predictions_p50, predictions_p90,
            run_ids, weights, n_samples
        )

        # Check shapes
        assert len(expanded_preds) == 2 * 3, "Should have N×K=6 pseudo-runs"
        assert len(expanded_ids) == 6, "Should have 6 pseudo-run IDs"
        assert len(expanded_weights) == 6, "Should have 6 weights"

        # Check weights sum to 1
        assert expanded_weights.sum() == pytest.approx(1.0), "Weights should sum to 1"

        # Check each prediction has correct shape
        for pred_df in expanded_preds.values():
            assert pred_df.shape == (3, 5), "Each prediction should have (3 basins, 5 years)"

    def test_expansion_non_negativity(self):
        """Test that all expanded predictions are non-negative."""
        predictions_p10 = {0: self.p10_0, 1: self.p10_1}
        predictions_p50 = {0: self.p50_0, 1: self.p50_1}
        predictions_p90 = {0: self.p90_0, 1: self.p90_1}

        run_ids = [0, 1]
        weights = np.array([0.5, 0.5])
        n_samples = 5

        expanded_preds, _, _ = expand_predictions_with_emulator_uncertainty(
            predictions_p10, predictions_p50, predictions_p90,
            run_ids, weights, n_samples
        )

        # Critical assertion: no negative values
        for pseudo_id, pred_df in expanded_preds.items():
            assert np.all(pred_df.values >= 0), \
                f"Pseudo-run {pseudo_id} contains negative values: min={pred_df.min().min()}"

    def test_expansion_preserves_median_approximately(self):
        """Test that median of samples approximates input p50."""
        predictions_p10 = {0: self.p10_0}
        predictions_p50 = {0: self.p50_0}
        predictions_p90 = {0: self.p90_0}

        run_ids = [0]
        weights = np.array([1.0])
        n_samples = 1000  # Use many samples for better approximation

        expanded_preds, _, _ = expand_predictions_with_emulator_uncertainty(
            predictions_p10, predictions_p50, predictions_p90,
            run_ids, weights, n_samples, seed=42
        )

        # Median across all samples should approximate p50 (not mean, which is biased for skewed distributions)
        all_values = np.array([expanded_preds[i].values for i in range(n_samples)])
        median_values = np.median(all_values, axis=0)

        # Should be close to input p50 (within 5% relative tolerance)
        np.testing.assert_allclose(
            median_values, self.p50_0.values,
            rtol=0.05,
            err_msg="Median of samples should approximate input p50"
        )

    def test_expansion_left_skewed_no_wild_extrapolation(self):
        """Test that left-skewed distributions don't extrapolate wildly."""
        predictions_p10 = {0: self.p10_1}
        predictions_p50 = {0: self.p50_1}
        predictions_p90 = {0: self.p90_1}

        run_ids = [0]
        weights = np.array([1.0])
        n_samples = 1000  # Many samples to check extremes

        expanded_preds, _, _ = expand_predictions_with_emulator_uncertainty(
            predictions_p10, predictions_p50, predictions_p90,
            run_ids, weights, n_samples, seed=42
        )

        # Collect all sample values
        all_samples = np.array([expanded_preds[i].values for i in range(n_samples)])

        # Check that samples stay within reasonable bounds
        # For left-skewed distributions (p90-p50 < p50-p10), samples should not
        # extrapolate wildly beyond the input p10-p90 range

        # Critical: no wild extrapolation (e.g., 100× larger than input range)
        input_range = self.p90_1.values.max() - self.p10_1.values.min()
        max_sample = all_samples.max()
        min_sample = all_samples.min()

        assert min_sample >= 0, "All samples must be non-negative"
        assert max_sample < self.p90_1.values.max() + 10 * input_range, \
            f"Samples should not extrapolate more than 10× the input range, got max={max_sample}"

        # Most samples should be within the p10-p90 range (at least 80%)
        within_range = np.sum((all_samples >= self.p10_1.values.min() * 0.8) &
                              (all_samples <= self.p90_1.values.max() * 1.2))
        total_samples = all_samples.size
        assert within_range / total_samples > 0.8, \
            f"At least 80% of samples should be within extended p10-p90 range, got {within_range/total_samples:.1%}"


class TestRIMEExpectationsAndCVaR:
    """Test expectations and CVaR computation across different RIME modes."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        """Create synthetic predictions for testing."""
        from message_ix_models.project.alps.rime import (
            compute_expectation,
            compute_rime_cvar,
            expand_predictions_with_emulator_uncertainty,
        )

        # Store functions as attributes for easy access
        self.compute_expectation = compute_expectation
        self.compute_rime_cvar = compute_rime_cvar
        self.expand = expand_predictions_with_emulator_uncertainty

        # Create synthetic predictions: 5 runs, 10 basins, 8 years
        years = [2020, 2025, 2030, 2035, 2040, 2045, 2050, 2055]
        basins = list(range(10))
        self.n_runs = 5
        self.n_basins = len(basins)
        self.n_years = len(years)
        self.years = years
        self.basins = basins

        # Generate predictions with varying means (1000, 1100, 1200, 1300, 1400)
        self.predictions_p50 = {}
        self.predictions_p10 = {}
        self.predictions_p90 = {}

        for i in range(self.n_runs):
            base_value = 1000 + i * 100
            # p50: increasing trend over years
            values_p50 = np.array([[base_value + 10*y for y in range(self.n_years)]] * self.n_basins)
            self.predictions_p50[i] = pd.DataFrame(values_p50, index=basins, columns=years)

            # p10: 20% below p50 (right-skewed)
            self.predictions_p10[i] = self.predictions_p50[i] * 0.8

            # p90: 50% above p50 (right-skewed)
            self.predictions_p90[i] = self.predictions_p50[i] * 1.5

        # Importance weights (non-uniform)
        self.importance_weights = np.array([0.4, 0.3, 0.2, 0.05, 0.05])
        self.uniform_weights = np.ones(self.n_runs) / self.n_runs
        self.run_ids = np.arange(self.n_runs)


class TestMode1_WeightedPlusEmulatorUncertainty(TestRIMEExpectationsAndCVaR):
    """Test Mode 1: Importance weighting + emulator uncertainty."""

    def test_produces_expectations_and_cvar(self):
        """Test that mode produces both expectations and CVaR outputs."""
        # Expand predictions with emulator uncertainty
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=5,
            seed=42
        )

        # Should have N×K pseudo-runs
        assert len(expanded_preds) == self.n_runs * 5
        assert len(expanded_ids) == self.n_runs * 5
        assert len(expanded_weights) == self.n_runs * 5

        # Compute expectations with importance weights
        expectation = self.compute_expectation(expanded_preds, expanded_ids, expanded_weights)

        # Compute CVaR with importance weights
        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        # Check output structure
        assert expectation.shape == (self.n_basins, self.n_years)
        assert 'expectation' in cvar_results
        assert 'cvar_10' in cvar_results
        assert 'cvar_50' in cvar_results
        assert 'cvar_90' in cvar_results

        # Check all outputs have correct shape
        for key, result_df in cvar_results.items():
            assert result_df.shape == (self.n_basins, self.n_years)

    def test_importance_weights_affect_expectation(self):
        """Test that importance weights shift expectations compared to uniform."""
        # Expand with importance weights
        expanded_preds, expanded_ids, importance_expanded = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=3,
            seed=42
        )

        # Expectation with importance weights
        weighted_exp = self.compute_expectation(expanded_preds, expanded_ids, importance_expanded)

        # Expectation with uniform weights
        uniform_expanded = np.ones(len(expanded_ids)) / len(expanded_ids)
        uniform_exp = self.compute_expectation(expanded_preds, expanded_ids, uniform_expanded)

        # Should differ because importance_weights are non-uniform (0.4, 0.3, 0.2, 0.05, 0.05)
        # Weighted should be closer to runs 0,1,2 (higher weights)
        assert not np.allclose(weighted_exp.values, uniform_exp.values, rtol=0.01), \
            "Importance-weighted expectation should differ from uniform"

    def test_cvar_ordering(self):
        """Test CVaR_10 ≤ CVaR_50 ≤ CVaR_90."""
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=5,
            seed=42
        )

        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        cvar10 = cvar_results['cvar_10'].values
        cvar50 = cvar_results['cvar_50'].values
        cvar90 = cvar_results['cvar_90'].values

        # CVaR_10 should be ≤ CVaR_50 ≤ CVaR_90 (since lower values are worse for water)
        assert np.all(cvar10 <= cvar50), "CVaR_10 should be ≤ CVaR_50"
        assert np.all(cvar50 <= cvar90), "CVaR_50 should be ≤ CVaR_90"

    def test_non_negativity(self):
        """Test all outputs are non-negative."""
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=5,
            seed=42
        )

        expectation = self.compute_expectation(expanded_preds, expanded_ids, expanded_weights)
        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        assert np.all(expectation.values >= 0), "Expectation must be non-negative"
        for key, result_df in cvar_results.items():
            assert np.all(result_df.values >= 0), f"{key} must be non-negative"


class TestMode2_WeightedOnly(TestRIMEExpectationsAndCVaR):
    """Test Mode 2: Importance weighting without emulator uncertainty."""

    def test_produces_expectations_and_cvar(self):
        """Test that mode produces both expectations and CVaR outputs."""
        # Use p50 predictions directly (no emulator uncertainty expansion)
        expectation = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.importance_weights
        )

        cvar_results = self.compute_rime_cvar(
            self.predictions_p50,
            self.importance_weights,
            self.run_ids,
            [10, 50, 90]
        )

        # Check output structure
        assert expectation.shape == (self.n_basins, self.n_years)
        assert 'expectation' in cvar_results
        assert 'cvar_10' in cvar_results
        assert 'cvar_50' in cvar_results
        assert 'cvar_90' in cvar_results

        for key, result_df in cvar_results.items():
            assert result_df.shape == (self.n_basins, self.n_years)

    def test_importance_weights_affect_expectation(self):
        """Test that importance weights shift expectations compared to uniform."""
        # Weighted expectation
        weighted_exp = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.importance_weights
        )

        # Uniform expectation
        uniform_exp = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.uniform_weights
        )

        # predictions_p50 has runs with base values [1000, 1100, 1200, 1300, 1400]
        # importance_weights = [0.4, 0.3, 0.2, 0.05, 0.05] → weighted toward lower runs
        # uniform_weights = [0.2, 0.2, 0.2, 0.2, 0.2] → balanced
        # weighted_exp should be < uniform_exp
        assert np.mean(weighted_exp.values) < np.mean(uniform_exp.values), \
            "Importance-weighted expectation should be lower (more weight on lower runs)"

    def test_cvar_ordering(self):
        """Test CVaR_10 ≤ CVaR_50 ≤ CVaR_90."""
        cvar_results = self.compute_rime_cvar(
            self.predictions_p50,
            self.importance_weights,
            self.run_ids,
            [10, 50, 90]
        )

        cvar10 = cvar_results['cvar_10'].values
        cvar50 = cvar_results['cvar_50'].values
        cvar90 = cvar_results['cvar_90'].values

        assert np.all(cvar10 <= cvar50), "CVaR_10 should be ≤ CVaR_50"
        assert np.all(cvar50 <= cvar90), "CVaR_50 should be ≤ CVaR_90"

    def test_non_negativity(self):
        """Test all outputs are non-negative."""
        expectation = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.importance_weights
        )
        cvar_results = self.compute_rime_cvar(
            self.predictions_p50,
            self.importance_weights,
            self.run_ids,
            [10, 50, 90]
        )

        assert np.all(expectation.values >= 0), "Expectation must be non-negative"
        for key, result_df in cvar_results.items():
            assert np.all(result_df.values >= 0), f"{key} must be non-negative"

    def test_cvar50_equals_weighted_median(self):
        """Test that CVaR_50 approximates the weighted median."""
        cvar_results = self.compute_rime_cvar(
            self.predictions_p50,
            self.importance_weights,
            self.run_ids,
            [50]
        )

        # CVaR_50 should be close to weighted median
        # With 5 runs and weights [0.4, 0.3, 0.2, 0.05, 0.05], median should be around run 1
        # (cumulative: 0.4, 0.7, 0.9, 0.95, 1.0 → 50th percentile falls in run 1)
        cvar50 = cvar_results['cvar_50']

        # Run 1 has base value 1100, check CVaR_50 is close to this range
        assert 1000 < np.mean(cvar50.values) < 1300, \
            "CVaR_50 should be in range of runs 0-2 (weighted median)"


class TestMode3_EmulatorUncertaintyOnly(TestRIMEExpectationsAndCVaR):
    """Test Mode 3: Emulator uncertainty with uniform weights."""

    def test_produces_expectations_and_cvar(self):
        """Test that mode produces both expectations and CVaR outputs."""
        # Expand with uniform weights
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=5,
            seed=42
        )

        assert len(expanded_preds) == self.n_runs * 5
        assert np.allclose(expanded_weights, 1.0 / (self.n_runs * 5)), \
            "Weights should be uniform"

        expectation = self.compute_expectation(expanded_preds, expanded_ids, expanded_weights)
        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        assert expectation.shape == (self.n_basins, self.n_years)
        assert 'expectation' in cvar_results
        assert 'cvar_10' in cvar_results
        assert 'cvar_50' in cvar_results
        assert 'cvar_90' in cvar_results

        for key, result_df in cvar_results.items():
            assert result_df.shape == (self.n_basins, self.n_years)

    def test_uniform_weights_sum_to_one(self):
        """Test that expanded uniform weights sum to 1.0."""
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=5,
            seed=42
        )

        assert expanded_weights.sum() == pytest.approx(1.0), "Weights must sum to 1.0"

    def test_emulator_uncertainty_increases_spread(self):
        """Test that emulator uncertainty increases spread vs p50 only."""
        # Expand with emulator uncertainty
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=5,
            seed=42
        )

        # CVaR with emulator uncertainty
        cvar_with_unc = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 90])

        # CVaR without emulator uncertainty (p50 only)
        cvar_without_unc = self.compute_rime_cvar(
            self.predictions_p50,
            self.uniform_weights,
            self.run_ids,
            [10, 90]
        )

        # Spread with emulator uncertainty
        spread_with = (cvar_with_unc['cvar_90'].values - cvar_with_unc['cvar_10'].values).mean()

        # Spread without emulator uncertainty
        spread_without = (cvar_without_unc['cvar_90'].values - cvar_without_unc['cvar_10'].values).mean()

        # Emulator uncertainty should increase spread
        assert spread_with > spread_without, \
            "Emulator uncertainty should increase CVaR spread"

    def test_cvar_ordering(self):
        """Test CVaR_10 ≤ CVaR_50 ≤ CVaR_90."""
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=5,
            seed=42
        )

        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        cvar10 = cvar_results['cvar_10'].values
        cvar50 = cvar_results['cvar_50'].values
        cvar90 = cvar_results['cvar_90'].values

        assert np.all(cvar10 <= cvar50), "CVaR_10 should be ≤ CVaR_50"
        assert np.all(cvar50 <= cvar90), "CVaR_50 should be ≤ CVaR_90"

    def test_non_negativity(self):
        """Test all outputs are non-negative."""
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=5,
            seed=42
        )

        expectation = self.compute_expectation(expanded_preds, expanded_ids, expanded_weights)
        cvar_results = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        assert np.all(expectation.values >= 0), "Expectation must be non-negative"
        for key, result_df in cvar_results.items():
            assert np.all(result_df.values >= 0), f"{key} must be non-negative"


class TestCrossMode_Comparisons:
    """Test comparisons across different RIME modes."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        """Create shared test data."""
        from message_ix_models.project.alps.rime import (
            compute_expectation,
            compute_rime_cvar,
            expand_predictions_with_emulator_uncertainty,
        )

        self.compute_expectation = compute_expectation
        self.compute_rime_cvar = compute_rime_cvar
        self.expand = expand_predictions_with_emulator_uncertainty

        years = [2020, 2030, 2040, 2050]
        basins = list(range(5))
        n_runs = 4

        self.predictions_p50 = {}
        self.predictions_p10 = {}
        self.predictions_p90 = {}

        for i in range(n_runs):
            base = 1000 + i * 100
            values_p50 = np.array([[base] * len(years)] * len(basins))
            self.predictions_p50[i] = pd.DataFrame(values_p50, index=basins, columns=years)
            self.predictions_p10[i] = self.predictions_p50[i] * 0.8
            self.predictions_p90[i] = self.predictions_p50[i] * 1.5

        self.run_ids = np.arange(n_runs)
        self.uniform_weights = np.ones(n_runs) / n_runs
        self.importance_weights = np.array([0.5, 0.3, 0.15, 0.05])

    def test_mode1_vs_mode2_emulator_uncertainty_increases_spread(self):
        """Test that adding emulator uncertainty (mode 1) increases spread vs mode 2."""
        # Mode 1: Weighted + emulator uncertainty
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=5,
            seed=42
        )
        cvar_mode1 = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 90])

        # Mode 2: Weighted only
        cvar_mode2 = self.compute_rime_cvar(
            self.predictions_p50,
            self.importance_weights,
            self.run_ids,
            [10, 90]
        )

        spread_mode1 = (cvar_mode1['cvar_90'].values - cvar_mode1['cvar_10'].values).mean()
        spread_mode2 = (cvar_mode2['cvar_90'].values - cvar_mode2['cvar_10'].values).mean()

        assert spread_mode1 > spread_mode2, \
            "Mode 1 (with emulator uncertainty) should have larger spread than mode 2"

    def test_mode2_vs_mode3_importance_weights_shift_expectation(self):
        """Test that importance weights (mode 2) shift expectation vs uniform weights (mode 3 without expansion)."""
        # Mode 2: Weighted only
        exp_mode2 = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.importance_weights
        )

        # Mode 3 analog: Uniform weights, no expansion
        exp_mode3 = self.compute_expectation(
            self.predictions_p50,
            self.run_ids,
            self.uniform_weights
        )

        # importance_weights = [0.5, 0.3, 0.15, 0.05] → weighted toward run 0 (base=1000)
        # uniform_weights = [0.25, 0.25, 0.25, 0.25] → balanced (mean base=1150)
        assert np.mean(exp_mode2.values) < np.mean(exp_mode3.values), \
            "Mode 2 (importance weighted) should have lower expectation than mode 3 (uniform)"

    def test_all_modes_produce_consistent_shapes(self):
        """Test that all modes produce consistent output shapes."""
        # Mode 1
        expanded_preds, expanded_ids, expanded_weights = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.importance_weights,
            n_samples=3,
            seed=42
        )
        exp1 = self.compute_expectation(expanded_preds, expanded_ids, expanded_weights)
        cvar1 = self.compute_rime_cvar(expanded_preds, expanded_weights, expanded_ids, [10, 50, 90])

        # Mode 2
        exp2 = self.compute_expectation(self.predictions_p50, self.run_ids, self.importance_weights)
        cvar2 = self.compute_rime_cvar(self.predictions_p50, self.importance_weights, self.run_ids, [10, 50, 90])

        # Mode 3
        expanded_preds3, expanded_ids3, expanded_weights3 = self.expand(
            self.predictions_p10,
            self.predictions_p50,
            self.predictions_p90,
            self.run_ids,
            self.uniform_weights,
            n_samples=3,
            seed=42
        )
        exp3 = self.compute_expectation(expanded_preds3, expanded_ids3, expanded_weights3)
        cvar3 = self.compute_rime_cvar(expanded_preds3, expanded_weights3, expanded_ids3, [10, 50, 90])

        # All should have same shape
        assert exp1.shape == exp2.shape == exp3.shape
        assert set(cvar1.keys()) == set(cvar2.keys()) == set(cvar3.keys())

        for key in cvar1.keys():
            assert cvar1[key].shape == cvar2[key].shape == cvar3[key].shape


class TestSeasonalDataIntegration:
    """Test that seasonal datasets work with expectations and CVaR computation."""

    @pytest.mark.parametrize("variable", ["qtot_mean", "qr"])
    @pytest.mark.parametrize("suban", [False, True])
    def test_seasonal_data_loads_and_computes(self, variable, suban):
        """Test that both annual and seasonal data can be loaded and processed."""
        # This test requires actual RIME datasets, skip if not available
        RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")
        temporal_res = "seasonal2step" if suban else "annual"
        dataset_filename = f"rime_regionarray_{variable}_CWatM_{temporal_res}_window0.nc"
        dataset_path = RIME_DATASETS_DIR / dataset_filename

        if not dataset_path.exists():
            pytest.skip(f"RIME dataset not found: {dataset_path}")

        # Create mock MAGICC data in IAMC format (one row per run with year columns)
        years = [2020, 2030, 2040, 2050]
        n_runs = 3

        # Mock MAGICC DataFrame - IAMC format
        magicc_data = []
        for run_id in range(n_runs):
            row = {
                'Model': f'MAGICCv7.5.3|run_{run_id}',
                'Variable': 'AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3',
                'Scenario': 'SSP2-baseline',
            }
            # Add year columns
            for year in years:
                row[str(year)] = 1.0 + run_id * 0.1 + (year - 2020) * 0.01
            magicc_data.append(row)
        magicc_df = pd.DataFrame(magicc_data)

        # Extract percentile predictions
        run_ids = list(range(n_runs))
        basin_mapping = pd.DataFrame()  # Not used in current implementation

        try:
            predictions_p10, predictions_p50, predictions_p90 = batch_rime_predictions_with_percentiles(
                magicc_df,
                run_ids,
                dataset_path,
                basin_mapping,
                variable,
                suban
            )

            # Check output structure
            assert len(predictions_p10) == n_runs
            assert len(predictions_p50) == n_runs
            assert len(predictions_p90) == n_runs

            # Check DataFrame shapes
            expected_n_timesteps = len(years) * 2 if suban else len(years)
            for run_id in run_ids:
                p10_df = predictions_p10[run_id]
                p50_df = predictions_p50[run_id]
                p90_df = predictions_p90[run_id]

                assert p10_df.shape[1] == expected_n_timesteps, \
                    f"Expected {expected_n_timesteps} timesteps, got {p10_df.shape[1]}"
                assert p50_df.shape[1] == expected_n_timesteps
                assert p90_df.shape[1] == expected_n_timesteps

                # Check value ranges
                min_allowed = -100 if variable.startswith('qr') else 0
                for pred, name in [(p10_df.values, 'p10'), (p50_df.values, 'p50'), (p90_df.values, 'p90')]:
                    assert np.all(pred >= min_allowed), f"{name} values exceed allowed threshold: min={pred.min()}, allowed>={min_allowed}"

                # Check ordering
                assert np.all(p10_df.values <= p50_df.values), "p10 ≤ p50"
                assert np.all(p50_df.values <= p90_df.values), "p50 ≤ p90"

            # Test that expectations and CVaR can be computed
            weights = np.ones(n_runs) / n_runs
            run_ids_array = np.array(run_ids)

            expectation = compute_expectation(predictions_p50, run_ids_array, weights)
            cvar_results = compute_rime_cvar(predictions_p50, weights, run_ids_array, [10, 50, 90])

            assert expectation.shape[1] == expected_n_timesteps
            for key in ['expectation', 'cvar_10', 'cvar_50', 'cvar_90']:
                if key in cvar_results:
                    assert cvar_results[key].shape[1] == expected_n_timesteps

        except Exception as e:
            pytest.fail(f"Failed for variable={variable}, suban={suban}: {e}")

    @pytest.mark.parametrize("variable", ["qtot_mean", "qr"])
    @pytest.mark.parametrize("suban", [False, True])
    def test_seasonal_emulator_uncertainty_expansion(self, variable, suban):
        """Test emulator uncertainty expansion works with seasonal data."""
        RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")
        temporal_res = "seasonal2step" if suban else "annual"
        dataset_filename = f"rime_regionarray_{variable}_CWatM_{temporal_res}_window0.nc"
        dataset_path = RIME_DATASETS_DIR / dataset_filename

        if not dataset_path.exists():
            pytest.skip(f"RIME dataset not found: {dataset_path}")

        # Create minimal mock data in IAMC format
        years = [2020, 2030]
        n_runs = 2

        magicc_data = []
        for run_id in range(n_runs):
            row = {
                'Model': f'MAGICCv7.5.3|run_{run_id}',
                'Variable': 'AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3',
                'Scenario': 'SSP2-baseline',
            }
            for year in years:
                row[str(year)] = 1.0 + run_id * 0.1
            magicc_data.append(row)
        magicc_df = pd.DataFrame(magicc_data)

        run_ids = list(range(n_runs))
        basin_mapping = pd.DataFrame()

        predictions_p10, predictions_p50, predictions_p90 = batch_rime_predictions_with_percentiles(
            magicc_df,
            run_ids,
            dataset_path,
            basin_mapping,
            variable,
            suban
        )

        # Test expansion with emulator uncertainty
        weights = np.ones(n_runs) / n_runs
        n_samples = 3

        expanded_preds, expanded_ids, expanded_weights = expand_predictions_with_emulator_uncertainty(
            predictions_p10,
            predictions_p50,
            predictions_p90,
            run_ids,
            weights,
            n_samples,
            seed=42
        )

        # Check expansion worked
        assert len(expanded_preds) == n_runs * n_samples
        assert len(expanded_ids) == n_runs * n_samples
        assert len(expanded_weights) == n_runs * n_samples

        # Check all expanded predictions have correct shape
        expected_n_timesteps = len(years) * 2 if suban else len(years)
        for pred_df in expanded_preds.values():
            assert pred_df.shape[1] == expected_n_timesteps
            assert np.all(pred_df.values >= 0), "Expanded predictions must be non-negative"


class TestBasinExpansion:
    """Test basin expansion from 157 RIME basins to 217 MESSAGE rows."""

    def test_batch_rime_basin_expansion(self):
        """Test that batch_rime_predictions_with_percentiles correctly expands 157 RIME basins to 217 MESSAGE rows.

        Regression test for bug where predictions were misaligned by row index instead of BASIN_ID,
        causing Amazon (BASIN_ID=9 at MESSAGE row 76) to receive Basin 77's value (RIME index 76).

        Critical assertions:
        1. Output DataFrame has 217 rows (not 157)
        2. Amazon basin (BASIN_ID=9) appears at correct row with correct name
        3. Amazon value is reasonable (>1000 km³/yr, not <100 km³/yr)
        4. Row indices match BASIN_ID mapping from basin_mapping CSV
        5. All BASIN_IDs in output match basin_mapping exactly
        """
        # Load test MAGICC data
        test_data_path = Path(__file__).parent / "alps" / "data" / "magicc_ssp2_snippet.csv"

        if not test_data_path.exists():
            pytest.skip(f"Test data not found: {test_data_path}")

        magicc_df = pd.read_csv(test_data_path)

        # Load basin mapping (R12 only)
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy().reset_index(drop=True)

        # Get RIME dataset
        RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")
        dataset_path = RIME_DATASETS_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window0.nc"

        if not dataset_path.exists():
            pytest.skip(f"RIME dataset not found: {dataset_path}")

        # Run predictions with 2 runs for efficiency
        run_ids = [0, 1]
        predictions_p10, predictions_p50, predictions_p90 = batch_rime_predictions_with_percentiles(
            magicc_df, run_ids, dataset_path, basin_mapping, "qtot_mean", suban=False
        )

        # Test with first run's p50 predictions
        pred = predictions_p50[0]

        # ASSERTION 1: Output has 217 rows (not 157)
        assert len(pred) == 217, f"Expected 217 MESSAGE rows, got {len(pred)}"

        # ASSERTION 2: Verify basin metadata columns exist
        required_cols = ['BASIN_ID', 'NAME', 'BASIN', 'REGION', 'BCU_name', 'area_km2']
        for col in required_cols:
            assert col in pred.columns, f"Missing metadata column: {col}"

        # ASSERTION 3: Amazon basin (BASIN_ID=9) has reasonable value
        amazon_rows = pred[pred['BASIN_ID'] == 9]
        assert len(amazon_rows) == 1, f"Expected exactly 1 Amazon basin row, got {len(amazon_rows)}"

        amazon_row = amazon_rows.iloc[0]
        year_cols = [col for col in pred.columns if isinstance(col, (int, np.integer))]
        amazon_values = amazon_row[year_cols].values

        # Amazon should have runoff > 1000 km³/yr (it's a huge basin with ~5900 km³/yr)
        # If basin expansion bug exists, it would show <100 km³/yr from a different basin
        assert np.all(amazon_values > 1000), \
            f"Amazon runoff should be >1000 km³/yr, got {amazon_values.min():.1f} - {amazon_values.max():.1f}"

        # More specific check: Amazon should be in reasonable range 4000-7000 km³/yr
        assert np.all(amazon_values > 4000) and np.all(amazon_values < 7000), \
            f"Amazon runoff should be 4000-7000 km³/yr, got {amazon_values.min():.1f} - {amazon_values.max():.1f}"

        # ASSERTION 4: Verify Amazon basin name
        assert amazon_row['BASIN'] == 'Amazon', \
            f"BASIN_ID=9 should be 'Amazon', got '{amazon_row['BASIN']}'"
        assert amazon_row['REGION'] == 'LAM', \
            f"Amazon should be in LAM region, got '{amazon_row['REGION']}'"

        # ASSERTION 5: All BASIN_IDs match basin_mapping exactly
        pred_basin_ids = set(pred['BASIN_ID'].values)
        mapping_basin_ids = set(basin_mapping['BASIN_ID'].values)

        assert pred_basin_ids == mapping_basin_ids, \
            f"BASIN_IDs don't match. Missing: {mapping_basin_ids - pred_basin_ids}, Extra: {pred_basin_ids - mapping_basin_ids}"

        # ASSERTION 6: Verify row-by-row alignment with basin_mapping
        for idx in range(len(pred)):
            pred_row = pred.iloc[idx]
            mapping_row = basin_mapping.iloc[idx]

            # Check BASIN_ID matches
            assert pred_row['BASIN_ID'] == mapping_row['BASIN_ID'], \
                f"Row {idx}: BASIN_ID mismatch - pred={pred_row['BASIN_ID']}, mapping={mapping_row['BASIN_ID']}"

            # Check area_km2 matches (strong indicator of correct basin)
            assert pred_row['area_km2'] == mapping_row['area_km2'], \
                f"Row {idx}: area_km2 mismatch for BASIN_ID={pred_row['BASIN_ID']} - pred={pred_row['area_km2']}, mapping={mapping_row['area_km2']}"

            # Check BCU_name matches (unique identifier)
            assert pred_row['BCU_name'] == mapping_row['BCU_name'], \
                f"Row {idx}: BCU_name mismatch - pred={pred_row['BCU_name']}, mapping={mapping_row['BCU_name']}"

        # ASSERTION 7: Verify all three percentile outputs have same structure
        assert len(predictions_p10[0]) == 217, "p10 should have 217 rows"
        assert len(predictions_p90[0]) == 217, "p90 should have 217 rows"

        # Verify Amazon values are ordered correctly (p10 < p50 < p90)
        amazon_p10 = predictions_p10[0][predictions_p10[0]['BASIN_ID'] == 9].iloc[0][year_cols].values
        amazon_p50 = predictions_p50[0][predictions_p50[0]['BASIN_ID'] == 9].iloc[0][year_cols].values
        amazon_p90 = predictions_p90[0][predictions_p90[0]['BASIN_ID'] == 9].iloc[0][year_cols].values

        assert np.all(amazon_p10 <= amazon_p50), "Amazon p10 should be <= p50"
        assert np.all(amazon_p50 <= amazon_p90), "Amazon p50 should be <= p90"

        print(f"✓ Basin expansion test passed:")
        print(f"  - 217 MESSAGE rows (expanded from 157 RIME basins)")
        print(f"  - Amazon (BASIN_ID=9) at row {amazon_rows.index[0]}")
        print(f"  - Amazon runoff: {amazon_values.min():.0f} - {amazon_values.max():.0f} km³/yr")
        print(f"  - All BASIN_IDs match basin_mapping")
        print(f"  - All row indices aligned correctly")
