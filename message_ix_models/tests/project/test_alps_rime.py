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
    _RimeEnsemble,
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
        # These are stored as DataFrames for eager evaluation (backward compatibility)
        self.predictions_p50 = {}

        for i in range(self.n_runs):
            base_value = 1000 + i * 100
            # p50: increasing trend over years
            values_p50 = np.array([[base_value + 10*y for y in range(self.n_years)]] * self.n_basins)
            self.predictions_p50[i] = pd.DataFrame(values_p50, index=basins, columns=years)

        # Importance weights (non-uniform)
        self.importance_weights = np.array([0.4, 0.3, 0.2, 0.05, 0.05])
        self.uniform_weights = np.ones(self.n_runs) / self.n_runs
        self.run_ids = np.arange(self.n_runs)

    def _create_mock_ensemble(self, weights: np.ndarray, with_percentile_sampling: bool = False) -> tuple[_RimeEnsemble, np.ndarray]:
        """Helper to create mock _RimeEnsemble for testing with emulator uncertainty.

        Args:
            weights: Weights for runs
            with_percentile_sampling: If True, add percentile sampling for emulator uncertainty

        Returns:
            Tuple of (ensemble, weights) - weights may be expanded if with_percentile_sampling=True
        """
        # Create mock GMT trajectories (constant for simplicity)
        gmt_trajectories = {i: np.ones(self.n_years) * 1.5 for i in range(self.n_runs)}

        # Create mock dataset path (not used in these synthetic tests)
        dataset_path = Path("/mock/path/to/dataset.nc")

        # Create base ensemble
        ensemble = _RimeEnsemble(
            gmt_trajectories=gmt_trajectories,
            variable="qtot_mean",
            dataset_path=dataset_path,
            years=np.array(self.years),
            percentile_sampling=None,
            suban=False,
            basin_mapping=None,
        )

        # Mock the evaluate method to return our synthetic predictions
        def mock_evaluate(run_ids=None, sel=None, as_dataframe=False):
            if run_ids is None:
                run_ids = list(self.predictions_p50.keys())
            return {rid: self.predictions_p50[rid] for rid in run_ids}

        ensemble.evaluate = mock_evaluate

        if with_percentile_sampling:
            # Expand with emulator uncertainty
            ensemble, weights = self.expand(ensemble, weights, n_samples=5, seed=42)
            # Override evaluate again for expanded ensemble
            ensemble.evaluate = mock_evaluate

        return ensemble, weights


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


class TestSeasonalDataIntegration:
    """Test that seasonal datasets work with expectations and CVaR computation."""

    @staticmethod
    def _get_year_columns(df, suban):
        """Extract year columns based on temporal resolution.

        Args:
            df: DataFrame with year columns
            suban: If True, look for seasonal string columns ending with _dry or _wet.
                   If False, look for integer year columns.

        Returns:
            List of column names representing time periods.
        """
        if suban:
            # Seasonal: filter for columns ending with _dry or _wet
            return [col for col in df.columns if isinstance(col, str) and (col.endswith('_dry') or col.endswith('_wet'))]
        else:
            # Annual: filter for integer columns
            return [col for col in df.columns if isinstance(col, (int, np.integer))]

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

        # Load basin mapping for basin expansion
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy().reset_index(drop=True)

        try:
            # New API: returns _RimeEnsemble (lazy)
            ensemble = batch_rime_predictions_with_percentiles(
                magicc_df,
                run_ids,
                dataset_path,
                basin_mapping,
                variable,
                suban
            )

            # Verify ensemble structure
            assert isinstance(ensemble, _RimeEnsemble), "Should return _RimeEnsemble"
            assert len(ensemble.gmt_trajectories) == n_runs
            assert ensemble.variable == variable
            assert ensemble.suban == suban

            # Evaluate ensemble to get predictions
            predictions = ensemble.evaluate(as_dataframe=True)

            # Check output structure
            assert len(predictions) == n_runs

            # Check DataFrame shapes
            expected_n_timesteps = len(years) * 2 if suban else len(years)
            for run_id in run_ids:
                pred_df = predictions[run_id]

                # Check for year columns (use helper to handle both annual and seasonal)
                year_cols = self._get_year_columns(pred_df, suban)
                assert len(year_cols) == expected_n_timesteps, \
                    f"Expected {expected_n_timesteps} year columns, got {len(year_cols)}"

                # Check value ranges (only year columns, ignoring NaN for basins 0, 141, 154)
                pred_values = pred_df[year_cols].values
                min_allowed = -100 if variable.startswith('qr') else 0
                # Use nanmin to ignore NaN values in validation
                valid_values = pred_values[~np.isnan(pred_values)]
                if len(valid_values) > 0:
                    assert np.all(valid_values >= min_allowed), \
                        f"Values exceed allowed threshold: min={valid_values.min()}, allowed>={min_allowed}"

            # Test that expectations and CVaR can be computed with lazy ensemble
            weights = np.ones(n_runs) / n_runs

            expectation = compute_expectation(ensemble, weights=weights)
            cvar_results = compute_rime_cvar(ensemble, weights, cvar_levels=[10, 50, 90])

            # Check year columns in results (use helper to handle both annual and seasonal)
            exp_year_cols = self._get_year_columns(expectation, suban)
            assert len(exp_year_cols) == expected_n_timesteps

            for key in ['expectation', 'cvar_10', 'cvar_50', 'cvar_90']:
                if key in cvar_results:
                    result_year_cols = self._get_year_columns(cvar_results[key], suban)
                    assert len(result_year_cols) == expected_n_timesteps

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
        years = [2025, 2030]  # Use 2025+ to avoid GMT < 0.6°C issues
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

        # Load basin mapping for basin expansion
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy().reset_index(drop=True)

        # New API: returns _RimeEnsemble
        base_ensemble = batch_rime_predictions_with_percentiles(
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

        expanded_ensemble, expanded_weights = expand_predictions_with_emulator_uncertainty(
            base_ensemble,
            weights,
            n_samples,
            seed=42
        )

        # Check expansion worked
        assert isinstance(expanded_ensemble, _RimeEnsemble), "Should return _RimeEnsemble"
        assert len(expanded_ensemble.gmt_trajectories) == n_runs * n_samples
        assert len(expanded_weights) == n_runs * n_samples
        assert expanded_ensemble.percentile_sampling is not None, "Should have percentile sampling"

        # Verify weights sum to 1
        assert expanded_weights.sum() == pytest.approx(1.0), "Weights should sum to 1.0"

        # Evaluate to check predictions
        expanded_preds = expanded_ensemble.evaluate(as_dataframe=True)

        # Check all expanded predictions have correct shape
        expected_n_timesteps = len(years) * 2 if suban else len(years)
        for pred_df in expanded_preds.values():
            year_cols = self._get_year_columns(pred_df, suban)
            assert len(year_cols) == expected_n_timesteps
            pred_values = pred_df[year_cols].values
            min_allowed = -100 if variable.startswith('qr') else 0
            # Ignore NaN values (basins 0, 141, 154 have missing RIME data)
            valid_values = pred_values[~np.isnan(pred_values)]
            if len(valid_values) > 0:
                assert np.all(valid_values >= min_allowed), \
                    f"Expanded predictions must be >= {min_allowed}"


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
        ensemble = batch_rime_predictions_with_percentiles(
            magicc_df, run_ids, dataset_path, basin_mapping, "qtot_mean", suban=False
        )

        # Evaluate ensemble to get predictions
        predictions = ensemble.evaluate(as_dataframe=True)

        # Test with first run's predictions
        pred = predictions[0]

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

        # ASSERTION 7: Verify all runs have same structure (217 rows)
        for run_id in run_ids:
            assert len(predictions[run_id]) == 217, f"Run {run_id} should have 217 rows"

        # Verify Amazon values across runs
        for run_id in run_ids:
            pred_run = predictions[run_id]
            amazon_run = pred_run[pred_run['BASIN_ID'] == 9].iloc[0][year_cols].values
            assert np.all(amazon_run > 1000), f"Run {run_id}: Amazon should have >1000 km³/yr"

        print(f"✓ Basin expansion test passed:")
        print(f"  - 217 MESSAGE rows (expanded from 157 RIME basins)")
        print(f"  - Amazon (BASIN_ID=9) at row {amazon_rows.index[0]}")
        print(f"  - Amazon runoff: {amazon_values.min():.0f} - {amazon_values.max():.0f} km³/yr")
        print(f"  - All BASIN_IDs match basin_mapping")
        print(f"  - All row indices aligned correctly")
