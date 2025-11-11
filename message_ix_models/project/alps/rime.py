"""RIME processing with importance weighting and CVaR calculation.

Core functions for:
- Extracting temperature timeseries from MAGICC output
- Running vectorized RIME predictions across multiple runs
- Computing weighted expectations and CVaR risk metrics
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from scipy.stats import norm

from rime.rime_functions import predict_from_gmt
from rimeX.stats import fit_dist
from .weighted_cvar import compute_weighted_cvar


def extract_temperature_timeseries(magicc_df: pd.DataFrame,
                                   percentile: Optional[float] = None,
                                   run_id: Optional[int] = None) -> pd.DataFrame:
    """Extract GSAT temperature trajectory from MAGICC DataFrame.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)
        percentile: Which percentile to use (5, 10, 50, 90, 95, etc.)
        run_id: Specific run_id to extract (0-599). Takes precedence over percentile.

    Returns:
        DataFrame with columns: ['year', 'gsat_anomaly_K', 'model', 'scenario', 'ssp_family']
    """
    # Determine selection mode
    if run_id is not None:
        # Select by run_id
        model_pattern = f"|run_{run_id}"
        var_pattern = "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"

        temp_data = magicc_df[
            (magicc_df['Model'].str.contains(model_pattern, na=False, regex=False)) &
            (magicc_df['Variable'].str.contains(var_pattern, na=False, regex=False))
        ]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for run_id {run_id}")
    else:
        # Select by percentile
        if percentile is None:
            percentile = 50.0
        percentile_str = f"{percentile}th Percentile" if percentile != 50.0 else "50.0th Percentile"
        var_pattern = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{percentile_str}"

        temp_data = magicc_df[magicc_df['Variable'] == var_pattern]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for percentile {percentile}")

    temp_row = temp_data.iloc[0]

    # Extract year columns (strings like '1990', '2000', etc.)
    year_cols = [col for col in magicc_df.columns
                 if isinstance(col, str) and col.isdigit()]

    temps = {int(year): temp_row[year]
             for year in year_cols
             if pd.notna(temp_row[year])}

    temp_df = pd.DataFrame({
        'year': list(temps.keys()),
        'gsat_anomaly_K': list(temps.values())
    })

    # Extract metadata
    scenario_name = temp_row['Scenario']
    model_name = temp_row['Model']

    # Determine SSP family
    scenario_lower = scenario_name.lower()
    if 'ssp1' in scenario_lower:
        ssp_family = 'SSP1'
    elif 'ssp2' in scenario_lower:
        ssp_family = 'SSP2'
    elif 'ssp3' in scenario_lower:
        ssp_family = 'SSP3'
    elif 'ssp4' in scenario_lower:
        ssp_family = 'SSP4'
    elif 'ssp5' in scenario_lower:
        ssp_family = 'SSP5'
    else:
        ssp_family = 'SSP2'  # default

    temp_df['model'] = model_name
    temp_df['scenario'] = scenario_name
    temp_df['ssp_family'] = ssp_family

    return temp_df


def extract_all_run_ids(magicc_df: pd.DataFrame) -> list[int]:
    """Extract all available run_ids from MAGICC DataFrame.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)

    Returns:
        Sorted list of run_ids
    """
    gsat_data = magicc_df[
        magicc_df['Variable'].str.contains(
            'AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3',
            na=False,
            regex=False
        )
    ]

    def parse_run_id(model_str):
        if '|run_' in model_str:
            try:
                return int(model_str.split('|run_')[1].split('|')[0])
            except (IndexError, ValueError):
                return None
        return None

    run_ids = [parse_run_id(m) for m in gsat_data['Model'].unique()]
    run_ids = [r for r in run_ids if r is not None]

    return sorted(run_ids)


def batch_rime_predictions(magicc_df: pd.DataFrame,
                           run_ids: list[int],
                           dataset_path: Path,
                           basin_mapping: pd.DataFrame,
                           variable: str) -> dict[int, pd.DataFrame]:
    """Run RIME predictions on multiple runs using vectorized GMT lookups.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame
        variable: Variable name (qtot_mean or qr)

    Returns:
        Dictionary mapping run_id -> MESSAGE format DataFrame (217 rows × year columns)
    """
    # Extract GMT timeseries for all runs
    gmt_timeseries = []
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_timeseries.append(temp_df['gsat_anomaly_K'].values)
        if years is None:
            years = temp_df['year'].values

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array(gmt_timeseries)
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Call predict_from_gmt for each unique GMT value (cached dataset)
    # Results: (n_runs * n_years) × n_basins
    predictions_flat = np.array([
        predict_from_gmt(gmt, str(dataset_path), variable)
        for gmt in gmt_flat
    ])

    # Reshape back to 3D: (n_runs × n_years × n_basins)
    n_basins = predictions_flat.shape[1]
    predictions_3d = predictions_flat.reshape(n_runs, n_years, n_basins)

    # Convert each run to MESSAGE format DataFrame
    results = {}
    for i, run_id in enumerate(run_ids):
        # predictions_3d[i]: (n_years × n_basins)
        df = pd.DataFrame(
            predictions_3d[i].T,  # Transpose to (n_basins × n_years)
            columns=years
        )
        results[run_id] = df

    return results


def batch_rime_predictions_with_percentiles(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str
) -> tuple[dict[int, pd.DataFrame], dict[int, pd.DataFrame], dict[int, pd.DataFrame]]:
    """Run RIME predictions extracting p10, p50, p90 for each GMT value.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame
        variable: Base variable name (qtot_mean or qr)

    Returns:
        Tuple of three dictionaries (predictions_p10, predictions_p50, predictions_p90),
        each mapping run_id -> MESSAGE format DataFrame
    """
    # Extract GMT timeseries for all runs
    gmt_timeseries = []
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_timeseries.append(temp_df['gsat_anomaly_K'].values)
        if years is None:
            years = temp_df['year'].values

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array(gmt_timeseries)
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Extract all three percentiles
    predictions_p10_flat = np.array([
        predict_from_gmt(gmt, str(dataset_path), f"{variable}_p10")
        for gmt in gmt_flat
    ])
    predictions_p50_flat = np.array([
        predict_from_gmt(gmt, str(dataset_path), f"{variable}_p50")
        for gmt in gmt_flat
    ])
    predictions_p90_flat = np.array([
        predict_from_gmt(gmt, str(dataset_path), f"{variable}_p90")
        for gmt in gmt_flat
    ])

    # ASSERTION: predict_from_gmt should return non-negative values
    assert np.all(predictions_p10_flat >= 0), \
        f"predict_from_gmt returned negative p10 values: min={predictions_p10_flat.min()}"
    assert np.all(predictions_p50_flat >= 0), \
        f"predict_from_gmt returned negative p50 values: min={predictions_p50_flat.min()}"
    assert np.all(predictions_p90_flat >= 0), \
        f"predict_from_gmt returned negative p90 values: min={predictions_p90_flat.min()}"

    # Reshape all to 3D: (n_runs × n_years × n_basins)
    n_basins = predictions_p10_flat.shape[1]
    predictions_p10_3d = predictions_p10_flat.reshape(n_runs, n_years, n_basins)
    predictions_p50_3d = predictions_p50_flat.reshape(n_runs, n_years, n_basins)
    predictions_p90_3d = predictions_p90_flat.reshape(n_runs, n_years, n_basins)

    # ASSERTION: Verify percentile ordering (p10 ≤ p50 ≤ p90)
    if not np.all(predictions_p10_3d <= predictions_p50_3d):
        n_violations = np.sum(predictions_p10_3d > predictions_p50_3d)
        print(f"WARNING: {n_violations} cases where p10 > p50")
    if not np.all(predictions_p50_3d <= predictions_p90_3d):
        n_violations = np.sum(predictions_p50_3d > predictions_p90_3d)
        print(f"WARNING: {n_violations} cases where p50 > p90")

    # Convert to MESSAGE format DataFrames
    results_p10 = {}
    results_p50 = {}
    results_p90 = {}

    for i, run_id in enumerate(run_ids):
        # Transpose to (n_basins × n_years)
        results_p10[run_id] = pd.DataFrame(predictions_p10_3d[i].T, columns=years)
        results_p50[run_id] = pd.DataFrame(predictions_p50_3d[i].T, columns=years)
        results_p90[run_id] = pd.DataFrame(predictions_p90_3d[i].T, columns=years)

    return results_p10, results_p50, results_p90


def expand_predictions_with_emulator_uncertainty(
    predictions_p10: dict[int, pd.DataFrame],
    predictions_p50: dict[int, pd.DataFrame],
    predictions_p90: dict[int, pd.DataFrame],
    run_ids: list[int],
    weights: np.ndarray,
    n_samples: int = 5,
    seed: int = 42
) -> tuple[dict[int, pd.DataFrame], np.ndarray, np.ndarray]:
    """Expand predictions with emulator uncertainty using RIME-style shuffled sampling.

    NOTE: We adapt RIME's shuffling logic rather than calling make_quantilemap_prediction()
    directly because:
    1. We already have p10/p50/p90 percentiles from predict_from_gmt (no need to reload NetCDF)
    2. Our CVaR calculation uses DataFrame format aligned with MESSAGE basins
    3. RIME's quantilemap operates on full xarray datasets (different data structure)
    4. This approach allows easy comparison of time-coherent vs shuffled strategies

    RIME's philosophy (see rimeX/preproc/quantilemaps.py):
    - Emulator uncertainty is ALEATORY (random year-to-year scatter), not EPISTEMIC (systematic bias)
    - Shuffles emulator quantiles randomly across years to break time-coherence
    - CVaR represents "X% chance in any given year", not "persistently bad trajectory"

    Implementation:
    - For each MAGICC run, generate K pseudo-runs by sampling random emulator quantiles per year
    - Shuffling ensures no persistent "pessimistic emulator" artifacts in CVaR tails

    Args:
        predictions_p10: P10 predictions (run_id -> DataFrame)
        predictions_p50: P50 predictions (run_id -> DataFrame)
        predictions_p90: P90 predictions (run_id -> DataFrame)
        run_ids: List of run IDs
        weights: Original importance weights (length N)
        n_samples: Number of pseudo-runs per MAGICC run (default: 5)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        Tuple of:
        - expanded_predictions: Dict mapping pseudo_run_id -> DataFrame
        - expanded_run_ids: Array of pseudo-run IDs (sequential integers)
        - expanded_weights: Array of weights for N×K pseudo-runs (each = original_weight / K)
    """
    rng = np.random.default_rng(seed)
    K = n_samples
    N = len(run_ids)

    # Get dimensions
    first_pred = predictions_p50[run_ids[0]]
    n_basins, n_years = first_pred.shape
    year_columns = first_pred.columns
    basin_index = first_pred.index

    # Initialize expanded predictions dict
    expanded_predictions = {}
    pseudo_run_id = 0

    # Process each run
    for i, run_id in enumerate(run_ids):
        # Get p10, p50, p90 arrays for this run (n_basins × n_years)
        p10_array = predictions_p10[run_id].values
        p50_array = predictions_p50[run_id].values
        p90_array = predictions_p90[run_id].values

        # Vectorized fit: fit all (basin, year) distributions at once
        # fit_dist automatically dispatches to vectorized version when inputs are arrays
        vec_dist = fit_dist([p50_array, p10_array, p90_array],
                            quantiles=[0.5, 0.05, 0.95],
                            dist_name='lognorm')

        # RIME-style shuffling: sample independent random quantiles for each (basin, year, sample)
        # Shape: (n_samples, n_basins, n_years)
        random_quantiles = rng.uniform(0, 1, size=(K, n_basins, n_years))

        # Sample from fitted distributions using per-year random quantiles
        # This breaks time-coherence: each year gets independent emulator draw
        for k in range(K):
            # Sample shape: (n_basins, n_years) using quantiles of shape (n_basins, n_years)
            samples = vec_dist.ppf_per_year(random_quantiles[k, :, :])

            # Clip negatives to zero: three-parameter lognormal with negative location parameter
            # can produce negative samples when extrapolating to extreme quantiles (<< p05).
            # This occurs when sampling q < 0.5% from right-skewed distributions with loc < 0.
            # Since runoff is physically non-negative, clip to zero.
            # Affects ~0.08% of samples in extreme CVaR tails.
            samples = np.maximum(samples, 0.0)

            pseudo_run_df = pd.DataFrame(
                samples,
                index=basin_index,
                columns=year_columns
            )

            # Store with sequential integer key
            expanded_predictions[pseudo_run_id] = pseudo_run_df
            pseudo_run_id += 1

    # Create array of pseudo-run IDs (0, 1, 2, ..., N×K-1)
    expanded_run_ids = np.arange(N * K)

    # Expand weights: each original weight is split equally among K samples
    expanded_weights = np.repeat(weights / K, K)

    return expanded_predictions, expanded_run_ids, expanded_weights


def compute_expectation(predictions: dict[int, pd.DataFrame],
                        run_ids: np.ndarray,
                        weights: Optional[np.ndarray] = None) -> pd.DataFrame:
    """Compute expectation across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        run_ids: Array of run IDs
        weights: Optional array of importance weights (must sum to ~1.0).
                 If None, uses uniform weights (unweighted mean).

    Returns:
        DataFrame with (weighted) mean predictions (MESSAGE format)
    """
    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    first_pred = predictions[run_ids[0]]
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(first_pred.columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id].values

    # Compute (weighted) mean
    if weights is not None:
        mean = np.average(values_3d, axis=0, weights=weights)
    else:
        mean = np.mean(values_3d, axis=0)

    # Convert back to DataFrame
    result = pd.DataFrame(
        mean,
        index=first_pred.index,
        columns=first_pred.columns
    )

    # ASSERTION: Check for negative values in expectation
    if np.any(result.values < 0):
        n_negative = np.sum(result.values < 0)
        min_value = result.values.min()
        print(f"WARNING: Expectation contains {n_negative}/{result.size} negative values, "
              f"min={min_value:.6f}")

    return result


def compute_rime_cvar(predictions: dict[int, pd.DataFrame],
                      weights: np.ndarray,
                      run_ids: np.ndarray,
                      cvar_levels: list[float] = [10, 50, 90]) -> dict[str, pd.DataFrame]:
    """Compute weighted CVaR across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        weights: Array of importance weights (must sum to ~1.0)
        run_ids: Array of run IDs corresponding to weights
        cvar_levels: List of CVaR percentiles (default: [10, 50, 90])

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', 'cvar_90'
        Each value is a DataFrame (MESSAGE format: n_basins × year columns)
    """
    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    first_pred = predictions[run_ids[0]]
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(first_pred.columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id].values

    # Get basin indices and year columns for DataFrame output
    basin_ids = list(first_pred.index)
    year_columns = list(first_pred.columns)

    # Compute weighted CVaR
    cvar_results = compute_weighted_cvar(
        values_3d,
        weights,
        cvar_levels,
        basin_ids=basin_ids,
        year_columns=year_columns
    )

    # ASSERTION: Check for negative values in CVaR results
    for key, result_df in cvar_results.items():
        if np.any(result_df.values < 0):
            n_negative = np.sum(result_df.values < 0)
            min_value = result_df.values.min()
            print(f"WARNING: CVaR '{key}' contains {n_negative}/{result_df.size} negative values, "
                  f"min={min_value:.6f}")

    return cvar_results
