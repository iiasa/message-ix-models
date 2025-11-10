"""Importance weighting for MAGICC ensemble via GWL-space calibration.

Matches target ensemble GWL distribution to reference (ISIMIP3b) using
entropy balancing to compute run-level weights.

Pure functions operate on data structures (arrays, DataFrames).
File I/O is handled by calling code.
"""

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from typing import Optional, Tuple


def extract_gmt_timeseries(magicc_df: pd.DataFrame,
                           run_ids: Optional[list[int]] = None) -> pd.DataFrame:
    """Extract GMT (GSAT) timeseries for all runs.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format, typically from 'data' sheet)
        run_ids: Optional list of specific run_ids to extract (if None, extracts all)

    Returns:
        DataFrame with columns ['run_id', 'year', 'gmt']
        Each row is one observation (run-year combination)
    """
    # Extract GSAT variable
    gsat_pattern = 'AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3'
    gsat_data = magicc_df[magicc_df['Variable'] == gsat_pattern].copy()

    if gsat_data.empty:
        raise ValueError("No GSAT data found in DataFrame")

    # Extract run_ids from Model column (format: "MODEL_NAME|run_N")
    def parse_run_id(model_str):
        if '|run_' in model_str:
            return int(model_str.split('|run_')[1].split('|')[0])
        return None

    gsat_data['run_id'] = gsat_data['Model'].apply(parse_run_id)
    gsat_data = gsat_data.dropna(subset=['run_id'])
    gsat_data['run_id'] = gsat_data['run_id'].astype(int)

    # Filter to requested run_ids
    if run_ids is not None:
        gsat_data = gsat_data[gsat_data['run_id'].isin(run_ids)]

    # Melt year columns to long format
    # Year columns are strings like '1990', '2000', etc.
    id_vars = ['run_id']
    year_cols = [c for c in gsat_data.columns if c not in ['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'run_id']]

    result = gsat_data[id_vars + year_cols].melt(
        id_vars=id_vars,
        value_vars=year_cols,
        var_name='year',
        value_name='gmt'
    )

    # Convert year to int, drop NaN values
    result['year'] = result['year'].astype(int)
    result = result.dropna(subset=['gmt'])
    result = result.sort_values(['run_id', 'year']).reset_index(drop=True)

    return result


def compute_gwl_histograms(gmt_timeseries: pd.DataFrame,
                           gwl_bins: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Compute GWL histogram for each run.

    Args:
        gmt_timeseries: DataFrame with columns ['run_id', 'year', 'gmt']
        gwl_bins: Array of bin edges (length G+1 for G bins)

    Returns:
        Tuple of:
        - n_ig: Array (n_runs × n_bins) with observation counts per run per bin
        - run_ids: Array of run IDs (length n_runs)
    """
    run_ids = np.sort(gmt_timeseries['run_id'].unique())
    n_bins = len(gwl_bins) - 1
    n_runs = len(run_ids)

    n_ig = np.zeros((n_runs, n_bins), dtype=int)

    for i, run_id in enumerate(run_ids):
        run_data = gmt_timeseries[gmt_timeseries['run_id'] == run_id]
        counts, _ = np.histogram(run_data['gmt'].values, bins=gwl_bins)
        n_ig[i, :] = counts

    return n_ig, run_ids


def compute_target_proportions(gmt_timeseries: pd.DataFrame,
                               gwl_bins: np.ndarray) -> np.ndarray:
    """Compute target GWL proportions from pooled observations.

    Args:
        gmt_timeseries: DataFrame with columns ['run_id', 'year', 'gmt']
        gwl_bins: Array of bin edges (length G+1 for G bins)

    Returns:
        Array of proportions π_g (length n_bins, sums to 1.0)
    """
    all_gmt = gmt_timeseries['gmt'].values
    counts, _ = np.histogram(all_gmt, bins=gwl_bins)
    proportions = counts / counts.sum()
    return proportions


def entropy_balancing_objective(w: np.ndarray, base_weights: np.ndarray) -> float:
    """Entropy objective: Σ w_i log(w_i / b_i)

    Args:
        w: Current weights
        base_weights: Base weights (typically uniform)

    Returns:
        Objective value
    """
    # Avoid log(0) by adding small epsilon
    epsilon = 1e-10
    return np.sum(w * np.log((w + epsilon) / (base_weights + epsilon)))


def entropy_balancing_constraints(w: np.ndarray,
                                  n_ig: np.ndarray,
                                  b_g: np.ndarray) -> np.ndarray:
    """Equality constraints: Σ_i w_i n_i(g) = b_g for all g

    Args:
        w: Run weights (length n_runs)
        n_ig: Observation counts per run per bin (n_runs × n_bins)
        b_g: Target counts per bin (length n_bins)

    Returns:
        Constraint residuals (should be zero at solution)
    """
    weighted_counts = w @ n_ig  # (n_runs,) @ (n_runs, n_bins) = (n_bins,)
    return weighted_counts - b_g


def solve_entropy_balancing(n_ig: np.ndarray,
                            pi_g: np.ndarray,
                            base_weights: Optional[np.ndarray] = None) -> dict:
    """Solve entropy balancing for run weights.

    Minimizes Σ w_i log(w_i / b_i)
    Subject to: Σ_i w_i n_i(g) = N π_g for all g

    Args:
        n_ig: Observation counts per run per bin (n_runs × n_bins)
        pi_g: Target proportions (length n_bins, sums to 1.0)
        base_weights: Base weights (default: uniform = 1.0 per run)

    Returns:
        Dictionary with:
        - weights: Array of run weights (length n_runs)
        - success: Whether optimization succeeded
        - ess: Effective sample size
        - residuals: Constraint residuals (should be ~0)
    """
    n_runs, n_bins = n_ig.shape

    if base_weights is None:
        base_weights = np.ones(n_runs)

    # Total observations and target counts
    N = n_ig.sum()
    b_g = N * pi_g

    # Initial weights (uniform)
    w0 = np.ones(n_runs)

    # Bounds: weights must be non-negative
    bounds = [(0, None) for _ in range(n_runs)]

    # Constraints: equality for each bin
    constraints = {
        'type': 'eq',
        'fun': lambda w: entropy_balancing_constraints(w, n_ig, b_g)
    }

    # Solve
    result = minimize(
        fun=lambda w: entropy_balancing_objective(w, base_weights),
        x0=w0,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'maxiter': 1000, 'ftol': 1e-9}
    )

    weights = result.x

    # Compute diagnostics
    ess = (weights.sum() ** 2) / (weights ** 2).sum()
    residuals = entropy_balancing_constraints(weights, n_ig, b_g)

    return {
        'weights': weights,
        'success': result.success,
        'message': result.message,
        'ess': ess,
        'ess_fraction': ess / n_runs,
        'residuals': residuals,
        'max_residual': np.abs(residuals).max(),
        'n_runs': n_runs,
        'n_bins': n_bins,
    }


def compute_gwl_importance_weights(target_timeseries: pd.DataFrame,
                                   reference_timeseries: pd.DataFrame,
                                   gwl_bin_width: float = 0.1) -> dict:
    """Compute importance weights via GWL-space entropy balancing.

    Args:
        target_timeseries: DataFrame with ['run_id', 'year', 'gmt'] for target ensemble
        reference_timeseries: DataFrame with ['run_id', 'year', 'gmt'] for reference ensemble
        gwl_bin_width: GWL bin width in °C (default: 0.1)

    Returns:
        Dictionary with:
        - weights: Array of run weights (length = n_target_runs)
        - run_ids: Array of run IDs
        - gwl_bins: GWL bin edges used
        - diagnostics: Additional diagnostic information
    """
    # Determine GWL range from both ensembles
    all_gmt = np.concatenate([
        target_timeseries['gmt'].values,
        reference_timeseries['gmt'].values
    ])
    gwl_min = np.floor(all_gmt.min() / gwl_bin_width) * gwl_bin_width
    gwl_max = np.ceil(all_gmt.max() / gwl_bin_width) * gwl_bin_width
    gwl_bins = np.arange(gwl_min, gwl_max + gwl_bin_width, gwl_bin_width)

    # Compute target histograms
    n_ig_target, target_run_ids = compute_gwl_histograms(target_timeseries, gwl_bins)

    # Compute reference proportions
    pi_g = compute_target_proportions(reference_timeseries, gwl_bins)

    # Solve entropy balancing
    result = solve_entropy_balancing(n_ig_target, pi_g)

    return {
        'weights': result['weights'],
        'run_ids': target_run_ids,
        'gwl_bins': gwl_bins,
        'gwl_bin_centers': (gwl_bins[:-1] + gwl_bins[1:]) / 2,
        'target_proportions_unweighted': n_ig_target.sum(axis=0) / n_ig_target.sum(),
        'reference_proportions': pi_g,
        'diagnostics': result
    }
