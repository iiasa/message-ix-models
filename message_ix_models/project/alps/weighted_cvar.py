"""Weighted CVaR calculation for ensemble risk metrics.

Computes importance-weighted conditional value-at-risk (CVaR) for ensemble data.
CVaR_α represents the expected value in the worst α% of outcomes.

Pure functions operate on data structures (arrays, DataFrames).
File I/O is handled by calling code.
"""

import numpy as np
import pandas as pd
from typing import Union


def compute_weighted_expectation(values: np.ndarray, weights: np.ndarray) -> float:
    """Compute weighted expectation.

    Args:
        values: Array of values
        weights: Array of normalized weights (must sum to 1)

    Returns:
        Weighted mean
    """
    values = np.asarray(values)
    weights = np.asarray(weights)

    if len(values) != len(weights):
        raise ValueError(f"values ({len(values)}) and weights ({len(weights)}) must have same length")

    return float(np.average(values, weights=weights))


def compute_weighted_cvar_single(values: np.ndarray, weights: np.ndarray, alpha: float) -> float:
    """Compute weighted CVaR for a single distribution.

    CVaR_α = E[X | X ≤ q_α] where q_α is the α-th percentile.

    Args:
        values: Array of values (e.g., water availability for one basin-year)
        weights: Array of normalized weights (must sum to 1)
        alpha: CVaR level as percentile (e.g., 10 for 10th percentile = worst 10%)

    Returns:
        Weighted CVaR value
    """
    values = np.asarray(values)
    weights = np.asarray(weights)

    if len(values) != len(weights):
        raise ValueError(f"values ({len(values)}) and weights ({len(weights)}) must have same length")

    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")

    # Sort by values (ascending for lower tail)
    sort_idx = np.argsort(values)
    sorted_values = values[sort_idx]
    sorted_weights = weights[sort_idx]

    # Find cutoff using cumulative weights
    cum_weights = np.cumsum(sorted_weights)
    threshold = alpha / 100.0

    # Find index where cumulative weight exceeds threshold
    cutoff_idx = np.searchsorted(cum_weights, threshold, side='right')

    if cutoff_idx == 0:
        # Edge case: threshold so low that tail is empty, use minimum
        return float(sorted_values[0])

    # Compute weighted mean of tail
    tail_values = sorted_values[:cutoff_idx]
    tail_weights = sorted_weights[:cutoff_idx]

    if tail_weights.sum() > 0:
        cvar = np.average(tail_values, weights=tail_weights)
    else:
        # Fallback to minimum if tail has zero weight
        cvar = sorted_values[0]

    return float(cvar)


def compute_weighted_cvar(values_3d: np.ndarray,
                          weights: np.ndarray,
                          cvar_levels: list[float],
                          basin_ids: list = None,
                          year_columns: list = None) -> dict[str, pd.DataFrame]:
    """Compute weighted CVaR across ensemble for all basin-year combinations.

    Args:
        values_3d: 3D array (n_runs, n_basins, n_years) of ensemble values
        weights: 1D array (n_runs,) of normalized weights
        cvar_levels: List of CVaR percentiles (e.g., [10, 50, 90])
        basin_ids: Optional list of basin identifiers for DataFrame index
        year_columns: Optional list of year values for DataFrame columns

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', etc.
        Each value is a DataFrame (n_basins × n_years)
    """
    values_3d = np.asarray(values_3d)
    weights = np.asarray(weights)

    if values_3d.ndim != 3:
        raise ValueError(f"values_3d must be 3D array, got shape {values_3d.shape}")

    n_runs, n_basins, n_years = values_3d.shape

    if len(weights) != n_runs:
        raise ValueError(f"weights ({len(weights)}) must match first dimension of values_3d ({n_runs})")

    # Verify weights are normalized
    if not np.isclose(weights.sum(), 1.0, atol=1e-6):
        raise ValueError(f"weights must sum to 1.0, got {weights.sum()}")

    # Initialize result arrays
    results = {
        'expectation': np.zeros((n_basins, n_years))
    }
    for alpha in cvar_levels:
        results[f'cvar_{int(alpha)}'] = np.zeros((n_basins, n_years))

    # Compute statistics for each basin-year
    for basin_idx in range(n_basins):
        for year_idx in range(n_years):
            # Extract values across runs for this basin-year
            values_dist = values_3d[:, basin_idx, year_idx]

            # Weighted expectation
            results['expectation'][basin_idx, year_idx] = np.average(values_dist, weights=weights)

            # Weighted CVaR for each level
            for alpha in cvar_levels:
                results[f'cvar_{int(alpha)}'][basin_idx, year_idx] = compute_weighted_cvar_single(
                    values_dist, weights, alpha
                )

    # Convert to DataFrames
    if basin_ids is None:
        basin_ids = list(range(n_basins))
    if year_columns is None:
        year_columns = list(range(n_years))

    for key in results:
        results[key] = pd.DataFrame(
            results[key],
            index=basin_ids,
            columns=year_columns
        )

    return results


def validate_cvar_monotonicity(cvar_results: dict[str, pd.DataFrame],
                                cvar_levels: list[float]) -> dict:
    """Validate that CVaR results satisfy monotonicity constraints.

    CVaR should be monotonically increasing: CVaR_10 ≤ CVaR_50 ≤ E[X]

    Args:
        cvar_results: Dictionary from compute_weighted_cvar
        cvar_levels: List of CVaR percentiles used

    Returns:
        Dictionary with validation results and violation counts
    """
    violations = {}
    sorted_levels = sorted(cvar_levels)

    # Check pairwise monotonicity
    for i in range(len(sorted_levels) - 1):
        level_low = sorted_levels[i]
        level_high = sorted_levels[i + 1]

        cvar_low = cvar_results[f'cvar_{int(level_low)}']
        cvar_high = cvar_results[f'cvar_{int(level_high)}']

        violation_mask = cvar_low > cvar_high
        n_violations = violation_mask.sum().sum()

        violations[f'cvar_{int(level_low)}_vs_{int(level_high)}'] = int(n_violations)

    # Check CVaR_max ≤ expectation
    if sorted_levels:
        highest_level = sorted_levels[-1]
        cvar_max = cvar_results[f'cvar_{int(highest_level)}']
        expectation = cvar_results['expectation']

        violation_mask = cvar_max > expectation
        n_violations = violation_mask.sum().sum()
        violations[f'cvar_{int(highest_level)}_vs_expectation'] = int(n_violations)

    total_violations = sum(violations.values())

    return {
        'violations': violations,
        'total_violations': total_violations,
        'is_valid': total_violations == 0
    }
