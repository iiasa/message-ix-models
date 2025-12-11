"""CVaR calculation for ensemble risk metrics.

Computes conditional value-at-risk (CVaR) for ensemble data.
CVaR_α represents the expected value in the worst α% of outcomes.

Pure functions operate on data structures (arrays, DataFrames).
File I/O is handled by calling code.
"""

import numpy as np
import pandas as pd


def compute_cvar_single(values: np.ndarray, alpha: float) -> float:
    """Compute CVaR for a single distribution.

    CVaR_α = E[X | X ≤ q_α] where q_α is the α-th percentile.

    Args:
        values: Array of values (e.g., water availability for one basin-year)
        alpha: CVaR level as percentile (e.g., 10 for 10th percentile = worst 10%)

    Returns:
        CVaR value (mean of worst α% of values)
    """
    values = np.asarray(values)
    n = len(values)

    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")

    # Sort by values (ascending for lower tail)
    sorted_values = np.sort(values)

    # Find cutoff index for bottom α%
    cutoff_idx = max(1, int(np.ceil(n * alpha / 100.0)))

    # Mean of tail values
    tail_values = sorted_values[:cutoff_idx]
    return float(np.mean(tail_values))


def compute_cvar(
    values_3d: np.ndarray,
    cvar_levels: list[float],
    basin_ids: list = None,
    year_columns: list = None,
    method: str = "pointwise",
) -> dict[str, pd.DataFrame]:
    """Compute CVaR across ensemble for all basin-year combinations.

    Args:
        values_3d: 3D array (n_runs, n_basins, n_years) of ensemble values
        cvar_levels: List of CVaR percentiles (e.g., [10, 50, 90])
        basin_ids: Optional list of basin identifiers for DataFrame index
        year_columns: Optional list of year values for DataFrame columns
        method: CVaR computation method:
            - "pointwise": Compute CVaR independently at each (basin, year).
                          Results can draw from different runs at different timesteps.
                          Represents "worst X% in any given year" (maximally pessimistic).
            - "coherent": Select worst X% of trajectories, then average them.
                         Results are temporally coherent (same runs across all timesteps).
                         Represents "persistently unlucky trajectories" (realizable paths).

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', etc.
        Each value is a DataFrame (n_basins × n_years)

    Notes:
        Pointwise CVaR (default) compounds worst-case outcomes across timesteps, creating
        a synthetic trajectory that no single run experiences. Coherent CVaR maintains
        temporal structure by conditioning on trajectory selection.
    """
    values_3d = np.asarray(values_3d)

    if values_3d.ndim != 3:
        raise ValueError(f"values_3d must be 3D array, got shape {values_3d.shape}")

    # Dispatch to method-specific implementation
    match method:
        case "pointwise":
            return _compute_cvar_pointwise(
                values_3d, cvar_levels, basin_ids, year_columns
            )
        case "coherent":
            return _compute_cvar_coherent(
                values_3d, cvar_levels, basin_ids, year_columns
            )
        case _:
            raise ValueError(
                f"Unknown method '{method}'. Expected 'pointwise' or 'coherent'."
            )


def _compute_cvar_pointwise(
    values_3d: np.ndarray,
    cvar_levels: list[float],
    basin_ids: list = None,
    year_columns: list = None,
) -> dict[str, pd.DataFrame]:
    """Pointwise CVaR: compute independently at each (basin, year).

    This is the original RIME methodology. CVaR at each timestep can draw from
    different runs, creating a synthetic trajectory of compounded worst-cases.
    """
    n_runs, n_basins, n_years = values_3d.shape

    # Initialize result arrays
    results = {"expectation": np.zeros((n_basins, n_years))}
    for alpha in cvar_levels:
        results[f"cvar_{int(alpha)}"] = np.zeros((n_basins, n_years))

    # Compute statistics for each basin-year
    for basin_idx in range(n_basins):
        for year_idx in range(n_years):
            # Extract values across runs for this basin-year
            values_dist = values_3d[:, basin_idx, year_idx]

            # Expectation (simple mean)
            results["expectation"][basin_idx, year_idx] = np.mean(values_dist)

            # CVaR for each level
            for alpha in cvar_levels:
                results[f"cvar_{int(alpha)}"][basin_idx, year_idx] = (
                    compute_cvar_single(values_dist, alpha)
                )

    # Convert to DataFrames
    if basin_ids is None:
        basin_ids = list(range(n_basins))
    if year_columns is None:
        year_columns = list(range(n_years))

    for key in results:
        results[key] = pd.DataFrame(results[key], index=basin_ids, columns=year_columns)

    return results


def _compute_cvar_coherent(
    values_3d: np.ndarray,
    cvar_levels: list[float],
    basin_ids: list = None,
    year_columns: list = None,
) -> dict[str, pd.DataFrame]:
    """Coherent CVaR: select worst trajectories, then average them.

    This approach maintains temporal coherence by conditioning on trajectory selection.
    CVaR represents averaging over persistently unlucky (but realizable) trajectories.

    Implementation:
    1. Compute trajectory-level "badness" score (mean across basins and years)
    2. For each CVaR level, select worst X% of trajectories based on this score
    3. Compute mean of selected trajectories
    """
    n_runs, n_basins, n_years = values_3d.shape

    # Compute trajectory-level badness scores (mean across basins and years)
    # Shape: (n_runs,)
    trajectory_scores = np.mean(values_3d, axis=(1, 2))

    # Initialize result arrays
    results = {"expectation": np.zeros((n_basins, n_years))}
    for alpha in cvar_levels:
        results[f"cvar_{int(alpha)}"] = np.zeros((n_basins, n_years))

    # Compute expectation (mean over all runs)
    results["expectation"] = np.mean(values_3d, axis=0)

    # Compute CVaR for each level
    for alpha in cvar_levels:
        # Sort trajectories by badness score (ascending = worst first)
        sort_idx = np.argsort(trajectory_scores)
        sorted_values = values_3d[sort_idx, :, :]

        # Find cutoff index for bottom α%
        cutoff_idx = max(1, int(np.ceil(n_runs * alpha / 100.0)))

        # Mean of tail trajectories
        tail_values = sorted_values[:cutoff_idx, :, :]
        results[f"cvar_{int(alpha)}"] = np.mean(tail_values, axis=0)

    # Convert to DataFrames
    if basin_ids is None:
        basin_ids = list(range(n_basins))
    if year_columns is None:
        year_columns = list(range(n_years))

    for key in results:
        results[key] = pd.DataFrame(results[key], index=basin_ids, columns=year_columns)

    return results


def validate_cvar_monotonicity(
    cvar_results: dict[str, pd.DataFrame], cvar_levels: list[float]
) -> dict:
    """Validate that CVaR results satisfy monotonicity constraints.

    CVaR should be monotonically increasing: CVaR_10 ≤ CVaR_50 ≤ E[X]

    Args:
        cvar_results: Dictionary from compute_cvar
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

        cvar_low = cvar_results[f"cvar_{int(level_low)}"]
        cvar_high = cvar_results[f"cvar_{int(level_high)}"]

        violation_mask = cvar_low > cvar_high
        n_violations = violation_mask.sum().sum()

        violations[f"cvar_{int(level_low)}_vs_{int(level_high)}"] = int(n_violations)

    # Check CVaR_max ≤ expectation
    if sorted_levels:
        highest_level = sorted_levels[-1]
        cvar_max = cvar_results[f"cvar_{int(highest_level)}"]
        expectation = cvar_results["expectation"]

        violation_mask = cvar_max > expectation
        n_violations = violation_mask.sum().sum()
        violations[f"cvar_{int(highest_level)}_vs_expectation"] = int(n_violations)

    total_violations = sum(violations.values())

    return {
        "violations": violations,
        "total_violations": total_violations,
        "is_valid": total_violations == 0,
    }
