"""CVaR ensemble risk metrics.

Conditional Value-at-Risk (CVaR) for ensemble climate impact predictions.
CVaR_alpha represents the expected value in the worst alpha% of outcomes.

Two methods:

- **pointwise** — CVaR independently at each (spatial, year) cell.
  Maximally pessimistic: compounds worst-case across timesteps into a
  synthetic trajectory no single run experiences.
- **coherent** — selects worst alpha% of full trajectories, then averages.
  Temporally coherent: represents persistently unlucky but realizable paths.

Pure numpy/pandas. No MESSAGE or scenario dependencies.
"""

import numpy as np
import pandas as pd


def compute_cvar_single(values: np.ndarray, alpha: float) -> float:
    """Compute CVaR for a single distribution.

    ``CVaR_alpha = E[X | X <= q_alpha]`` where ``q_alpha`` is the
    alpha-th percentile.

    Parameters
    ----------
    values
        1D array of outcomes.
    alpha
        CVaR level as percentile (0 < alpha < 100).
        E.g., 10 means worst 10%.

    Returns
    -------
    float
        Mean of the worst alpha% of values.
    """
    values = np.asarray(values)

    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")

    sorted_values = np.sort(values)
    cutoff_idx = max(1, int(np.ceil(len(values) * alpha / 100.0)))
    return float(np.mean(sorted_values[:cutoff_idx]))


def compute_cvar(
    values_3d: np.ndarray,
    cvar_levels: list[float],
    basin_ids: list | None = None,
    year_columns: list | None = None,
    method: str = "pointwise",
) -> dict[str, pd.DataFrame]:
    """Compute CVaR across ensemble for all spatial-year combinations.

    Parameters
    ----------
    values_3d
        3D array ``(n_runs, n_spatial, n_years)``.
    cvar_levels
        CVaR percentiles (e.g. ``[10, 50, 90]``).
    basin_ids
        Labels for spatial dimension (DataFrame index).
    year_columns
        Labels for time dimension (DataFrame columns).
    method
        ``"pointwise"`` or ``"coherent"``.

    Returns
    -------
    dict
        Keys: ``"expectation"``, ``"cvar_10"``, ``"cvar_50"``, etc.
        Each value is a DataFrame ``(n_spatial, n_years)``.
    """
    values_3d = np.asarray(values_3d)

    if values_3d.ndim != 3:
        raise ValueError(f"values_3d must be 3D, got shape {values_3d.shape}")

    if method == "pointwise":
        result = _compute_cvar_pointwise(
            values_3d, cvar_levels, basin_ids, year_columns
        )
    elif method == "coherent":
        result = _compute_cvar_coherent(values_3d, cvar_levels, basin_ids, year_columns)
    else:
        raise ValueError(
            f"Unknown method '{method}'. Expected 'pointwise' or 'coherent'."
        )

    return result


def _compute_cvar_pointwise(
    values_3d: np.ndarray,
    cvar_levels: list[float],
    basin_ids: list | None = None,
    year_columns: list | None = None,
) -> dict[str, pd.DataFrame]:
    """Pointwise CVaR: independent at each (spatial, year) cell."""
    n_runs, n_basins, n_years = values_3d.shape

    results = {"expectation": np.zeros((n_basins, n_years))}
    for alpha in cvar_levels:
        results[f"cvar_{int(alpha)}"] = np.zeros((n_basins, n_years))

    for basin_idx in range(n_basins):
        for year_idx in range(n_years):
            values_dist = values_3d[:, basin_idx, year_idx]
            results["expectation"][basin_idx, year_idx] = np.mean(values_dist)
            for alpha in cvar_levels:
                results[f"cvar_{int(alpha)}"][basin_idx, year_idx] = (
                    compute_cvar_single(values_dist, alpha)
                )

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
    basin_ids: list | None = None,
    year_columns: list | None = None,
) -> dict[str, pd.DataFrame]:
    """Coherent CVaR: select worst trajectories, then average.

    Trajectory "badness" = mean across all spatial units and years.
    For each CVaR level, select worst alpha% of trajectories by this score.
    """
    n_runs, n_basins, n_years = values_3d.shape

    trajectory_scores = np.mean(values_3d, axis=(1, 2))

    results = {"expectation": np.mean(values_3d, axis=0)}

    for alpha in cvar_levels:
        sort_idx = np.argsort(trajectory_scores)
        sorted_values = values_3d[sort_idx, :, :]
        cutoff_idx = max(1, int(np.ceil(n_runs * alpha / 100.0)))
        results[f"cvar_{int(alpha)}"] = np.mean(
            sorted_values[:cutoff_idx, :, :], axis=0
        )

    if basin_ids is None:
        basin_ids = list(range(n_basins))
    if year_columns is None:
        year_columns = list(range(n_years))

    for key in results:
        results[key] = pd.DataFrame(results[key], index=basin_ids, columns=year_columns)

    return results


def validate_cvar_monotonicity(
    cvar_results: dict[str, pd.DataFrame],
    cvar_levels: list[float],
) -> dict:
    """Validate CVaR monotonicity: higher alpha should mean higher risk.

    CVaR_10 <= CVaR_50 <= E[X] (for lower-tail risk, i.e., water scarcity).

    Parameters
    ----------
    cvar_results
        Output of :func:`compute_cvar`.
    cvar_levels
        CVaR levels used in computation.

    Returns
    -------
    dict
        ``violations`` (per-pair counts), ``total_violations``,
        ``is_valid`` (True if zero violations).
    """
    violations = {}
    sorted_levels = sorted(cvar_levels)

    for i in range(len(sorted_levels) - 1):
        level_low = sorted_levels[i]
        level_high = sorted_levels[i + 1]

        cvar_low = cvar_results[f"cvar_{int(level_low)}"]
        cvar_high = cvar_results[f"cvar_{int(level_high)}"]

        violation_mask = cvar_low > cvar_high
        violations[f"cvar_{int(level_low)}_vs_{int(level_high)}"] = int(
            violation_mask.sum().sum()
        )

    if sorted_levels:
        highest_level = sorted_levels[-1]
        cvar_max = cvar_results[f"cvar_{int(highest_level)}"]
        expectation = cvar_results["expectation"]

        violation_mask = cvar_max > expectation
        violations[f"cvar_{int(highest_level)}_vs_expectation"] = int(
            violation_mask.sum().sum()
        )

    total_violations = sum(violations.values())

    return {
        "violations": violations,
        "total_violations": total_violations,
        "is_valid": total_violations == 0,
    }
