"""CVaR ensemble risk metrics.

Conditional Value-at-Risk (CVaR) for ensemble climate impact predictions.
CVaR_alpha = E[X | X <= q_alpha]: the expected value in the worst alpha%
of outcomes.

Two modes:

- :func:`cvar_pointwise` — independent at each (spatial, year) cell.
  Maximally pessimistic: compounds worst-case across timesteps.
- :func:`cvar_coherent` — selects worst alpha% of full trajectories.
  Temporally coherent: represents persistently unlucky but realizable paths.

Both return numpy arrays. Callers wrap in DataFrames if they need labels.
"""

import numpy as np


def compute_cvar_single(values: np.ndarray, alpha: float) -> float:
    """CVaR for a 1D distribution.

    Parameters
    ----------
    values
        1D array of outcomes.
    alpha
        CVaR level as percentile (0 < alpha < 100). 10 = worst 10%.
    """
    values = np.asarray(values)
    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")
    cutoff = max(1, int(np.ceil(len(values) * alpha / 100.0)))
    return float(np.mean(np.sort(values)[:cutoff]))


def cvar_pointwise(
    values_3d: np.ndarray,
    alpha: float,
) -> np.ndarray:
    """Pointwise CVaR along the ensemble axis.

    Sorts runs independently at each (spatial, year) cell, takes the
    worst *alpha*% and averages.

    Parameters
    ----------
    values_3d
        Shape ``(n_runs, n_spatial, n_years)``.
    alpha
        CVaR level as percentile (0 < alpha < 100).

    Returns
    -------
    np.ndarray
        Shape ``(n_spatial, n_years)``.
    """
    values_3d = np.asarray(values_3d)
    if values_3d.ndim != 3:
        raise ValueError(f"Expected 3D array, got shape {values_3d.shape}")
    n_runs = values_3d.shape[0]
    cutoff = max(1, int(np.ceil(n_runs * alpha / 100.0)))
    sorted_vals = np.sort(values_3d, axis=0)
    return np.mean(sorted_vals[:cutoff], axis=0)


def cvar_coherent(
    values_3d: np.ndarray,
    alpha: float,
) -> np.ndarray:
    """Coherent CVaR: select worst trajectories, then average.

    Trajectory "badness" = mean across all spatial units and years.
    Selects worst *alpha*% of trajectories by this score.

    Parameters
    ----------
    values_3d
        Shape ``(n_runs, n_spatial, n_years)``.
    alpha
        CVaR level as percentile (0 < alpha < 100).

    Returns
    -------
    np.ndarray
        Shape ``(n_spatial, n_years)``.
    """
    values_3d = np.asarray(values_3d)
    if values_3d.ndim != 3:
        raise ValueError(f"Expected 3D array, got shape {values_3d.shape}")
    n_runs = values_3d.shape[0]
    cutoff = max(1, int(np.ceil(n_runs * alpha / 100.0)))
    scores = np.mean(values_3d, axis=(1, 2))
    worst_idx = np.argsort(scores)[:cutoff]
    return np.mean(values_3d[worst_idx], axis=0)
