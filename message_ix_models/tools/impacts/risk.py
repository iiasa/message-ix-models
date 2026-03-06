"""CVaR risk metrics over RIME ensemble predictions.

Conditional Value-at-Risk (CVaR_alpha) = E[X | X <= q_alpha]:
the expected impact in the worst *alpha*% of climate outcomes.

Both functions call :func:`~.rime.predict_rime` with ``aggregate="none"``
to obtain the full ``(n_runs, n_spatial, n_years)`` ensemble, then reduce
along the run axis.

Two reduction modes:

- :func:`cvar_pointwise` — independent at each (spatial, year) cell.
  Maximally pessimistic: compounds worst-case across timesteps.
- :func:`cvar_coherent` — selects worst alpha% of full trajectories.
  Temporally coherent: represents persistently unlucky but realizable paths.

Both return ``(n_spatial, n_years)`` arrays. Callers wrap in DataFrames
if they need labels.
"""

from pathlib import Path

import numpy as np

from .rime import predict_rime


def cvar_pointwise(
    gmt_array: np.ndarray,
    dataset_path: str | Path,
    var_name: str,
    alpha: float,
    sel: dict | None = None,
) -> np.ndarray:
    """Pointwise CVaR over RIME ensemble predictions.

    For each (spatial, year) cell independently, sorts runs and averages
    the worst *alpha*% — maximally pessimistic across timesteps.

    Parameters
    ----------
    gmt_array
        Shape ``(n_runs, n_years)``. Must be 2D.
    dataset_path
        Path to RIME NetCDF dataset.
    var_name
        Variable name within the dataset.
    alpha
        CVaR level as percentile (0 < alpha < 100). E.g. 10 = worst 10%.
    sel
        Optional dimension selections passed to :func:`~.rime.predict_rime`.

    Returns
    -------
    np.ndarray
        Shape ``(n_spatial, n_years)``.
    """
    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")
    ensemble = predict_rime(
        gmt_array, dataset_path, var_name, sel=sel, aggregate="none"
    )
    n_runs = ensemble.shape[0]
    cutoff = max(1, int(np.ceil(n_runs * alpha / 100.0)))
    return np.mean(np.sort(ensemble, axis=0)[:cutoff], axis=0)


def cvar_coherent(
    gmt_array: np.ndarray,
    dataset_path: str | Path,
    var_name: str,
    alpha: float,
    sel: dict | None = None,
) -> np.ndarray:
    """Coherent CVaR over RIME ensemble predictions.

    Ranks trajectories by mean impact across all spatial units and years,
    selects the worst *alpha*%, and averages — temporally coherent paths.

    Parameters
    ----------
    gmt_array
        Shape ``(n_runs, n_years)``. Must be 2D.
    dataset_path
        Path to RIME NetCDF dataset.
    var_name
        Variable name within the dataset.
    alpha
        CVaR level as percentile (0 < alpha < 100).
    sel
        Optional dimension selections passed to :func:`~.rime.predict_rime`.

    Returns
    -------
    np.ndarray
        Shape ``(n_spatial, n_years)``.
    """
    if not 0 < alpha < 100:
        raise ValueError(f"alpha must be between 0 and 100, got {alpha}")
    ensemble = predict_rime(
        gmt_array, dataset_path, var_name, sel=sel, aggregate="none"
    )
    n_runs = ensemble.shape[0]
    cutoff = max(1, int(np.ceil(n_runs * alpha / 100.0)))
    scores = np.mean(ensemble, axis=(1, 2))
    worst_idx = np.argsort(scores)[:cutoff]
    return np.mean(ensemble[worst_idx], axis=0)
