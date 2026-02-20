"""Unified RIME prediction engine.

All RIME emulator variables (water, cooling, building energy intensity)
use the same GWL-binned nearest-neighbor lookup. The mechanism is identical:
``xarray.DataArray.sel(gwl=..., method='nearest')``. Only output dimensions
differ across variables.

This module provides:

- :func:`predict_rime` — unified entry point for all variables
- :func:`clip_gmt` — clip below-range GMT with skewed noise
- :func:`check_emulator_linearity` — diagnostic for percentile input gating

Predictions are returned at **native emulator resolution** — domain modules
own the transformation to MESSAGE-compatible arrays (e.g. basin expansion
from 157 RIME basins to 217 MESSAGE rows lives in
``model.water.data.impacts``).

GMT range: RIME emulators have empirical support for 0.6-7.4 degC.  Low-budget
overshoot scenarios can exit this range at late-century.  Mitigation: clip with
skewed noise (beta(2,5)) to avoid boundary artifacts.

Attribution
-----------
This module is an adapted reimplementation of the GWL-binned nearest-neighbor
prediction from the ``rime`` package by Werning et al. It is **not** a
derivative of the GPL-3.0 source — it reimplements the lookup logic to consume
RIME data products (NetCDF region arrays) within the MESSAGE-ix ecosystem.

Upstream: https://github.com/iiasa/rime (GPL-3.0)

Reference:
    Werning, M., Parkinson, S., et al. (2024).
    "RIME: Regional Impact Model Emulators."
    EarthArXiv preprint. https://doi.org/10.31223/X5H10D

Key differences from upstream ``rime``:
- Simplified API: single ``predict_rime()`` dispatches all variables
- GMT clipping with Beta(2,5) noise for overshoot scenarios
- No Dask parallelism, no pyam dependency

Staleness warning: this implementation tracks the RIME data format as of
early 2025 (GWL-binned NetCDF arrays). Upstream changes to ``rime.core``
or ``rime.rime_functions`` will not propagate automatically. Periodically
compare against the upstream API.
"""

import functools
import logging
from pathlib import Path

import numpy as np
import xarray as xr

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=8)
def _load_rime_dataset(dataset_path: str) -> xr.Dataset:
    """Cached dataset loading."""
    return xr.open_dataset(dataset_path)


# ---------------------------------------------------------------------------
# GMT clipping
# ---------------------------------------------------------------------------


def clip_gmt(
    gmt_array: np.ndarray,
    gmt_min: float = 0.6,
    gmt_ceil: float = 0.9,
    seed: int = 42,
) -> np.ndarray:
    """Clip GMT values below RIME emulator minimum with skewed noise.

    Values below *gmt_min* are replaced with ``gmt_min + beta(2,5) * (gmt_ceil
    - gmt_min)``, landing in ``[gmt_min, gmt_ceil]``.  Values at or above
    *gmt_min* are unchanged.

    Parameters
    ----------
    gmt_array
        GMT values (degC above pre-industrial). Any shape.
    gmt_min
        Lower bound of emulator support.
    gmt_ceil
        Upper bound of noise range for clipped values.
    seed
        RNG seed for reproducibility.
    """
    gmt_clipped = np.asarray(gmt_array).copy()
    original_shape = gmt_clipped.shape
    gmt_flat = gmt_clipped.flatten()

    low_gmt_mask = gmt_flat < gmt_min
    n_low = int(np.sum(low_gmt_mask))

    if n_low > 0:
        rng = np.random.default_rng(seed)
        noise = rng.beta(2, 5, size=n_low) * (gmt_ceil - gmt_min)
        gmt_flat[low_gmt_mask] = gmt_min + noise

    return gmt_flat.reshape(original_shape)


# ---------------------------------------------------------------------------
# Core prediction
# ---------------------------------------------------------------------------


def _predict_from_gmt(
    gmt: float | np.floating,
    dataset_path: str,
    var_name: str,
    sel: dict | None = None,
) -> np.ndarray:
    """GWL-binned nearest-neighbor lookup for a scalar GMT value."""
    ds = _load_rime_dataset(dataset_path)
    data = ds[var_name]

    if sel is not None:
        for dim, value in sel.items():
            if dim in data.dims:
                data = data.sel({dim: value})

    return data.sel(gwl=gmt, method="nearest").values


def predict_rime(
    gmt_array: np.ndarray,
    dataset_path: str | Path,
    var_name: str,
    sel: dict | None = None,
    aggregate: str = "mean",
) -> np.ndarray:
    """Predict RIME variable from GMT array.

    Performs a GWL-binned nearest-neighbour lookup for each GMT value.
    For ensemble input ``(n_runs, n_years)``, applies the lookup per run
    per year — a Monte Carlo estimate of ``E_{P(GMT)}[f(GMT)]``: for each
    timestep *t*, samples ``f(GMT_{run,t})`` across all ensemble members
    and aggregates. This is meaningful only when the emulator response is
    approximately linear; use :func:`check_emulator_linearity` to verify.

    Parameters
    ----------
    gmt_array
        GMT values (degC above pre-industrial).
        Shape ``(n_years,)`` for single trajectory, or
        ``(n_runs, n_years)`` for ensemble.
    dataset_path
        Path to RIME NetCDF dataset.
    var_name
        Variable name within the dataset (e.g. ``"qtot_mean"``,
        ``"capacity_factor"``, ``"EI_cool"``).
    sel
        Optional dimension selections applied before GWL lookup.
    aggregate
        How to reduce the ensemble axis for 2D input:

        ``"mean"`` *(default)*
            Return ``E[f(GMT)]`` — sample mean across runs.
            Shape matches 1D output (no run axis).
        ``"none"``
            Return the full ``(n_runs, n_spatial, n_years)`` array.
            Use this when downstream callers need per-run data,
            e.g. to compute CVaR via :func:`~.risk.cvar_pointwise`.

        Ignored for 1D input.

    Returns
    -------
    np.ndarray
        Native emulator resolution. Shape depends on variable and input:

        - 1D input or ``aggregate="mean"``:
          basin variables ``(157, n_years)``,
          capacity_factor ``(12, n_years)``,
          EI variables ``(12, ..., n_years)``
        - ``aggregate="none"``:
          ``(n_runs, n_spatial, n_years)`` (spatial dims as above)
    """
    if aggregate not in ("mean", "none"):
        raise ValueError(f"aggregate must be 'mean' or 'none', got {aggregate!r}")

    gmt_array = np.asarray(gmt_array)
    path_str = str(dataset_path)

    if gmt_array.ndim == 1:
        preds = [
            _predict_from_gmt(float(g), path_str, var_name, sel=sel) for g in gmt_array
        ]
        return np.stack(preds, axis=-1)

    if gmt_array.ndim == 2:
        n_runs = gmt_array.shape[0]
        lin = check_emulator_linearity(
            path_str, var_name, (float(gmt_array.min()), float(gmt_array.max()))
        )
        if lin["max_deviation"] > 0.01:
            if n_runs == 1:
                raise ValueError(
                    f"predict_rime: single-run ensemble with non-linear emulator "
                    f"({var_name}, max deviation {lin['max_deviation']:.1%}). "
                    f"f(E[GMT]) is not a reliable substitute for E[f(GMT)]."
                )
            log.warning(
                "predict_rime: non-linear emulator response for %s "
                "(max deviation %.1f%%)%s — MC mean is approximate",
                var_name,
                lin["max_deviation"] * 100,
                f"; only {n_runs} runs (recommend >= 100)" if n_runs < 100 else "",
            )
        run_results = []
        for i in range(n_runs):
            preds = [
                _predict_from_gmt(float(g), path_str, var_name, sel=sel)
                for g in gmt_array[i]
            ]
            run_results.append(np.stack(preds, axis=-1))
        ensemble = np.stack(run_results, axis=0)  # (n_runs, n_spatial, n_years)
        if aggregate == "none":
            return ensemble
        return np.mean(ensemble, axis=0)

    raise ValueError(f"gmt_array must be 1D or 2D, got shape {gmt_array.shape}")


@functools.lru_cache(maxsize=32)
def check_emulator_linearity(
    dataset_path: str | Path,
    var_name: str,
    gmt_range: tuple[float, float],
    n_probe: int = 20,
) -> dict:
    """Probe emulator response linearity over a GMT range.

    Tests whether ``E[f(GMT)]`` approximates ``f(E[GMT])`` by comparing
    predictions at uniformly spaced GMT values against the prediction at
    the mean GMT. Large deviations indicate non-linear response, meaning
    percentile-based input (which implicitly assumes linearity) would be
    unreliable.

    Parameters
    ----------
    dataset_path
        Path to RIME NetCDF dataset.
    var_name
        Variable name within the dataset.
    gmt_range
        ``(gmt_low, gmt_high)`` range to probe.
    n_probe
        Number of GMT values to sample.

    Returns
    -------
    dict
        Keys: ``max_deviation``, ``mean_deviation``, ``is_linear``
        (True if max deviation < 5%).
    """
    gmt_low, gmt_high = gmt_range
    gmt_probes = np.linspace(gmt_low, gmt_high, n_probe)
    gmt_mean = float(np.mean(gmt_probes))
    path_str = str(dataset_path)

    results = [_predict_from_gmt(float(g), path_str, var_name) for g in gmt_probes]
    e_of_f = np.nanmean(np.stack(results, axis=0), axis=0)  # E[f(GMT)]
    f_of_e = _predict_from_gmt(gmt_mean, path_str, var_name)

    with np.errstate(divide="ignore", invalid="ignore"):
        rel_dev = np.abs(e_of_f - f_of_e) / np.abs(f_of_e)

    rel_dev = rel_dev[np.isfinite(rel_dev)]
    max_dev = float(np.max(rel_dev)) if len(rel_dev) > 0 else 0.0
    mean_dev = float(np.mean(rel_dev)) if len(rel_dev) > 0 else 0.0

    return {
        "max_deviation": max_dev,
        "mean_deviation": mean_dev,
        "is_linear": max_dev < 0.05,
    }
