"""GWL-binned nearest-neighbor lookup for RIME emulator data.

Adapted reimplementation of the prediction core in `iiasa/rime
<https://github.com/iiasa/rime>`_ (GPL-3.0); see
`doi:10.1088/2752-5295/adee3d <https://doi.org/10.1088/2752-5295/adee3d>`_
for the underlying method. Predictions return at native emulator resolution;
domain modules own any reshape to MESSAGE-compatible arrays.
"""

import functools
import logging

import numpy as np
import xarray as xr

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=8)
def open_rime_dataset(filename: str) -> xr.Dataset:
    """Cached open of a packaged RIME dataset under ``data/impacts/rime/``."""
    return xr.open_dataset(str(package_data_path("impacts", "rime", filename)))


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
    filename: str,
    var_name: str,
    sel: dict | None = None,
) -> np.ndarray:
    """GWL-binned nearest-neighbor lookup for a scalar GMT value."""
    data = open_rime_dataset(filename)[var_name]

    if sel is not None:
        for dim, value in sel.items():
            if dim in data.dims:
                data = data.sel({dim: value})

    return data.sel(gwl=gmt, method="nearest").values


def predict_rime(
    gmt_array: np.ndarray,
    filename: str,
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
    filename
        RIME NetCDF filename relative to ``data/impacts/rime/``.
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
            Use this when downstream callers need per-run data.

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

    if gmt_array.ndim == 1:
        preds = [
            _predict_from_gmt(float(g), filename, var_name, sel=sel) for g in gmt_array
        ]
        return np.stack(preds, axis=-1)

    if gmt_array.ndim == 2:
        n_runs = gmt_array.shape[0]
        lin = check_emulator_linearity(
            filename, var_name, (float(gmt_array.min()), float(gmt_array.max()))
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
                _predict_from_gmt(float(g), filename, var_name, sel=sel)
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
    filename: str,
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
    filename
        RIME NetCDF filename relative to ``data/impacts/rime/``.
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

    results = [_predict_from_gmt(float(g), filename, var_name) for g in gmt_probes]
    e_of_f = np.nanmean(np.stack(results, axis=0), axis=0)  # E[f(GMT)]
    f_of_e = _predict_from_gmt(gmt_mean, filename, var_name)

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
