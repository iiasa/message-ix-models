"""Unified RIME prediction engine.

All RIME emulator variables (water, cooling, building energy intensity)
use the same GWL-binned nearest-neighbor lookup. The mechanism is identical:
``xarray.DataArray.sel(gwl=..., method='nearest')``. Only output dimensions
differ across variables.

This module provides:

- :func:`predict_rime` — unified entry point for all variables
- :func:`load_basin_mapping` — 217-row BCU mapping
- :func:`split_basin_macroregion` — expand 157 RIME basins to 217 MESSAGE rows
- :func:`check_emulator_linearity` — diagnostic for percentile input gating

GMT range: RIME emulators have empirical support for 0.6-7.4 degC.  Low-budget
overshoot scenarios can exit this range at late-century.  Mitigation: clip with
skewed noise (beta(2,5)) to avoid boundary artifacts.
"""

import functools
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Emulator-intrinsic constants (not configurable; derived from RIME datasets)
# ---------------------------------------------------------------------------

_NAN_BASIN_IDS = frozenset({0, 141, 154})
_N_RIME_BASINS = 157
_N_MESSAGE_BASINS = 217

# External-name -> dataset-internal-name
_VAR_MAP = {"local_temp": "temp_mean_anomaly"}

# RIME datasets currently live under data/alps/rime_datasets/. This will
# migrate to data/impacts/ in a future PR; until then the path couples
# this module to the ALPS data directory.
_RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _get_rime_region_mapping() -> dict[int, int]:
    """Mapping from BASIN_ID to RIME array index.

    RIME datasets have 157 basins indexed by region IDs [1..162] with gaps.
    """
    dataset_path = (
        _RIME_DATASETS_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    )
    ds = xr.open_dataset(dataset_path)
    rime_region_ids = ds.region.values
    return {int(region_id): i for i, region_id in enumerate(rime_region_ids)}


@functools.lru_cache(maxsize=1)
def load_basin_mapping() -> pd.DataFrame:
    """Load R12 basin mapping with MESSAGE basin codes.

    Returns
    -------
    pd.DataFrame
        217 rows with columns including BASIN_ID, NAME, BASIN, REGION,
        BCU_name, area_km2, model_region, basin_code.
    """
    basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
    basin_df = pd.read_csv(basin_file)
    basin_df = basin_df[basin_df["model_region"] == "R12"].copy()
    basin_df = basin_df.reset_index(drop=True)
    basin_df["basin_code"] = (
        "B" + basin_df["BASIN_ID"].astype(str) + "|" + basin_df["REGION"]
    )
    return basin_df


@functools.lru_cache(maxsize=8)
def _load_rime_dataset(dataset_path: str) -> xr.Dataset:
    """Cached dataset loading."""
    return xr.open_dataset(dataset_path)


# ---------------------------------------------------------------------------
# Dataset path resolution
# ---------------------------------------------------------------------------


def _get_dataset_path(
    variable: str, temporal_res: str = "annual", hydro_model: str = "CWatM"
) -> Path:
    """Resolve path to a RIME NetCDF dataset.

    Parameters
    ----------
    variable
        Target variable. One of: qtot_mean, qr, local_temp,
        capacity_factor, EI_cool, EI_heat.
    temporal_res
        ``"annual"`` or ``"seasonal2step"``.
    hydro_model
        Hydrological model name (basin variables only).

    Raises
    ------
    NotImplementedError
        If capacity_factor or EI requested with seasonal resolution.
    FileNotFoundError
        If the dataset file does not exist.
    """
    if variable.startswith("EI_"):
        if temporal_res == "seasonal2step":
            raise NotImplementedError(
                "Building energy intensity only supports annual temporal resolution"
            )
        mode = variable.split("_")[1]  # 'cool' or 'heat'
        dataset_path = _RIME_DATASETS_DIR / f"region_EI_{mode}_gwl_binned.nc"

    elif variable == "capacity_factor":
        if temporal_res == "seasonal2step":
            raise NotImplementedError(
                "Capacity factor only supports annual temporal resolution"
            )
        dataset_path = _RIME_DATASETS_DIR / "r12_capacity_gwl_ensemble.nc"

    else:
        rime_var = _VAR_MAP.get(variable, variable)
        window = (
            "0"
            if rime_var == "temp_mean_anomaly" and temporal_res == "annual"
            else "11"
        )
        dataset_filename = (
            f"rime_regionarray_{rime_var}_{hydro_model}"
            f"_{temporal_res}_window{window}.nc"
        )
        dataset_path = _RIME_DATASETS_DIR / dataset_filename

    if not dataset_path.exists():
        raise FileNotFoundError(
            f"RIME dataset not found: {dataset_path}\n"
            f"Available variables: qtot_mean, qr, local_temp, "
            f"capacity_factor, EI_cool, EI_heat\n"
            f"Available temporal resolutions: annual, seasonal2step "
            f"(not for capacity_factor/EI)\n"
            f"Available hydro models: CWatM, H08, MIROC-INTEG-LAND, WaterGAP2-2e"
        )

    return dataset_path


# ---------------------------------------------------------------------------
# Low-level prediction
# ---------------------------------------------------------------------------


def _predict_from_gmt(gmt, dataset_path: str, variable: str, sel: dict | None = None):
    """GWL-binned nearest-neighbor lookup for a scalar GMT value.

    Parameters
    ----------
    gmt
        Global warming level (degC above pre-industrial).
    dataset_path
        Path to RIME NetCDF dataset.
    variable
        Variable name within the dataset.
    sel
        Optional dimension selections applied before GWL interpolation.
    """
    ds = _load_rime_dataset(dataset_path)
    data = ds[variable]

    if sel is not None:
        for dim, value in sel.items():
            if dim in data.dims:
                data = data.sel({dim: value})

    return data.sel(gwl=gmt, method="nearest").values


# ---------------------------------------------------------------------------
# GMT clipping
# ---------------------------------------------------------------------------


def _clip_gmt(gmt_array: np.ndarray, temporal_res: str, seed: int = 42) -> np.ndarray:
    """Clip GMT values below RIME emulator minimum with skewed noise.

    Annual emulators: support [0.6, 7.4] degC.
    Seasonal emulators: support [0.8, 7.4] degC (87% NaN at 0.6-0.7 for many basins).

    Values below minimum are clipped to [min, min + noise_range] with
    beta(2, 5) noise to avoid boundary artifacts.
    """
    gmt_clipped = np.asarray(gmt_array).copy()
    original_shape = gmt_clipped.shape
    gmt_flat = gmt_clipped.flatten()

    gmt_min = 0.8 if temporal_res == "seasonal2step" else 0.6
    gmt_ceil = 1.2 if temporal_res == "seasonal2step" else 0.9

    low_gmt_mask = gmt_flat < gmt_min
    n_low = np.sum(low_gmt_mask)

    if n_low > 0:
        rng = np.random.default_rng(seed)
        # Clipped values land in [gmt_min, gmt_ceil]
        noise = rng.beta(2, 5, size=n_low) * (gmt_ceil - gmt_min)
        gmt_flat[low_gmt_mask] = gmt_min + noise

    return gmt_flat.reshape(original_shape)


# ---------------------------------------------------------------------------
# Basin expansion
# ---------------------------------------------------------------------------


def split_basin_macroregion(
    rime_predictions: np.ndarray,
    basin_mapping: pd.DataFrame | None = None,
) -> np.ndarray:
    """Expand 157 RIME basins to 217 MESSAGE basin-region rows.

    Some basins span multiple macroregions. Predictions are split
    proportionally by area of each basin-region fragment.

    Parameters
    ----------
    rime_predictions
        Shape ``(157, n_timesteps)`` for annual or
        ``(157, n_timesteps, n_seasons)`` for seasonal.
    basin_mapping
        If *None*, uses :func:`load_basin_mapping`.

    Returns
    -------
    np.ndarray
        Shape ``(217, ...)`` matching input trailing dimensions.
        Basins 0, 141, 154 have NaN (missing RIME data).
    """
    if basin_mapping is None:
        basin_mapping = load_basin_mapping()

    basin_id_to_rime_idx = _get_rime_region_mapping()

    is_seasonal = rime_predictions.ndim == 3
    if is_seasonal:
        n_rime, n_timesteps, n_seasons = rime_predictions.shape
        message_predictions = np.full((217, n_timesteps, n_seasons), np.nan)
    else:
        n_rime, n_timesteps = rime_predictions.shape
        message_predictions = np.full((217, n_timesteps), np.nan)

    basin_total_areas = basin_mapping.groupby("BASIN_ID")["area_km2"].sum()

    for i, row in basin_mapping.iterrows():
        basin_id = row["BASIN_ID"]
        area_km2 = row["area_km2"]

        if basin_id in basin_id_to_rime_idx:
            rime_idx = basin_id_to_rime_idx[basin_id]
            total_area = basin_total_areas[basin_id]
            area_fraction = area_km2 / total_area

            if is_seasonal:
                message_predictions[i, :, :] = (
                    rime_predictions[rime_idx, :, :] * area_fraction
                )
            else:
                message_predictions[i, :] = (
                    rime_predictions[rime_idx, :] * area_fraction
                )

    return message_predictions


# ---------------------------------------------------------------------------
# Single-trajectory prediction (internal)
# ---------------------------------------------------------------------------


def _predict_single(
    gmt_array: np.ndarray,
    variable: str,
    temporal_res: str,
    percentile: str | None,
    sel: dict | None,
    hydro_model: str,
):
    """Single-trajectory RIME prediction (internal helper)."""
    dataset_path = _get_dataset_path(variable, temporal_res, hydro_model)

    # Building energy intensity
    if variable.startswith("EI_"):
        mode = variable.split("_")[1]
        if mode == "cool":
            var_name = "EI_ac_m2" if percentile is None else f"EI_ac_m2_{percentile}"
        else:
            var_name = "EI_h_m2" if percentile is None else f"EI_h_m2_{percentile}"
        return _predict_from_gmt(gmt_array, str(dataset_path), var_name, sel=sel)

    # Regional variables (capacity_factor)
    if variable == "capacity_factor":
        predict_var = variable if percentile is None else f"{variable}_{percentile}"
        return _predict_from_gmt(gmt_array, str(dataset_path), predict_var)

    # Basin-level variables (qtot_mean, qr, local_temp)
    rime_var = _VAR_MAP.get(variable, variable)

    if temporal_res == "seasonal2step":
        var_dry = (
            f"{rime_var}_dry" if percentile is None else f"{rime_var}_dry_{percentile}"
        )
        pred_dry = _predict_from_gmt(gmt_array, str(dataset_path), var_dry)
        pred_dry_expanded = split_basin_macroregion(pred_dry)

        var_wet = (
            f"{rime_var}_wet" if percentile is None else f"{rime_var}_wet_{percentile}"
        )
        pred_wet = _predict_from_gmt(gmt_array, str(dataset_path), var_wet)
        pred_wet_expanded = split_basin_macroregion(pred_wet)

        return (pred_dry_expanded, pred_wet_expanded)

    predict_var = rime_var if percentile is None else f"{rime_var}_{percentile}"
    rime_predictions = _predict_from_gmt(gmt_array, str(dataset_path), predict_var)
    return split_basin_macroregion(rime_predictions)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def predict_rime(
    gmt_array,
    variable: str,
    temporal_res: str = "annual",
    percentile: str | None = None,
    sel: dict | None = None,
    hydro_model: str = "CWatM",
):
    """Predict RIME variable from GMT array.

    Handles single trajectories (1D) and ensembles (2D). For 2D input
    ``(n_runs, n_years)``, returns ``E[RIME(GMT_i)]`` — the expectation
    across ensemble members.

    Parameters
    ----------
    gmt_array
        GMT values (degC above pre-industrial).
        Shape ``(n_years,)`` for single trajectory, or
        ``(n_runs, n_years)`` for ensemble.
    variable
        Target variable. One of: ``qtot_mean``, ``qr``, ``local_temp``,
        ``capacity_factor``, ``EI_cool``, ``EI_heat``.
    temporal_res
        ``"annual"`` or ``"seasonal2step"``.
    percentile
        Uncertainty percentile suffix (e.g. ``"p10"``, ``"p50"``).
    sel
        Dimension selections for building variables.
    hydro_model
        Hydrological model for basin variables.

    Returns
    -------
    np.ndarray or tuple
        1D input:
            - basin variables: ``(217, n_years)``
            - regional variables: ``(12, n_years)`` or ``(12, arch, urt, n_years)``
        2D input:
            Same shapes, ensemble-averaged.
        Seasonal returns ``(dry, wet)`` tuple.
    """
    gmt_array = np.asarray(gmt_array)

    if gmt_array.ndim == 1:
        gmt_clipped = _clip_gmt(gmt_array, temporal_res)
        return _predict_single(
            gmt_clipped, variable, temporal_res, percentile, sel, hydro_model
        )

    if gmt_array.ndim == 2:
        n_runs, n_years = gmt_array.shape
        gmt_clipped = _clip_gmt(gmt_array, temporal_res)

        predictions = [
            _predict_single(
                gmt_clipped[i],
                variable,
                temporal_res,
                percentile,
                sel,
                hydro_model,
            )
            for i in range(n_runs)
        ]

        if temporal_res == "seasonal2step":
            dry_stack = np.stack([p[0] for p in predictions], axis=0)
            wet_stack = np.stack([p[1] for p in predictions], axis=0)
            return (np.mean(dry_stack, axis=0), np.mean(wet_stack, axis=0))

        pred_stack = np.stack(predictions, axis=0)
        return np.mean(pred_stack, axis=0)

    raise ValueError(f"gmt_array must be 1D or 2D, got shape {gmt_array.shape}")


def check_emulator_linearity(
    variable: str,
    gmt_range: tuple[float, float],
    temporal_res: str = "annual",
    n_probe: int = 20,
    hydro_model: str = "CWatM",
) -> dict:
    """Probe emulator response linearity over a GMT range.

    Tests whether ``E[f(GMT)]`` approximates ``f(E[GMT])`` by comparing
    predictions at uniformly spaced GMT values against the prediction at
    the mean GMT. Large deviations indicate non-linear response, meaning
    percentile-based input (which implicitly assumes linearity) would be
    unreliable.

    Parameters
    ----------
    variable
        RIME variable to test.
    gmt_range
        ``(gmt_low, gmt_high)`` range to probe.
    temporal_res
        Temporal resolution.
    n_probe
        Number of GMT values to sample.
    hydro_model
        Hydrological model.

    Returns
    -------
    dict
        Keys: ``max_deviation``, ``mean_deviation``, ``is_linear``
        (True if max deviation < 5%).
    """
    gmt_low, gmt_high = gmt_range
    gmt_probes = np.linspace(gmt_low, gmt_high, n_probe)
    gmt_mean = np.mean(gmt_probes)

    # Predict at each probe point
    results = []
    for g in gmt_probes:
        pred = _predict_single(
            np.array([g]), variable, temporal_res, None, None, hydro_model
        )
        if isinstance(pred, tuple):
            pred = pred[0]  # use dry season for diagnostic
        results.append(pred)

    probe_stack = np.stack(results, axis=0)
    e_of_f = np.nanmean(probe_stack, axis=0)  # E[f(GMT)]

    # Predict at mean GMT
    f_of_e = _predict_single(
        np.array([gmt_mean]), variable, temporal_res, None, None, hydro_model
    )
    if isinstance(f_of_e, tuple):
        f_of_e = f_of_e[0]

    # Compute relative deviation where f_of_e is non-zero
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
