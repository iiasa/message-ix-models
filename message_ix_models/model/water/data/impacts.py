"""Water-domain RIME impact transformations.

Transforms raw RIME predictions (157 basins at native emulator resolution)
to MESSAGE-compatible arrays (217 basin-region rows). The expansion handles
transboundary basins that span multiple macroregions via area-weighted
splitting.

Integration point for water CID workflows. Future additions:
- Groundwater share formula: ``gw_share = 0.95 * qr / (qtot + qr)``
- Unit conversion: km3 -> MCM (multiply by 1000)
- Sign convention: negate for MESSAGE demand semantics
"""

import functools
import logging

import numpy as np
import pandas as pd
import xarray as xr

from message_ix_models.tools.impacts.rime import _RIME_DATASETS_DIR, predict_rime
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Basin geometry constants (MESSAGE water module structure)
# ---------------------------------------------------------------------------

_NAN_BASIN_IDS = frozenset({0, 141, 154})
_N_RIME_BASINS = 157
_N_MESSAGE_BASINS = 217


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _get_rime_region_mapping() -> dict[int, int]:
    """Mapping from BASIN_ID to RIME array index.

    RIME datasets have 157 basins indexed by region IDs [1..162] with gaps.
    Uses a reference dataset (qtot_mean annual) to discover the mapping;
    region IDs are identical across all basin-level RIME datasets.
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


# ---------------------------------------------------------------------------
# Basin expansion (157 RIME -> 217 MESSAGE)
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
        message_predictions = np.full(
            (_N_MESSAGE_BASINS, n_timesteps, n_seasons), np.nan
        )
    else:
        n_rime, n_timesteps = rime_predictions.shape
        message_predictions = np.full((_N_MESSAGE_BASINS, n_timesteps), np.nan)

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
# Public API
# ---------------------------------------------------------------------------


def predict_water_rime(
    gmt_array,
    variable: str,
    temporal_res: str = "annual",
    **kwargs,
) -> np.ndarray | tuple[np.ndarray, np.ndarray]:
    """Predict water variable at MESSAGE basin resolution (217 rows).

    Wraps :func:`~message_ix_models.tools.impacts.predict_rime` with
    basin expansion from 157 RIME basins to 217 MESSAGE basin-region rows.

    Parameters
    ----------
    gmt_array
        GMT values (degC above pre-industrial).
    variable
        Basin-level variable: ``qtot_mean``, ``qr``, or ``local_temp``.
    temporal_res
        ``"annual"`` or ``"seasonal2step"``.
    **kwargs
        Passed to :func:`predict_rime` (percentile, sel, hydro_model).

    Returns
    -------
    np.ndarray or tuple
        Annual: ``(217, n_years)``.
        Seasonal: ``((217, n_years), (217, n_years))`` tuple (dry, wet).
    """
    raw = predict_rime(gmt_array, variable, temporal_res, **kwargs)
    if temporal_res == "seasonal2step":
        return (split_basin_macroregion(raw[0]), split_basin_macroregion(raw[1]))
    return split_basin_macroregion(raw)
