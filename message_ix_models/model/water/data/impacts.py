"""Transform water-domain RIME predictions to MESSAGE basin rows.

See :doc:`/impacts/index` for the water-impact representation.
"""

import functools
import logging

import numpy as np

from message_ix_models.model.water.utils import (
    load_basin_mapping,
    split_basin_macroregion,
)
from message_ix_models.tools.impacts import (
    clip_gmt,
    open_rime_dataset,
    predict_rime,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RIME-specific basin index mapping
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _get_rime_region_mapping() -> dict[int, int]:
    """Mapping from BASIN_ID to RIME array index.

    RIME datasets have 157 basins indexed by region IDs [1..162] with gaps.
    Uses a reference dataset (qtot_mean annual window11) to discover the
    mapping; region IDs are identical across all basin-level RIME datasets.
    """
    ds = open_rime_dataset("rime_regionarray_qtot_mean_CWatM_annual_window11.nc")
    return {int(region_id): i for i, region_id in enumerate(ds.region.values)}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def predict_water_rime(
    gmt_array,
    variable: str,
    temporal_res: str = "annual",
    hydro_model: str = "CWatM",
    percentile: str | None = None,
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
    hydro_model
        Hydrological model name for dataset selection.
    percentile
        Uncertainty percentile suffix (e.g. ``"p10"``).

    Returns
    -------
    np.ndarray or tuple
        Annual: ``(217, n_years)``.
        Seasonal: ``((217, n_years), (217, n_years))`` tuple (dry, wet).
    """
    gmt_array = np.asarray(gmt_array)

    if temporal_res == "seasonal2step":
        gmt_clipped = clip_gmt(gmt_array, gmt_min=0.8, gmt_ceil=1.2)
    else:
        gmt_clipped = clip_gmt(gmt_array, gmt_min=0.6, gmt_ceil=0.9)

    window = "11"
    filename = (
        f"rime_regionarray_{variable}_{hydro_model}_{temporal_res}_window{window}.nc"
    )

    basin_mapping = load_basin_mapping()
    basin_id_to_rime_idx = _get_rime_region_mapping()

    def _expand(raw: np.ndarray) -> np.ndarray:
        return split_basin_macroregion(raw, basin_mapping, basin_id_to_rime_idx)

    if temporal_res == "seasonal2step":
        sfx = f"_{percentile}" if percentile else ""
        raw_dry = predict_rime(gmt_clipped, filename, f"{variable}_dry{sfx}")
        raw_wet = predict_rime(gmt_clipped, filename, f"{variable}_wet{sfx}")
        return (_expand(raw_dry), _expand(raw_wet))

    var_name = variable if not percentile else f"{variable}_{percentile}"
    return _expand(predict_rime(gmt_clipped, filename, var_name))
