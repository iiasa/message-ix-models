"""Generate water-availability data for the message-ix-models water module.

Source data is the ISIMIP3b CWaTM hydrology ensemble: 5 GCMs (GFDL-ESM4,
IPSL-CM6A-LR, MPI-ESM1-2-HR, MRI-ESM2-0, UKESM1-0-LL) × climate scenarios
``ssp126``, ``ssp370``, ``ssp585``, W5E5 bias-adjusted, 0.5° global. See the
ISIMIP protocol repository `ISI-MIP/isimip-protocol-3`_ for the specifier
vocabulary; the raw NetCDFs live in the IIASA local ISIMIP sample at
``/mnt/p/watxene/ISIMIP/ISIMIP3b/``. Basin-level aggregation onto the
MESSAGE R12 BCU mapping (``basins_delineated/basins_by_region_simpl_R12.shp``)
is performed by the standalone ``hydro_preprocess`` pipeline (Julia + Python);
its outputs are staged at
``/mnt/p/watxene/ISIMIP_postprocessed/data_for_vignesh/message_nexus_input_2026/``.

Method (linear except the quantile order statistic and the Variable-MF
environmental-flow mask):

1. Per (variable, SSP), load the 5 GCM monthly basin frames. Daily CWaTM
   ``qtot`` is mean-aggregated to monthly inside the loader.
2. Collapse the realization axis: cross-GCM mean per (basin, month) gives
   the ensemble-mean monthly series. Steps 3-5 reduce the temporal axis
   on that series.
3. **Annual percentile**: q50 / q30 / q10 over the 60 months in each
   trailing 5y window per basin, written to ``low`` / ``med`` / ``high``
   files. The shipped label inversion (lower quantile → higher stress →
   ``high`` file) is preserved.
4. **5y seasonal-monthly cycle**: mean per (basin, calendar month, 5y
   bin); shipped only as ``_low`` per the legacy convention.
5. **Environmental flow**: Variable-MF method on the ensemble-mean
   monthly series, then 5y annual + 5y seasonal-monthly variants.

Output filenames use the legacy RCP forcing key: source ``ssp126`` writes
to ``*_2p6_*``, ``ssp370`` to ``*_7p0_*``, ``ssp585`` to ``*_8p5_*`` (new
RCP label added for the high-forcing series, since the legacy set only
covered up to 7p0). Runoff and recharge are clipped at zero post-aggregation.
The three R12 basins ``30|FSU``, ``51|FSU``, ``154|FSU`` are 100% NaN across
all 5 CWaTM GCMs and appear as zero rows in every output; the water build
excludes them at the runtime basin filter.

Run::

    uv run --no-sync python -m message_ix_models.model.water.data.pre_processing.generate_hydro_availability

.. _ISI-MIP/isimip-protocol-3: https://github.com/ISI-MIP/isimip-protocol-3
"""

# TODO: move this module's docstring content to DOCS when the docs update lands.

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)

HYDRO_PREPROCESS_DIR = Path(
    "/mnt/p/watxene/ISIMIP_postprocessed/data_for_vignesh/message_nexus_input_2026"
)
DELINEATION = package_data_path("water", "delineation")
AVAILABILITY = package_data_path("water", "availability")

CWATM_GCMS = (
    "gfdl-esm4",
    "ipsl-cm6a-lr",
    "mpi-esm1-2-hr",
    "mri-esm2-0",
    "ukesm1-0-ll",
)
SSPS = ("ssp126", "ssp370", "ssp585")
META_COLS = ("BASIN_ID", "BCU_name", "NAME", "REGION", "area_km2")

# Source SSP label (used for loading from pdrive) -> RCP forcing label
# (used for the output filename in `data/water/availability/`). 8p5 is a
# new RCP key added by this refresh; legacy set covered up to 7p0 only.
_SSP_TO_RCP: dict[str, str] = {
    "ssp126": "2p6",
    "ssp370": "7p0",
    "ssp585": "8p5",
}

# Year sample for shipped 5y outputs: 2015, 2020, ..., 2100
SAMPLE_YEARS: tuple[int, ...] = tuple(int(y) for y in np.arange(2015, 2105, 5))

# Legacy convention: "low" means low-stress = median availability; "high" means
# high-stress = tail of low availability. Hence the inverted quantile values.
PERCENTILES: dict[str, float] = {"low": 0.5, "med": 0.3, "high": 0.1}

type Region = Literal["R12", "ZMB"]
type Variable = Literal["qtot", "qr"]


# ---------- Loading and monthly aggregation ----------

def _load_csv(path: Path) -> pd.DataFrame:
    """Read a hydro_preprocess CSV; drop meta columns; index by BCU_name."""
    df = pd.read_csv(path)
    missing = set(META_COLS) - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: missing meta columns {missing}")
    out = df.drop(columns=list(META_COLS))
    out.index = pd.Index(df["BCU_name"], name="BCU_name")
    return out


def _to_period_columns(values: pd.DataFrame) -> pd.DataFrame:
    """Convert ISO date-string columns to monthly PeriodIndex.

    Daily inputs get collapsed to monthly via mean (same calendar-month group).
    """
    dates = pd.to_datetime(values.columns)
    periods = pd.PeriodIndex(dates, freq="M")
    if periods.is_unique:
        out = values.copy()
        out.columns = periods
        return out
    arr = values.values
    return (
        pd.DataFrame(arr, columns=periods, index=values.index)
        .T.groupby(level=0)
        .mean()
        .T
    )


def load_cwatm_monthly(var: Variable, ssp: str, gcm: str) -> pd.DataFrame:
    """Load one CWaTM ``(var, ssp, gcm)`` file as a monthly basin frame.

    Daily ``qtot`` files are mean-aggregated to monthly inside the loader.
    """
    freq = "monthly" if var == "qr" else "daily"
    path = HYDRO_PREPROCESS_DIR / f"{var}_{freq}_CWatM_{gcm}_{ssp}_future.csv"
    return _to_period_columns(_load_csv(path))


def _zero_fill_all_nan_basins(panel: np.ndarray, basins: pd.Index, ssp: str) -> None:
    """In-place: rows that are all-NaN across every GCM × month get zero-filled.

    Keeps the array finite during quantile computation. These basins ship as
    zero rows in every output and are excluded by the water build's runtime
    basin filter.
    """
    all_nan = np.isnan(panel).all(axis=(1, 2))
    if all_nan.any():
        gaps = basins[all_nan].tolist()
        log.warning("Zero-filling all-NaN basins (%s): %s", ssp, gaps)
        panel[all_nan, :, :] = 0.0


def load_gcm_panel(var: Variable, ssp: str) -> tuple[np.ndarray, pd.Index, pd.PeriodIndex]:
    """Load the 5-GCM monthly basin panel as ``(basin, gcm, month)`` array.

    Returns ``(panel, basins, months)`` where ``panel.shape ==
    (n_basins, n_gcms, n_months)``.
    """
    frames = [load_cwatm_monthly(var, ssp, gcm) for gcm in CWATM_GCMS]
    # All GCM frames share the same basin index and monthly column range; take
    # the first as canonical and validate the rest in-flight.
    basins = frames[0].index
    months = frames[0].columns
    for gcm, df in zip(CWATM_GCMS[1:], frames[1:], strict=True):
        if not df.index.equals(basins):
            raise ValueError(f"basin index mismatch for {ssp}/{gcm}")
        if not df.columns.equals(months):
            raise ValueError(f"month columns mismatch for {ssp}/{gcm}")
    panel = np.stack([df.values for df in frames], axis=1)  # (basin, gcm, month)
    _zero_fill_all_nan_basins(panel, basins, ssp)
    return panel, basins, months


# ---------- 5y aggregation ----------

def _bin_masks(months: pd.PeriodIndex) -> dict[int, np.ndarray]:
    """Boolean column mask per sample year: months in (year-4) .. year."""
    years = months.year
    return {y: (years >= y - 4) & (years <= y) for y in SAMPLE_YEARS}


# TODO: route the 5y temporal reducers (pooled_annual_quantile,
# seasonal_5y_monthly_mean, eflow_annual_from_monthly, eflow_seasonal_5y) through
# tools/impacts/temporal.py once that module supports quantile.
def pooled_annual_quantile(
    ensemble: np.ndarray, months: pd.PeriodIndex, q: float
) -> np.ndarray:
    """Quantile of the ensemble-mean monthly series within each 5y bin.

    For each (basin, sample year), take the ``q`` quantile of the 60 monthly
    values in the trailing 5-year window (12 for the leading 2015 bin).

    Returns ``(n_basins, n_sample_years)``.
    """
    masks = _bin_masks(months)
    out = np.empty((ensemble.shape[0], len(SAMPLE_YEARS)), dtype=float)
    for j, y in enumerate(SAMPLE_YEARS):
        out[:, j] = np.quantile(ensemble[:, masks[y]], q, axis=1)
    return out


def seasonal_5y_monthly_mean(
    ensemble: np.ndarray, months: pd.PeriodIndex
) -> np.ndarray:
    """Mean of calendar-month values within each 5y bin.

    Returns ``(n_basins, n_sample_years × 12)`` flattened as
    ``(year_0_jan, year_0_feb, …, year_0_dec, year_1_jan, …)``.
    """
    masks = _bin_masks(months)
    month_of = months.month  # 1..12 per column
    n_basins = ensemble.shape[0]
    out = np.empty((n_basins, len(SAMPLE_YEARS) * 12), dtype=float)
    for j, y in enumerate(SAMPLE_YEARS):
        bin_mask = masks[y]
        bin_months = month_of[bin_mask]
        bin_ensemble = ensemble[:, bin_mask]  # (basin, k)
        for m in range(1, 13):
            col_mask = bin_months == m
            if not col_mask.any():
                out[:, j * 12 + (m - 1)] = np.nan
                continue
            out[:, j * 12 + (m - 1)] = bin_ensemble[:, col_mask].mean(axis=1)
    return out


# ---------- Environmental flow (Variable-MF) ----------

def variable_mf_monthly(monthly: np.ndarray, months: pd.PeriodIndex) -> np.ndarray:
    """Variable-MF environmental-flow reserve, applied per calendar year.

    For each (basin, year), MAF = mean of the 12 monthly values; per month:

    - value > 0.8 · MAF        → reserve 0.20 · value
    - 0.4 · MAF < value ≤ 0.8 · MAF → reserve 0.45 · value
    - value ≤ 0.4 · MAF        → reserve 0.60 · value

    Returns ``np.abs`` of the result (legacy convention).
    """
    years = months.year
    out = np.empty_like(monthly, dtype=float)
    for y in np.unique(years):
        sl = years == y
        block = monthly[:, sl]
        maf = block.mean(axis=1, keepdims=True)
        high = block > 0.8 * maf
        med = (block > 0.4 * maf) & (block <= 0.8 * maf)
        out[:, sl] = np.where(high, block * 0.2, np.where(med, block * 0.45, block * 0.6))
    return np.abs(out)


def eflow_annual_from_monthly(
    eflow_monthly: np.ndarray, months: pd.PeriodIndex
) -> np.ndarray:
    """5y mean of the monthly e-flow series per basin."""
    masks = _bin_masks(months)
    out = np.empty((eflow_monthly.shape[0], len(SAMPLE_YEARS)), dtype=float)
    for j, y in enumerate(SAMPLE_YEARS):
        out[:, j] = eflow_monthly[:, masks[y]].mean(axis=1)
    return out


def eflow_seasonal_5y(
    eflow_monthly: np.ndarray, months: pd.PeriodIndex
) -> np.ndarray:
    """Cross-month seasonal cycle of e-flow per 5y bin (mean, not quantile)."""
    masks = _bin_masks(months)
    month_of = months.month
    n_basins = eflow_monthly.shape[0]
    out = np.empty((n_basins, len(SAMPLE_YEARS) * 12), dtype=float)
    for j, y in enumerate(SAMPLE_YEARS):
        bin_mask = masks[y]
        bin_months = month_of[bin_mask]
        bin_block = eflow_monthly[:, bin_mask]
        for m in range(1, 13):
            col_mask = bin_months == m
            if not col_mask.any():
                out[:, j * 12 + (m - 1)] = np.nan
                continue
            out[:, j * 12 + (m - 1)] = bin_block[:, col_mask].mean(axis=1)
    return out


# ---------- Output formatting ----------

def _annual_columns() -> pd.Index:
    return pd.Index(
        pd.to_datetime([f"{y}-12-31" for y in SAMPLE_YEARS]).strftime("%Y-%m-%d")
    )


def _monthly_columns() -> pd.Index:
    cols = []
    for y in SAMPLE_YEARS:
        for m in range(1, 13):
            cols.append(pd.Period(f"{y}-{m:02d}", freq="M").to_timestamp(how="end"))
    return pd.Index(pd.DatetimeIndex(cols).strftime("%Y-%m-%d"))


def _annual_frame(values: np.ndarray, basins: pd.Index) -> pd.DataFrame:
    return pd.DataFrame(values, index=basins, columns=_annual_columns())


def _monthly_frame(values: np.ndarray, basins: pd.Index) -> pd.DataFrame:
    return pd.DataFrame(values, index=basins, columns=_monthly_columns())


# ---------- Filtering ----------

def _filter_region_basins(df: pd.DataFrame, region: Region) -> pd.DataFrame:
    """Reorder rows to the region's delineation file (positional contract)."""
    delin = pd.read_csv(DELINEATION / f"basins_by_region_simpl_{region}.csv")
    return df.reindex(index=delin["BCU_name"].values)


def _format_for_write(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror the legacy shipped schema: integer index, string date columns."""
    return df.reset_index(drop=True)


# ---------- Orchestration ----------

def build_ssp_outputs(var: Variable = "qtot") -> dict[str, pd.DataFrame]:
    """Build all percentile + e-flow frames for the configured SSPs.

    Returns a dict keyed by output filename (without region suffix), with the
    RCP forcing label: ``qtot_5y_2p6_low`` (from CWaTM ssp126), ``..._7p0_...``
    (from ssp370), ``..._8p5_...`` (from ssp585). The caller filters per
    region and writes.
    """
    out: dict[str, pd.DataFrame] = {}

    for ssp in SSPS:
        rcp = _SSP_TO_RCP[ssp]
        log.info("Loading %s panel for %s (output key: %s)", var, ssp, rcp)
        panel, basins, months = load_gcm_panel(var, ssp)
        ensemble = panel.mean(axis=1)  # realization axis -> (basin, month)

        # Annual percentile: temporal quantile of the ensemble-mean series
        for rel, q in PERCENTILES.items():
            arr = pooled_annual_quantile(ensemble, months, q)
            arr = np.clip(arr, 0, None) if var == "qtot" else arr
            out[f"{var}_5y_{rcp}_{rel}"] = _annual_frame(arr, basins)

        # 5y seasonal-monthly cycle
        seasonal_arr = seasonal_5y_monthly_mean(ensemble, months)
        seasonal_arr = np.clip(seasonal_arr, 0, None) if var == "qtot" else seasonal_arr
        out[f"{var}_5y_m_{rcp}_low"] = _monthly_frame(seasonal_arr, basins)

        # e-flow on the ensemble-mean monthly series
        eflow_monthly = variable_mf_monthly(ensemble, months)
        out[f"e-flow_{rcp}"] = _annual_frame(
            eflow_annual_from_monthly(eflow_monthly, months), basins
        )
        out[f"e-flow_5y_m_{rcp}"] = _monthly_frame(
            eflow_seasonal_5y(eflow_monthly, months), basins
        )

    return out


def write_outputs(
    frames: dict[str, pd.DataFrame], regions: tuple[Region, ...] = ("R12",)
) -> None:
    """Filter each frame to each region and write."""
    AVAILABILITY.mkdir(parents=True, exist_ok=True)
    for region in regions:
        for key, df in frames.items():
            regional = _filter_region_basins(df, region)
            shipped = _format_for_write(regional)
            out_path = AVAILABILITY / f"{key}_{region}.csv"
            shipped.to_csv(out_path)
            log.info("Wrote %s (%d × %d)", out_path.name, *shipped.shape)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for var in ("qtot", "qr"):
        frames = build_ssp_outputs(var=var)
        # ZMB delineation uses Zambian-province BCU names with zero overlap
        # with R12-aggregated BCUs; the hydro_preprocess output is R12-keyed
        # only. Out of scope for #527 — legacy ZMB CSVs remain in place.
        write_outputs(frames, regions=("R12",))


if __name__ == "__main__":
    main()
