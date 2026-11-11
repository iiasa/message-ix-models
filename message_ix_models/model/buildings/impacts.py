"""Building energy CIDs from RIME energy-intensity surfaces.

See :doc:`/global/impacts/index` for the calibration-layer definitions.
"""

import logging
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import xarray as xr
from iam_units import registry

from message_ix_models.tools.impacts import (
    GmtArray,
    clip_gmt,
    open_rime_dataset,
    predict_rime,
)
from message_ix_models.tools.impacts.temporal import sample_to_model_years
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)

# MJ/m2 * Mm2 = MJ * 1e6 = TJ; TJ / 1e6 = EJ
_MJ_MM2_TO_EJ = 1e-6

# MESSAGE demand parameter expects GWa
_EJ_TO_GWA = registry("1 EJ").to("GW * year").magnitude

_CORRECTION_COEFFICIENT_SCENARIO = "SSP2"  # gamma tables are SSP2-derived


def _buildings_data_path(*parts: str):
    return package_data_path("buildings", *parts)


#: File names a scenario data directory may hold in place of the packaged files.
#: Any other file in the directory is an error, so a misnamed file cannot be
#: silently ignored.
SCENARIO_DATA_FILES = frozenset(
    [f"region_EI_{m}_gwl_binned.nc" for m in ("cool", "heat")]
    + [
        f"correction_coefficients_{m}_{_CORRECTION_COEFFICIENT_SCENARIO}_{s}.csv"
        for m in ("cool", "heat")
        for s in ("resid", "comm")
    ]
)


def check_scenario_data(data_dir: str | Path) -> list[str]:
    """Return the scenario data files in *data_dir*; raise on anything else.

    A scenario data directory replaces some of the packaged EI surfaces
    (``region_EI_{cool,heat}_gwl_binned.nc``, e.g. a different building
    envelope or cooling setpoint) and gamma tables
    (``correction_coefficients_{cool,heat}_SSP2_{resid,comm}.csv``, e.g. a
    different AC adoption or insulation path in STURM). Files it does not hold
    are read from the package.
    """
    path = Path(data_dir)
    if not path.is_dir():
        raise FileNotFoundError(f"buildings scenario data directory not found: {path}")
    names = sorted(p.name for p in path.iterdir() if p.is_file())
    if unknown := [n for n in names if n not in SCENARIO_DATA_FILES]:
        raise ValueError(
            f"Unexpected files in buildings scenario data {path}: {unknown}; "
            f"allowed: {sorted(SCENARIO_DATA_FILES)}"
        )
    if not names:
        raise ValueError(f"buildings scenario data {path} holds no data files")
    return names


def _ei_source(mode: Literal["cool", "heat"], data_dir: str | Path | None) -> str:
    """EI dataset to read: the scenario's file if it has one, else the packaged one."""
    name = f"region_EI_{mode}_gwl_binned.nc"
    if data_dir is not None and (Path(data_dir) / name).is_file():
        return str((Path(data_dir) / name).resolve())
    return name


def _load_ei_dataset(
    mode: Literal["cool", "heat"], data_dir: str | Path | None = None
) -> xr.Dataset:
    return open_rime_dataset(_ei_source(mode, data_dir))


def load_correction_coefficients(
    mode: Literal["cool", "heat"],
    sector: Literal["resid", "comm"] = "resid",
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Load gamma and floor-area terms for archetype-level STURM calibration.

    *data_dir*, if given and holding the matching file, replaces the packaged
    table (see :func:`check_scenario_data`).
    """
    scenario = _CORRECTION_COEFFICIENT_SCENARIO
    name = f"correction_coefficients_{mode}_{scenario}_{sector}.csv"
    path = _buildings_data_path("correction_coefficients", name)
    if data_dir is not None and (Path(data_dir) / name).is_file():
        path = Path(data_dir) / name
    return pd.read_csv(path, comment="#")


def load_sector_fractions(reference_scenario: str) -> pd.DataFrame:
    """Load sector fractions of rc_spec/rc_therm per (node, year)."""
    return pd.read_csv(
        _buildings_data_path(f"rc_sector_fractions_{reference_scenario}.csv"),
        comment="#",
    )


def load_floor_areas(sector: Literal["resid", "comm"] = "resid") -> pd.DataFrame:
    """Load SSP2 STURM floor area projections."""
    return pd.read_csv(_buildings_data_path(f"sturm_floor_area_R12_{sector}.csv"))


def load_theta(
    mode: Literal["cool", "heat"],
    reference_scenario: str,
) -> pd.DataFrame:
    """Load theta(node, year) calibrated to the reference scenario demand."""
    path = _buildings_data_path(f"theta_{mode}_{reference_scenario}.csv")
    return pd.read_csv(path, comment="#")


def predict_building_ei(
    gmt_array: np.ndarray,
    mode: Literal["cool", "heat"],
    *,
    percentile: int | str | None = None,
    data_dir: str | Path | None = None,
) -> np.ndarray:
    """Predict EI at native RIME resolution, mean-reduced across the ensemble.

    Parameters
    ----------
    gmt_array
        Shape ``(n_runs, n_years)`` for ensemble or ``(n_years,)`` for single
        trajectory.
    mode
        ``"cool"`` or ``"heat"``.
    percentile
        Conditional response percentile within each RIME GWL window.
    data_dir
        Optional scenario data directory (see :func:`check_scenario_data`).

    Returns
    -------
    np.ndarray
        Shape ``(12, 10, 3, n_years)`` — (region, arch, urt, year).
    """
    gmt_clipped = clip_gmt(gmt_array, gmt_min=0.6, gmt_ceil=0.9)
    return predict_rime(
        gmt_clipped,
        _ei_source(mode, data_dir),
        f"EI_{mode}",
        aggregate="mean",
        percentile=percentile,
    )


def _ei_to_dataframe(
    ei_all: np.ndarray,
    ds: xr.Dataset,
    msg_years: list[int],
) -> pd.DataFrame:
    """Flatten EI prediction array to long DataFrame.

    Returns columns [region, arch, urt, year, ei].
    """
    regions = ds.region.values
    archs = ds.arch.values
    urts = ds.urt.values
    idx = pd.MultiIndex.from_product(
        [regions, archs, urts, msg_years],
        names=["region", "arch", "urt", "year"],
    )
    return pd.DataFrame({"ei": ei_all.ravel()}, index=idx).reset_index()


def _mfh_weighted_ei(
    ei_df: pd.DataFrame,
    resid_floor_df: pd.DataFrame,
) -> pd.DataFrame:
    """Floor-weighted average of MFH archetype EI per (region, urt, year).

    Commercial buildings use residential MFH archetypes weighted by 2020
    floor area (STURM precedent).

    Returns columns [region, urt, year, ei].
    """
    # MFH floor weights at 2020 reference year
    mfh_floors = resid_floor_df[
        (resid_floor_df["arch"].str.startswith("mfh_"))
        & (resid_floor_df["year"] == 2020)
        & (resid_floor_df["floor_Mm2"] > 0)
    ][["region", "arch", "urt", "floor_Mm2"]]

    if mfh_floors.empty:
        return pd.DataFrame(columns=["region", "urt", "year", "ei"])

    # Join EI with floor weights
    mfh_ei = ei_df[ei_df["arch"].str.startswith("mfh_")].merge(
        mfh_floors,
        on=["region", "arch", "urt"],
        how="inner",
    )
    mfh_ei["weighted_ei"] = mfh_ei["ei"] * mfh_ei["floor_Mm2"]

    # Weighted average per (region, urt, year)
    grouped = mfh_ei.groupby(["region", "urt", "year"], as_index=False).agg(
        weighted_sum=("weighted_ei", "sum"),
        floor_sum=("floor_Mm2", "sum"),
    )
    grouped["ei"] = grouped["weighted_sum"] / grouped["floor_sum"]
    return grouped[["region", "urt", "year", "ei"]]


def _compute_sector_energy(
    mode: Literal["cool", "heat"],
    sector: Literal["resid", "comm"],
    ei_df: pd.DataFrame,
    mfh_ei_df: pd.DataFrame,
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Compute energy demand for one mode/sector via vectorized join.

    Returns DataFrame with columns [region, year, value] in EJ.
    """
    coeff = load_correction_coefficients(mode, sector, data_dir)
    valid = coeff["correction_coeff"].notna() & (coeff["floor_Mm2"] > 0)
    coeff = coeff[valid]

    if sector == "resid":
        # Direct join on (region, arch, urt, year)
        merged = coeff.merge(ei_df, on=["region", "arch", "urt", "year"], how="left")
    else:
        # Commercial: join MFH-weighted EI on (region, urt, year)
        merged = coeff.merge(mfh_ei_df, on=["region", "urt", "year"], how="left")

    has_ei = merged["ei"].notna() & (merged["ei"] > 0)
    n_total = len(merged)
    n_computed = has_ei.sum()
    log.info(
        "%s/%s: %d/%d rows with valid EI (%.0f%%)",
        mode,
        sector,
        n_computed,
        n_total,
        100 * n_computed / n_total if n_total else 0,
    )

    merged = merged[has_ei]
    merged["value"] = (
        merged["correction_coeff"] * merged["ei"] * merged["floor_Mm2"] * _MJ_MM2_TO_EJ
    )
    return merged.groupby(["region", "year"], as_index=False)["value"].sum()


def _apply_theta(
    demand: pd.DataFrame,
    mode: Literal["cool", "heat"],
    reference_scenario: str,
) -> pd.DataFrame:
    """Scale raw RIME demand by theta to match SSP-calibrated STURM levels.

    Theta = calibrated / raw at GWL 1.1; it carries the calibration shape
    under warming while EI provides the climate response. Stored at source
    resolution (decadal + 2025); interpolated to demand years. The theta
    min year defines the CID validity range — years before it are excluded.
    """
    theta = load_theta(mode, reference_scenario)
    demand_years = sorted(demand["year"].unique())
    theta = sample_to_model_years(
        theta, ["node"], demand_years, method="interpolate", value_cols=["theta"]
    )

    scaled = demand.merge(theta, on=["node", "year"], how="left")

    missing = scaled.loc[scaled["theta"].isna(), ["node", "year"]]
    if not missing.empty:
        pairs = missing.drop_duplicates().to_dict("records")
        raise ValueError(
            f"Missing theta values for {mode}/{reference_scenario}: {pairs}"
        )

    return scaled.assign(value=lambda df: df["value"] * df["theta"])[
        ["node", "year", "value"]
    ]


def compute_building_cids(
    gmt: GmtArray,
    model_years: list[int],
    reference_scenario: str,
    regions: str = "R12",
    response_percentile: int | str | None = None,
    data_dir: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute building energy CIDs from a GMT ensemble.

    Parameters
    ----------
    gmt
        GMT ensemble + year labels, as returned by :func:`load_magicc_gmt`.
        ``.values`` shape ``(n_runs, n_years)`` for ensemble or
        ``(n_years,)`` for single trajectory.
    model_years
        MESSAGE model years from ``ScenarioInfo.Y``. Only positions in
        ``gmt.years`` that match these are used; 2110 is forward-filled
        from 2100.
    reference_scenario
        SSP scenario for theta and sector fractions.
    regions
        MESSAGE node codelist. Only ``"R12"`` is supported by the shipped
        RIME EI datasets and STURM floor-area files.
    response_percentile
        Conditional response percentile within each RIME GWL window.
    data_dir
        Optional scenario data directory replacing some packaged EI surfaces
        and gamma tables (see :func:`check_scenario_data`). Theta stays the
        packaged one, so demand is a scenario change against the same
        calibration.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        ``(cooling, heating)`` DataFrames with columns
        ``[node, year, value]`` where value is GWa.
    """
    if regions != "R12":
        raise NotImplementedError(
            f"regions={regions!r}; this kernel currently only supports R12 "
            "(RIME EI datasets and STURM floor-area files are R12-coded)."
        )
    if data_dir is not None:
        log.info(
            "Buildings scenario data from %s: %s",
            data_dir,
            check_scenario_data(data_dir),
        )
    values = np.asarray(gmt.values)
    years = np.asarray(gmt.years, dtype=int)

    # CID year range: from theta coverage (excludes historical years)
    # through model horizon, with 2110 forward-filled from 2100.
    theta_min_year = min(
        load_theta("cool", reference_scenario)["year"].min(),
        load_theta("heat", reference_scenario)["year"].min(),
    )
    target = [y for y in model_years if y != 2110 and y >= theta_min_year]
    msg_mask = np.isin(years, target)
    if not msg_mask.any():
        raise ValueError(f"No model years in input. Years: {years[0]}-{years[-1]}")
    gmt_subset = values[:, msg_mask] if values.ndim == 2 else values[msg_mask]
    msg_years = years[msg_mask].tolist()

    log.info(
        "Computing building CIDs: %s, %d model years, GMT shape %s",
        reference_scenario,
        len(msg_years),
        gmt_subset.shape,
    )

    # Pre-compute MFH-weighted EI for commercial (shared across cool/heat)
    resid_floor = load_floor_areas("resid")

    results = {}
    for mode in ("cool", "heat"):
        ei_all = predict_building_ei(
            gmt_subset, mode, percentile=response_percentile, data_dir=data_dir
        )
        ds = _load_ei_dataset(mode, data_dir)
        ei_df = _ei_to_dataframe(ei_all, ds, msg_years)
        mfh_ei_df = _mfh_weighted_ei(ei_df, resid_floor)

        resid = _compute_sector_energy(mode, "resid", ei_df, mfh_ei_df, data_dir)
        comm = _compute_sector_energy(mode, "comm", ei_df, mfh_ei_df, data_dir)

        total = (
            pd.concat([resid, comm], ignore_index=True)
            .groupby(["region", "year"], as_index=False)["value"]
            .sum()
            .assign(
                node=lambda df: f"{regions}_" + df["region"],
                value=lambda df: df["value"] * _EJ_TO_GWA,
            )[["node", "year", "value"]]
        )
        total = _apply_theta(total, mode, reference_scenario)

        # Forward-fill 2110 from 2100 if 2110 is a model year
        if 2110 in model_years and 2100 in total["year"].values:
            total = pd.concat(
                [total, total[total["year"] == 2100].assign(year=2110)],
                ignore_index=True,
            )

        results[mode] = total.sort_values(["node", "year"]).reset_index(drop=True)

    return results["cool"], results["heat"]
