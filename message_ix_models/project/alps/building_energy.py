"""
Apply correction factors to compute energy demand timeseries.

Formula: E(t,r,a) = γ(t,r,a) × C(r,a,GSAT(t)) × F(t,r,a)

Where:
- γ(t,r,a): correction coefficient (computed at GWL=1.2, climate-invariant)
- C(r,a,GSAT(t)): CHILLED EI at year-specific GSAT from MAGICC
- F(t,r,a): floor area from STURM

Supports both residential ('resid') and commercial ('comm') sectors.
For commercial, CHILLED EI is based on floor-weighted MFH average.
See: sturm/data/input_csv_SSP_2023_comm/cool_intensity.csv (commercial = MFH values)
"""

import logging
from typing import Literal, Optional

import numpy as np
import pandas as pd
import xarray as xr

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


def predict_EI_buildings(
    gmt_array: np.ndarray,
    mode: Literal["cool", "heat"],
    floor_weights: Optional[pd.DataFrame] = None,
) -> xr.DataArray:
    """Predict building energy intensity from GMT array.

    Parameters
    ----------
    gmt_array : np.ndarray
        GMT values for each timestep, shape (n_years,)
    mode : str
        'cool' or 'heat'
    floor_weights : pd.DataFrame, optional
        Floor area weights for MFH aggregation (commercial sector).
        Expected columns: region, arch, urt, floor_Mm2
        If provided, returns MFH-weighted average with dims (region, urt, time).
        If None, returns full array with dims (region, arch, urt, time).

    Returns
    -------
    xr.DataArray
        EI values with dimensions (region, arch, urt, time) or (region, urt, time)
        Units: MJ/m2
    """
    ei_file = package_data_path(
        "alps", "rime_datasets", f"region_EI_{mode}_gwl_binned.nc"
    )
    ds = xr.open_dataset(ei_file)

    ei_var = 'EI_ac_m2' if mode == 'cool' else 'EI_h_m2'
    gwl_values = ds.gwl.values

    # Vectorized nearest-neighbor lookup
    gwl_indices = np.argmin(np.abs(gwl_values[:, None] - gmt_array), axis=0)

    # Select all GWLs at once via isel
    ei_data = ds[ei_var].isel(gwl=xr.DataArray(gwl_indices, dims='time'))
    ei_data = ei_data.assign_coords(time=np.arange(len(gmt_array)))

    if floor_weights is None:
        return ei_data

    # MFH-weighted aggregation for commercial sector
    mfh_weights = floor_weights[floor_weights['arch'].str.startswith('mfh_')].copy()
    if mfh_weights.empty:
        return ei_data

    # Build weight array matching (region, arch, urt)
    regions = ei_data.coords['region'].values
    archs = ei_data.coords['arch'].values
    urts = ei_data.coords['urt'].values

    weight_arr = np.zeros((len(regions), len(archs), len(urts)))
    for _, row in mfh_weights.iterrows():
        try:
            r_idx = list(regions).index(row['region'])
            a_idx = list(archs).index(row['arch'])
            u_idx = list(urts).index(row['urt'])
            weight_arr[r_idx, a_idx, u_idx] = row['floor_Mm2']
        except ValueError:
            continue

    # Normalize weights per (region, urt)
    weight_sum = weight_arr.sum(axis=1, keepdims=True)
    weight_sum[weight_sum == 0] = 1  # avoid division by zero
    weight_arr = weight_arr / weight_sum

    # Apply weights: sum over arch dimension
    weights_da = xr.DataArray(weight_arr, dims=['region', 'arch', 'urt'],
                               coords={'region': regions, 'arch': archs, 'urt': urts})
    ei_weighted = (ei_data * weights_da).sum(dim='arch')

    return ei_weighted


def load_data(mode='cool', scenario='S1', sector='resid'):
    """Load correction coefficients, CHILLED EI, and floor areas.

    Parameters
    ----------
    mode : str
        'cool' or 'heat'
    scenario : str
        'S1', 'S2', or 'S3'
    sector : str
        'resid' or 'comm'
    """
    # Load correction coefficients (skip metadata comment line)
    coeff_file = package_data_path(
        "alps", "correction_coefficients",
        f"correction_coefficients_{mode}_{scenario}_{sector}.csv"
    )
    coeff_df = pd.read_csv(coeff_file, comment='#')

    # Load CHILLED EI
    ei_file = package_data_path(
        "alps", "rime_datasets", f"region_EI_{mode}_gwl_binned.nc"
    )
    chilled_ds = xr.open_dataset(ei_file)

    # Load floor areas for the specified sector
    floor_file = package_data_path("alps", f"sturm_floor_area_R12_{sector}.csv")
    floor_df = pd.read_csv(floor_file)

    return coeff_df, chilled_ds, floor_df


def _compute_mfh_weighted_ei(chilled_ds, resid_floor_df, region, urt, mode='cool'):
    """Compute floor-weighted average of MFH CHILLED EI across all GWL levels.

    For commercial, we use MFH archetypes as reference following STURM precedent.
    See: sturm/data/input_csv_SSP_2023_comm/cool_intensity.csv

    Returns array of EI values at each GWL level (same shape as chilled_ds.gwl).
    """
    ei_var = 'EI_ac_m2' if mode == 'cool' else 'EI_h_m2'
    gwl_values = chilled_ds.gwl.values

    # Get MFH archetypes for this region/urt at year 2020 (reference weights)
    mask = (
        (resid_floor_df['region'] == region) &
        (resid_floor_df['urt'] == urt) &
        (resid_floor_df['arch'].str.startswith('mfh_')) &
        (resid_floor_df['year'] == 2020)
    )
    mfh_archs = resid_floor_df[mask]

    if len(mfh_archs) == 0:
        return np.full(len(gwl_values), np.nan)

    # Compute floor-weighted average at each GWL
    weighted_ei = np.zeros(len(gwl_values))
    total_floor = 0.0

    for _, row in mfh_archs.iterrows():
        arch = row['arch']
        floor = row['floor_Mm2']

        if floor <= 0:
            continue

        try:
            ei_at_gwls = chilled_ds[ei_var].sel(
                region=region, arch=arch, urt=urt
            ).values
            weighted_ei += ei_at_gwls * floor
            total_floor += floor
        except (KeyError, ValueError):
            continue

    if total_floor <= 0:
        return np.full(len(gwl_values), np.nan)

    return weighted_ei / total_floor


def compute_energy_demand_ensemble(
    mode='cool',
    scenario='S1',
    gmt_trajectories=None,
    years=None,
    sector='resid',
):
    """Compute energy demand with full MAGICC ensemble (vectorized).

    For each (region, arch, urt, year), computes E for all GSAT trajectories
    and returns mean and std.

    Parameters
    ----------
    mode : str
        'cool' or 'heat'
    scenario : str
        'S1', 'S2', or 'S3'
    gmt_trajectories : dict[int, np.ndarray]
        Dict mapping run_id -> GMT trajectory array, from rime.get_gmt_ensemble()
    years : np.ndarray
        Year labels corresponding to trajectory indices, from rime.get_gmt_ensemble()
    sector : str
        'resid' or 'comm'

    Returns
    -------
    pd.DataFrame
        Energy demand with mean and std across ensemble
    """
    log.info(f"Computing {mode.upper()} energy demand ensemble (scenario {scenario}, sector {sector})...")

    coeff_df, chilled_ds, floor_df = load_data(mode, scenario, sector)
    ei_var = 'EI_ac_m2' if mode == 'cool' else 'EI_h_m2'
    gwl_values = chilled_ds.gwl.values

    # For commercial: load residential floor data for MFH-weighted EI computation
    resid_floor_df = None
    mfh_ei_cache = {}
    if sector == 'comm':
        resid_floor_df = pd.read_csv(package_data_path("alps", "sturm_floor_area_R12_resid.csv"))
        log.info("Pre-computing MFH-weighted EI for commercial (following STURM precedent)")

    # Build year -> index mapping
    years_list = list(years)
    year_to_idx = {int(y): i for i, y in enumerate(years_list)}
    run_ids = sorted(gmt_trajectories.keys())
    n_runs = len(run_ids)
    log.info(f"Using {n_runs} MAGICC ensemble members")

    results = []
    n_total = 0
    n_success = 0

    for _, row in coeff_df.iterrows():
        region, arch, urt, year = row['region'], row['arch'], row['urt'], row['year']
        γ, floor_Mm2 = row['correction_coeff'], row['floor_Mm2']
        n_total += 1

        if np.isnan(γ) or floor_Mm2 <= 0:
            continue

        if year not in year_to_idx:
            continue

        year_idx = year_to_idx[year]
        # Get GSAT values for all runs at this year, then map to GWL
        gsats_for_year = np.array([gmt_trajectories[rid][year_idx] for rid in run_ids])
        gwls_for_year = gwl_values[np.argmin(np.abs(gwl_values[:, None] - gsats_for_year), axis=0)]

        # Get EI at each GWL (vectorized lookup)
        try:
            # Select EI for this region/arch/urt at all GWL values
            ei_at_gwls = chilled_ds[ei_var].sel(
                region=region, arch=arch, urt=urt
            ).values  # shape (n_gwl,)
        except (KeyError, ValueError):
            # For commercial: use MFH-weighted average (STURM precedent)
            if sector == 'comm' and resid_floor_df is not None:
                cache_key = (region, urt)
                if cache_key not in mfh_ei_cache:
                    mfh_ei_cache[cache_key] = _compute_mfh_weighted_ei(
                        chilled_ds, resid_floor_df, region, urt, mode=mode
                    )
                ei_at_gwls = mfh_ei_cache[cache_key]
                if np.all(np.isnan(ei_at_gwls)):
                    continue
            else:
                continue

        # Map GWL values to indices
        gwl_indices = np.searchsorted(gwl_values, gwls_for_year)
        gwl_indices = np.clip(gwl_indices, 0, len(gwl_values) - 1)

        # Get EI for each run
        ei_vals = ei_at_gwls[gwl_indices]  # shape (n_runs,)

        # Filter out invalid EI values
        valid_mask = ~np.isnan(ei_vals) & (ei_vals > 0)
        if not valid_mask.any():
            continue

        # Compute E for all runs: E = γ × EI × floor
        E_vals = (γ * ei_vals[valid_mask] * floor_Mm2) / 1e6  # EJ

        results.append({
            'region': region, 'year': year, 'arch': arch, 'urt': urt, 'vintage': arch,
            f'E_{mode}_mean_EJ': np.mean(E_vals),
            f'E_{mode}_std_EJ': np.std(E_vals),
            f'E_{mode}_p10_EJ': np.percentile(E_vals, 10),
            f'E_{mode}_p50_EJ': np.percentile(E_vals, 50),
            f'E_{mode}_p90_EJ': np.percentile(E_vals, 90),
        })
        n_success += 1

    log.info(f"Processed {n_total} combinations")
    log.info(f"Successful: {n_success} ({100*n_success/n_total:.1f}%)")
    return pd.DataFrame(results)
