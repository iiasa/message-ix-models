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

import pandas as pd
import numpy as np
import xarray as xr
import sys
from pathlib import Path
from typing import Dict, Literal, Optional

from message_ix_models.util import package_data_path

# Paths
# External data (CHILLED EI outputs)
DATA_PATH = Path("/mnt/p/watxene/ISIMIP_postprocessed/data_for_vignesh/ALPS_2025/output")

# Local data directory (relative to this file)
LOCAL_DATA = Path(__file__).parent.parent / "data"

# Local output directory for energy demand CSVs
LOCAL_OUTPUT_DIR = Path("energy_demand_timeseries")

# Scenarios
SCENARIOS = ['S1', 'S2', 'S3']
SCENARIO_LABELS = {
    'S1': 'SSP2-BL',
    'S2': 'SSP2-HI',
    'S3': 'SSP2-2020'
}

# SSP scenario MAGICC files mapping
MAGICC_FILES = {
    "SSP2": LOCAL_DATA / "magicc" / "MESSAGE_GLOBIOM_SSP2_v6.4_baseline_magicc.xlsx",
    "SSP5": LOCAL_DATA / "magicc" / "SSP_SSP5_v6.4_baseline_magicc.xlsx"
}


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


def get_gsat_by_year(magicc_file: str) -> Dict[int, float]:
    """
    Extract median GSAT by year from MAGICC Excel file.

    Takes 50th percentile (median) across all model runs.

    Parameters
    ----------
    magicc_file : str
        Path to MAGICC Excel file with GSAT data

    Returns
    -------
    dict
        Mapping of year to median GSAT across all runs
    """
    df = pd.read_excel(magicc_file, sheet_name="data")

    # Filter to Surface Temperature (GSAT) variable
    gsat_data = df[
        df["Variable"] == "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"
    ].copy()

    # Year columns are numeric (1995, 1996, ...)
    year_cols = [col for col in df.columns if isinstance(col, (int, float)) or (
        isinstance(col, str) and col.isdigit()
    )]
    year_cols = sorted([int(col) if isinstance(col, str) else col for col in year_cols])

    # Calculate median across all runs for each year
    gsat_by_year = {}
    for year in year_cols:
        # Convert year to string to access DataFrame column
        year_str = str(year)
        median_val = gsat_data[year_str].median()
        gsat_by_year[year] = median_val

    return gsat_by_year


def get_gsat_ensemble(magicc_file: str) -> pd.DataFrame:
    """
    Load full GSAT ensemble from MAGICC (600 runs × years).

    Returns DataFrame with columns: [run, year, gsat]
    """
    df = pd.read_excel(magicc_file, sheet_name="data")

    # Filter to GSAT
    gsat_data = df[
        df["Variable"] == "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"
    ].copy()

    # Extract run number
    gsat_data['run'] = gsat_data['Model'].str.extract(r'run_(\d+)')[0].astype(int)

    # Year columns - keep as they appear in DataFrame (may be str or int)
    year_cols = [col for col in df.columns if isinstance(col, (int, float)) or (
        isinstance(col, str) and col.isdigit()
    )]

    # Melt to long format using original column names
    gsat_long = gsat_data.melt(
        id_vars=['run'],
        value_vars=year_cols,
        var_name='year',
        value_name='gsat'
    )
    gsat_long['year'] = gsat_long['year'].astype(int)

    return gsat_long


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


def compute_energy_demand(mode='cool', scenario='S1', gsat_by_year=None, sector='resid'):
    """
    Compute energy demand for all years using correction factors (single trajectory).

    Parameters
    ----------
    mode : str
        'cool' or 'heat'
    scenario : str
        'S1', 'S2', or 'S3'
    gsat_by_year : dict, optional
        Mapping of year to GSAT from MAGICC
    sector : str
        'resid' or 'comm'
    """
    print(f"\nComputing {mode.upper()} energy demand (scenario {scenario}, sector {sector})...")
    if gsat_by_year:
        print(f"Using year-specific GSAT from MAGICC")
    else:
        print(f"Using fixed GWL=1.2")

    # Load correction coefficients
    coeff_file = package_data_path(
        "alps", "correction_coefficients",
        f"correction_coefficients_{mode}_{scenario}_{sector}.csv"
    )
    coeff_df = pd.read_csv(coeff_file, comment='#')

    if gsat_by_year is None:
        # Fallback to pre-computed EI in coefficients
        results = []
        for _, row in coeff_df.iterrows():
            γ, floor_Mm2 = row['correction_coeff'], row['floor_Mm2']
            if np.isnan(γ) or floor_Mm2 <= 0:
                continue
            ei_val = row['chilled_ei_MJ_m2']
            if np.isnan(ei_val) or ei_val <= 0:
                continue
            E_EJ = (γ * ei_val * floor_Mm2) / 1e6
            results.append({
                'region': row['region'], 'year': row['year'],
                'arch': row['arch'], 'urt': row['urt'], 'vintage': row['arch'],
                f'E_{mode}_mean_EJ': E_EJ, f'E_{mode}_p50_EJ': E_EJ,
            })
        return pd.DataFrame(results)

    # Build GMT array from gsat_by_year
    years = sorted(gsat_by_year.keys())
    gmt_array = np.array([gsat_by_year[y] for y in years])
    year_to_idx = {y: i for i, y in enumerate(years)}

    # Get EI predictions
    if sector == 'comm':
        # Commercial: MFH-weighted EI
        resid_floor_df = pd.read_csv(package_data_path("alps", "sturm_floor_area_R12_resid.csv"))
        floor_weights = resid_floor_df[resid_floor_df['year'] == 2020]
        ei_data = predict_EI_buildings(gmt_array, mode, floor_weights=floor_weights)
        print("  Using MFH-weighted EI for commercial (STURM precedent)")
    else:
        # Residential: full archetype resolution
        ei_data = predict_EI_buildings(gmt_array, mode)

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

        t_idx = year_to_idx[year]

        try:
            if sector == 'comm':
                # Commercial: dims are (region, urt, time)
                ei_val = float(ei_data.sel(region=region, urt=urt).isel(time=t_idx).values)
            else:
                # Residential: dims are (region, arch, urt, time)
                ei_val = float(ei_data.sel(region=region, arch=arch, urt=urt).isel(time=t_idx).values)
        except (KeyError, ValueError):
            continue

        if np.isnan(ei_val) or ei_val <= 0:
            continue

        E_EJ = (γ * ei_val * floor_Mm2) / 1e6
        results.append({
            'region': region, 'year': year, 'arch': arch, 'urt': urt,
            'vintage': arch, f'E_{mode}_mean_EJ': E_EJ, f'E_{mode}_p50_EJ': E_EJ,
        })
        n_success += 1

    print(f"  Processed {n_total} combinations")
    print(f"  Successful: {n_success} ({100*n_success/n_total:.1f}%)")
    return pd.DataFrame(results)


def compute_mfh_weighted_ei(chilled_ds, resid_floor_df, region, urt, mode='cool'):
    """
    Compute floor-weighted average of MFH CHILLED EI across all GWL levels.

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


def compute_energy_demand_ensemble(mode='cool', scenario='S1', gsat_ensemble=None, sector='resid'):
    """
    Compute energy demand with full MAGICC ensemble (vectorized).

    For each (region, arch, urt, year), computes E for all 600 GSAT trajectories
    and returns mean and std.

    Parameters
    ----------
    mode : str
        'cool' or 'heat'
    scenario : str
        'S1', 'S2', or 'S3'
    gsat_ensemble : pd.DataFrame
        DataFrame with columns [run, year, gsat] from get_gsat_ensemble()
    sector : str
        'resid' or 'comm'

    Returns
    -------
    pd.DataFrame
        Energy demand with mean and std across ensemble
    """
    print(f"\nComputing {mode.upper()} energy demand ensemble (scenario {scenario}, sector {sector})...")

    coeff_df, chilled_ds, floor_df = load_data(mode, scenario, sector)
    ei_var = 'EI_ac_m2' if mode == 'cool' else 'EI_h_m2'
    gwl_values = chilled_ds.gwl.values

    # For commercial: load residential floor data for MFH-weighted EI computation
    resid_floor_df = None
    mfh_ei_cache = {}
    if sector == 'comm':
        resid_floor_df = pd.read_csv(package_data_path("alps", "sturm_floor_area_R12_resid.csv"))
        print("  Pre-computing MFH-weighted EI for commercial (following STURM precedent)")

    # Pre-compute GWL lookup for all GSAT values
    unique_gsats = gsat_ensemble['gsat'].unique()
    gsat_to_gwl = {g: gwl_values[np.argmin(np.abs(gwl_values - g))] for g in unique_gsats}

    # Add GWL column to ensemble
    gsat_ensemble = gsat_ensemble.copy()
    gsat_ensemble['gwl'] = gsat_ensemble['gsat'].map(gsat_to_gwl)

    # Pivot to wide format: rows=years, cols=runs
    gsat_wide = gsat_ensemble.pivot(index='year', columns='run', values='gwl')
    n_runs = gsat_wide.shape[1]
    print(f"  Using {n_runs} MAGICC ensemble members")

    results = []
    n_total = 0
    n_success = 0

    # Get unique years in coefficients
    coeff_years = sorted(coeff_df['year'].unique())

    for _, row in coeff_df.iterrows():
        region, arch, urt, year = row['region'], row['arch'], row['urt'], row['year']
        γ, floor_Mm2 = row['correction_coeff'], row['floor_Mm2']
        n_total += 1

        if np.isnan(γ) or floor_Mm2 <= 0:
            continue

        if year not in gsat_wide.index:
            continue

        # Get GWL values for all runs at this year
        gwls_for_year = gsat_wide.loc[year].values  # shape (n_runs,)

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
                    mfh_ei_cache[cache_key] = compute_mfh_weighted_ei(
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

    print(f"  Processed {n_total} combinations")
    print(f"  Successful: {n_success} ({100*n_success/n_total:.1f}%)")
    return pd.DataFrame(results)


def main(ssp="SSP2", scenario="S1", sector="resid"):
    """
    Compute energy demand for both cooling and heating.

    Parameters
    ----------
    ssp : str
        'SSP2' or 'SSP5'
    scenario : str
        'S1', 'S2', or 'S3'
    sector : str
        'resid' or 'comm'
    """
    sector_label = "Residential" if sector == 'resid' else "Commercial"
    suffix = f"_{sector}"

    print("="*80)
    print("APPLYING CORRECTION FACTORS TO COMPUTE ENERGY DEMAND")
    print(f"SSP: {ssp}, Scenario: {scenario}, Sector: {sector_label}")
    print("="*80)

    # Get MAGICC file for the specified SSP
    if ssp not in MAGICC_FILES:
        print(f"Error: Unknown SSP scenario '{ssp}'. Available: {list(MAGICC_FILES.keys())}")
        sys.exit(1)

    magicc_file = MAGICC_FILES[ssp]

    # Ensure local output directory exists
    LOCAL_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    # Load GSAT timeseries from MAGICC
    print(f"\nLoading GSAT timeseries from {magicc_file}...")
    gsat_by_year = get_gsat_by_year(magicc_file)
    print(f"  Loaded GSAT for {len(gsat_by_year)} years")
    print(f"  GSAT range: {min(gsat_by_year.values()):.2f} - {max(gsat_by_year.values()):.2f}°C")

    # Compute cooling energy demand
    cool_demand = compute_energy_demand(
        mode='cool', scenario=scenario, gsat_by_year=gsat_by_year, sector=sector
    )
    cool_output = LOCAL_OUTPUT_DIR / f"energy_demand_cool_{ssp}_{scenario}{suffix}.csv"
    cool_demand.to_csv(cool_output, index=False)
    print(f"\nSaved: {cool_output}")
    print(f"  Shape: {cool_demand.shape}")
    print(f"  Years: {cool_demand['year'].min()} - {cool_demand['year'].max()}")
    print(f"  Total cooling: {cool_demand['E_cool_mean_EJ'].sum():.4f} EJ (across all years)")

    # Compute heating energy demand
    heat_demand = compute_energy_demand(
        mode='heat', scenario=scenario, gsat_by_year=gsat_by_year, sector=sector
    )
    heat_output = LOCAL_OUTPUT_DIR / f"energy_demand_heat_{ssp}_{scenario}{suffix}.csv"
    heat_demand.to_csv(heat_output, index=False)
    print(f"\nSaved: {heat_output}")
    print(f"  Shape: {heat_demand.shape}")
    print(f"  Years: {heat_demand['year'].min()} - {heat_demand['year'].max()}")
    print(f"  Total heating: {heat_demand['E_heat_mean_EJ'].sum():.4f} EJ (across all years)")

    # Summary by year
    print("\n" + "="*80)
    print(f"SUMMARY BY YEAR ({sector_label})")
    print("="*80)

    cool_by_year = cool_demand.groupby('year')['E_cool_mean_EJ'].sum()
    heat_by_year = heat_demand.groupby('year')['E_heat_mean_EJ'].sum()

    summary = pd.DataFrame({
        'year': cool_by_year.index,
        'cooling_EJ': cool_by_year.values,
        'heating_EJ': heat_by_year.values,
        'total_EJ': cool_by_year.values + heat_by_year.values
    })

    print(summary.to_string(index=False))

    print("\n" + "="*80)
    print("DONE")
    print("="*80)


if __name__ == "__main__":
    # Usage: python energy_demand.py [SSP] [scenario] [sector]
    # sector: 'resid' (default), 'comm', or 'both'
    ssp = sys.argv[1] if len(sys.argv) > 1 else "SSP2"
    scenario = sys.argv[2] if len(sys.argv) > 2 else "S1"
    sector_arg = sys.argv[3] if len(sys.argv) > 3 else "resid"
    if sector_arg == 'both':
        for s in ['resid', 'comm']:
            main(ssp=ssp, scenario=scenario, sector=s)
    else:
        main(ssp=ssp, scenario=scenario, sector=sector_arg)
