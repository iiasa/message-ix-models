"""
Compute buildings energy demand from CHILLED EI and STURM correction coefficients.

Formula: E(t,r,a) = γ(t,r,a) × C(r,a,GSAT(t)) × F(t,r,a)

Where:
- γ(t,r,a): Correction coefficient (climate-invariant, captures STURM dynamics)
- C(r,a,GSAT(t)): CHILLED energy intensity at given GSAT (from GWL-binned data)
- F(t,r,a): Floor area from STURM building stock

Temperature (GSAT) is provided dynamically by ALPS/RIME, not from static files.
"""

import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Dict, Literal, Union

# Data paths - use message_ix_models/data/alps/ directory
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "alps"
RIME_DIR = DATA_DIR / "rime_datasets"
PARAMS_FILE = DATA_DIR / "sturm_correction_params.nc"

# EI files in rime_datasets
EI_COOL_FILE = RIME_DIR / "region_EI_cool_gwl_binned.nc"
EI_HEAT_FILE = RIME_DIR / "region_EI_heat_gwl_binned.nc"


def load_data(mode: Literal["cool", "heat"] = "cool"):
    """
    Load consolidated parameters and CHILLED EI data.

    Parameters
    ----------
    mode : str
        "cool" or "heat"

    Returns
    -------
    params_ds : xr.Dataset
        Consolidated parameters with floor_Mm2, gamma_cool, gamma_heat
    ei_ds : xr.Dataset
        CHILLED energy intensity binned by GWL
    """
    # Load consolidated parameters
    params_ds = xr.open_dataset(PARAMS_FILE)

    # Load CHILLED EI
    ei_file = EI_COOL_FILE if mode == "cool" else EI_HEAT_FILE
    ei_ds = xr.open_dataset(ei_file)

    return params_ds, ei_ds


def interpolate_ei_at_gsat(
    ei_ds: xr.Dataset,
    gsat: float,
    mode: Literal["cool", "heat"] = "cool",
    statistic: str = "mean"
) -> xr.DataArray:
    """
    Interpolate energy intensity at a given GSAT value.

    Parameters
    ----------
    ei_ds : xr.Dataset
        CHILLED EI dataset with gwl dimension
    gsat : float
        Global Surface Air Temperature (°C above pre-industrial)
    mode : str
        "cool" or "heat"
    statistic : str
        Which EI statistic to use: "mean", "p10", "p50", "p90"

    Returns
    -------
    xr.DataArray
        Energy intensity at given GSAT, dimensions [region, arch, urt]
    """
    ei_var = f"EI_ac_m2" if mode == "cool" else f"EI_h_m2"
    if statistic != "mean":
        ei_var = f"{ei_var}_{statistic}"

    # Interpolate along gwl dimension
    ei_interp = ei_ds[ei_var].interp(gwl=gsat, method="linear")

    return ei_interp


def compute_energy_demand_pointwise(
    gamma: float,
    ei: float,
    floor_m2: float,
) -> float:
    """
    Compute energy demand for a single point (region, year, arch, urt).

    Parameters
    ----------
    gamma : float
        Correction coefficient (dimensionless)
    ei : float
        Energy intensity at GSAT (MJ/m²)
    floor_m2 : float
        Floor area (million m²)

    Returns
    -------
    float
        Energy demand (EJ/yr)

    Formula
    -------
    E = γ × EI × floor / 1e6
    (Divide by 1e6 to convert MJ·Mm² to EJ)
    """
    # Skip if any input is NaN
    if np.isnan(gamma) or np.isnan(ei) or np.isnan(floor_m2):
        return np.nan

    energy_ej = gamma * ei * floor_m2 / 1e6
    return energy_ej


def compute_energy_demand(
    gsat_by_year: Dict[int, float],
    mode: Literal["cool", "heat"] = "cool",
    statistic: str = "mean",
    aggregate_by: list = None,
) -> pd.DataFrame:
    """
    Compute energy demand timeseries from dynamic GSAT input.

    Parameters
    ----------
    gsat_by_year : dict
        Mapping of year to GSAT (°C above pre-industrial)
        Example: {2020: 1.2, 2025: 1.3, 2030: 1.5, ...}
    mode : str
        "cool" or "heat"
    statistic : str
        Which EI statistic to use: "mean", "p10", "p50", "p90"
    aggregate_by : list, optional
        Grouping dimensions for aggregation (e.g., ["region", "year"])
        If None, returns disaggregated results

    Returns
    -------
    pd.DataFrame
        Energy demand by region, year, arch, urt
        Columns: [region, year, arch, urt, E_{mode}_EJ, gsat]
        If aggregate_by is provided, sums over non-grouped dimensions
    """
    # Load data
    params_ds, ei_ds = load_data(mode=mode)

    gamma_var = f"gamma_{mode}"

    results = []

    # Iterate over years
    for year, gsat in gsat_by_year.items():
        # Skip if year not in params data
        if year not in params_ds.year.values:
            continue

        # Interpolate EI at this year's GSAT
        ei_at_gsat = interpolate_ei_at_gsat(ei_ds, gsat, mode=mode, statistic=statistic)

        # Get floor and gamma for this year
        floor = params_ds['floor_Mm2'].sel(year=year)
        gamma = params_ds[gamma_var].sel(year=year)

        # Iterate over all combinations
        for region in params_ds.region.values:
            for arch in params_ds.arch.values:
                # Skip if arch not in EI data
                if arch not in ei_ds.arch.values:
                    continue

                for urt in params_ds.urt.values:
                    # Extract values
                    gamma_val = gamma.sel(region=region, arch=arch, urt=urt).item()
                    ei_val = ei_at_gsat.sel(region=region, arch=arch, urt=urt).item()
                    floor_val = floor.sel(region=region, arch=arch, urt=urt).item()

                    # Compute energy demand
                    energy_ej = compute_energy_demand_pointwise(gamma_val, ei_val, floor_val)

                    # Skip NaN results
                    if np.isnan(energy_ej):
                        continue

                    results.append({
                        'region': region,
                        'year': year,
                        'arch': arch,
                        'urt': urt,
                        f'E_{mode}_EJ': energy_ej,
                        'gsat': gsat,
                    })

    df = pd.DataFrame(results)

    # Aggregate if requested
    if aggregate_by is not None:
        df = df.groupby(aggregate_by).agg({
            f'E_{mode}_EJ': 'sum',
            'gsat': 'first',  # GSAT is same for all rows in a year
        }).reset_index()

    return df


def compute_total_energy_demand(
    gsat_by_year: Dict[int, float],
    statistic: str = "mean",
    aggregate_by: list = None,
) -> pd.DataFrame:
    """
    Compute total energy demand (cooling + heating) from dynamic GSAT input.

    Parameters
    ----------
    gsat_by_year : dict
        Mapping of year to GSAT (°C above pre-industrial)
    statistic : str
        Which EI statistic to use: "mean", "p10", "p50", "p90"
    aggregate_by : list, optional
        Grouping dimensions for aggregation

    Returns
    -------
    pd.DataFrame
        Total energy demand with columns:
        [region, year, arch, urt, E_cool_EJ, E_heat_EJ, E_total_EJ, gsat]
    """
    # Compute cooling and heating
    df_cool = compute_energy_demand(gsat_by_year, mode="cool", statistic=statistic, aggregate_by=None)
    df_heat = compute_energy_demand(gsat_by_year, mode="heat", statistic=statistic, aggregate_by=None)

    # Merge on common dimensions
    merge_cols = ['region', 'year', 'arch', 'urt']
    df_total = df_cool.merge(df_heat, on=merge_cols, how='outer', suffixes=('_cool', '_heat'))

    # Use gsat from cool (should be identical)
    df_total['gsat'] = df_total['gsat_cool'].fillna(df_total['gsat_heat'])
    df_total = df_total.drop(columns=['gsat_cool', 'gsat_heat'])

    # Fill NaNs with 0 for summation
    df_total['E_cool_EJ'] = df_total['E_cool_EJ'].fillna(0)
    df_total['E_heat_EJ'] = df_total['E_heat_EJ'].fillna(0)

    # Compute total
    df_total['E_total_EJ'] = df_total['E_cool_EJ'] + df_total['E_heat_EJ']

    # Aggregate if requested
    if aggregate_by is not None:
        agg_dict = {
            'E_cool_EJ': 'sum',
            'E_heat_EJ': 'sum',
            'E_total_EJ': 'sum',
            'gsat': 'first',
        }
        df_total = df_total.groupby(aggregate_by).agg(agg_dict).reset_index()

    return df_total


if __name__ == "__main__":
    # Test with sample GSAT values
    print("Testing energy demand computation with sample GSAT values...\n")

    # Sample GSAT trajectory (1.2°C baseline to 2.5°C by 2100)
    test_gsat = {
        2020: 1.2,
        2030: 1.4,
        2050: 1.8,
        2070: 2.1,
        2100: 2.5,
    }

    print("Input GSAT trajectory:")
    for year, gsat in test_gsat.items():
        print(f"  {year}: {gsat:.2f}°C")
    print()

    # Compute cooling demand
    print("Computing cooling energy demand...")
    df_cool = compute_energy_demand(test_gsat, mode="cool", aggregate_by=["region", "year"])
    print(f"Results: {len(df_cool)} region-years")
    print("\nSample results (first 10 rows):")
    print(df_cool.head(10))
    print()

    # Compute heating demand
    print("Computing heating energy demand...")
    df_heat = compute_energy_demand(test_gsat, mode="heat", aggregate_by=["region", "year"])
    print(f"Results: {len(df_heat)} region-years")
    print()

    # Compute total
    print("Computing total energy demand (cooling + heating)...")
    df_total = compute_total_energy_demand(test_gsat, aggregate_by=["region", "year"])
    print(f"Results: {len(df_total)} region-years")
    print("\nSample results (first 10 rows):")
    print(df_total.head(10))
    print()

    # Summary statistics
    print("Summary by year:")
    summary = df_total.groupby('year').agg({
        'E_cool_EJ': 'sum',
        'E_heat_EJ': 'sum',
        'E_total_EJ': 'sum',
    }).round(2)
    print(summary)
