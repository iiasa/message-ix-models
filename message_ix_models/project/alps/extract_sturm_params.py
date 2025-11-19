"""
Extract region-specific STURM parameters for energy demand calculation.

Reads STURM CSV files and computes timeseries of parameter values by region.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# STURM data directory
STURM_DIR = Path("message_ix_buildings/sturm/data/input_csv_SSP_2023_resid")


def harmonize_region_names(df, region_col='region'):
    """Harmonize region names (LAC → LAM for consistency with R11 mapping)."""
    df[region_col] = df[region_col].replace({'LAC': 'LAM'})
    return df


def extract_cool_params():
    """Extract cooling parameters by region with full timeseries."""

    print("=" * 80)
    print("EXTRACTING COOLING PARAMETERS (FULL TIMESERIES)")
    print("=" * 80)

    # Load R32 → R11 region mapping
    df_region_map = pd.read_csv(STURM_DIR / "regions_R61.csv")
    region_map = dict(zip(df_region_map['region_bld'], df_region_map['R11']))
    print(f"\nLoaded region mapping: {len(region_map)} countries → R11 regions")

    # 1. AC efficiency (eff_cool) - already by region/year
    print("\n1. AC Efficiency (eff_cool):")
    df_eff = pd.read_csv(STURM_DIR / "eff_cool_ssp2.csv")
    eff_cool = df_eff[['region_gea', 'year', 'value']].copy()
    eff_cool.columns = ['region', 'year', 'eff']
    eff_cool = harmonize_region_names(eff_cool)
    print(f"  Shape: {eff_cool.shape}")
    print(f"  Years: {eff_cool['year'].min()} - {eff_cool['year'].max()}")
    print(f"  Regions: {sorted(eff_cool['region'].unique())}")

    # 2. Cooling hours (f_hours) - static by region
    print("\n2. Cooling Hours (f_hours = hours/24):")
    df_hours = pd.read_csv(STURM_DIR / "cool_hours.csv")
    df_hours['f_hours'] = df_hours['value'] / 24.0  # Convert to fraction
    df_hours = harmonize_region_names(df_hours, region_col='region_gea')
    # Broadcast to all years
    years = sorted(df_eff['year'].unique())
    f_hours_list = []
    for _, row in df_hours.iterrows():
        for year in years:
            f_hours_list.append({
                'region': row['region_gea'],
                'year': year,
                'f_hours': row['f_hours']
            })
    f_hours = pd.DataFrame(f_hours_list)
    print(f"  Shape: {f_hours.shape}")
    print(f"  Note: Static values broadcast to all years")

    # 3. Floor area share cooled (shr_floor) - static by region
    print("\n3. Share of Floor Cooled (shr_floor):")
    df_floor = pd.read_csv(STURM_DIR / "cool_area_share.csv")
    df_floor = harmonize_region_names(df_floor, region_col='region_gea')
    # Broadcast to all years
    shr_floor_list = []
    for _, row in df_floor.iterrows():
        for year in years:
            shr_floor_list.append({
                'region': row['region_gea'],
                'year': year,
                'shr_floor': row['value']
            })
    shr_floor = pd.DataFrame(shr_floor_list)
    print(f"  Shape: {shr_floor.shape}")
    print(f"  Note: Static values broadcast to all years")

    # 4. AC access/penetration (shr_acc) - aggregate R32 countries to R11 regions
    print("\n4. AC Access/Penetration (shr_acc) - aggregate R32 → R11:")
    df_acc = pd.read_csv(STURM_DIR / "bld_shr_access_cool_resid_ssp2_rev.csv")

    # Map R32 country codes to R11 regions
    df_acc['region'] = df_acc['region_bld'].map(region_map)

    # Check coverage before aggregation
    n_total = len(df_acc)
    n_mapped = df_acc['region'].notna().sum()
    print(f"  Mapped {n_mapped}/{n_total} rows to R11 regions ({100*n_mapped/n_total:.1f}%)")

    # Drop unmapped rows (should be few/none)
    df_acc = df_acc.dropna(subset=['region'])

    # Aggregate from R32 country-level to R11 region-level (average across countries/climate/urt/income)
    shr_acc = df_acc.groupby(['region', 'year'])['value'].mean().reset_index()
    shr_acc.columns = ['region', 'year', 'shr_acc']
    print(f"  Shape after aggregation: {shr_acc.shape}")
    print(f"  Years: {shr_acc['year'].min()} - {shr_acc['year'].max()}")
    print(f"  R11 regions with data: {sorted(shr_acc['region'].unique())}")

    # 5. Renovation savings (en_sav_ren) - set to 0 (baseline) for all region/year
    print("\n5. Renovation Savings (en_sav_ren): 0.0 for all regions/years (baseline)")

    # Merge all cooling parameters
    params_cool = eff_cool.merge(f_hours, on=['region', 'year'])
    params_cool = params_cool.merge(shr_floor, on=['region', 'year'])
    params_cool = params_cool.merge(shr_acc, on=['region', 'year'])
    params_cool['en_sav_ren'] = 0.0

    # Reorder columns
    params_cool = params_cool[['region', 'year', 'en_sav_ren', 'shr_acc', 'eff', 'shr_floor', 'f_hours']]

    print("\n" + "=" * 80)
    print("CONSOLIDATED COOLING PARAMETERS:")
    print("=" * 80)
    print(f"Shape: {params_cool.shape}")
    print(f"\nSample (year=2020):")
    print(params_cool[params_cool['year'] == 2020].to_string(index=False))

    return params_cool


def extract_heat_params():
    """Extract heating parameters by region with full timeseries."""

    print("\n\n" + "=" * 80)
    print("EXTRACTING HEATING PARAMETERS (FULL TIMESERIES)")
    print("=" * 80)

    # 1. Heating efficiency (eff_heat) - by region/fuel/year, average across fuels
    print("\n1. Heating Efficiency (eff_heat) - average across fuel types:")
    df_eff = pd.read_csv(STURM_DIR / "eff_heat_ssp2.csv")

    # Average across fuel types per region/year (simple mean)
    eff_heat = df_eff.groupby(['region_gea', 'year'])['value'].mean().reset_index()
    eff_heat.columns = ['region', 'year', 'eff']
    print(f"  Shape: {eff_heat.shape}")
    print(f"  Years: {eff_heat['year'].min()} - {eff_heat['year'].max()}")

    # 2. Share of floor heated (shr_floor) - static by region
    print("\n2. Share of Floor Heated (shr_floor):")
    df_floor = pd.read_csv(STURM_DIR / "heat_floor_resid.csv")
    # Broadcast to all years
    years = sorted(df_eff['year'].unique())
    shr_floor_list = []
    for _, row in df_floor.iterrows():
        for year in years:
            shr_floor_list.append({
                'region': row['region_gea'],
                'year': year,
                'shr_floor': row['value']
            })
    shr_floor = pd.DataFrame(shr_floor_list)
    print(f"  Shape: {shr_floor.shape}")
    print(f"  Note: Static values broadcast to all years")

    # 3. Heating hours (f_hours) - by region/climate/year, average across climate zones
    print("\n3. Heating Hours (f_hours) - average across climate zones:")
    df_hours = pd.read_csv(STURM_DIR / "heat_operation_hours_ssp2.csv")

    # Average across climate zones per region/year
    f_hours = df_hours.groupby(['region_gea', 'year'])['value'].mean().reset_index()

    # Check if values are > 1 (if so, might be hours/day, need to divide by 24)
    if f_hours['value'].max() > 1:
        print(f"  Note: Heat hours appear to be in absolute hours (max={f_hours['value'].max():.2f})")
        print(f"  Interpreting as hours/day, converting to fraction...")
        f_hours['f_hours'] = f_hours['value'] / 24.0
    else:
        f_hours['f_hours'] = f_hours['value']

    f_hours = f_hours[['region_gea', 'year', 'f_hours']]
    f_hours.columns = ['region', 'year', 'f_hours']
    print(f"  Shape: {f_hours.shape}")
    print(f"  Years: {f_hours['year'].min()} - {f_hours['year'].max()}")

    # 4. Heating access (shr_acc) - assume 100% (universal heating access)
    print("\n4. Heating Access (shr_acc): 1.0 for all regions/years (assumed universal)")

    # 5. Renovation savings (en_sav_ren) - set to 0 (baseline)
    print("\n5. Renovation Savings (en_sav_ren): 0.0 for all regions/years (baseline)")

    # Merge all heating parameters
    params_heat = eff_heat.merge(f_hours, on=['region', 'year'])
    params_heat = params_heat.merge(shr_floor, on=['region', 'year'])
    params_heat['shr_acc'] = 1.0
    params_heat['en_sav_ren'] = 0.0

    # Reorder columns
    params_heat = params_heat[['region', 'year', 'en_sav_ren', 'shr_acc', 'eff', 'shr_floor', 'f_hours']]

    print("\n" + "=" * 80)
    print("CONSOLIDATED HEATING PARAMETERS:")
    print("=" * 80)
    print(f"Shape: {params_heat.shape}")
    print(f"\nSample (year=2020):")
    print(params_heat[params_heat['year'] == 2020].to_string(index=False))

    return params_heat


def save_params(params_cool, params_heat, output_dir="."):
    """Save parameters to CSV files."""

    output_dir = Path(output_dir)

    cool_file = output_dir / "sturm_params_cool_timeseries.csv"
    heat_file = output_dir / "sturm_params_heat_timeseries.csv"

    params_cool.to_csv(cool_file, index=False)
    params_heat.to_csv(heat_file, index=False)

    print("\n" + "=" * 80)
    print("SAVED PARAMETERS:")
    print("=" * 80)
    print(f"Cooling: {cool_file}")
    print(f"  Shape: {params_cool.shape}")
    print(f"  Columns: {list(params_cool.columns)}")
    print(f"\nHeating: {heat_file}")
    print(f"  Shape: {params_heat.shape}")
    print(f"  Columns: {list(params_heat.columns)}")


if __name__ == "__main__":
    # Extract parameters
    params_cool = extract_cool_params()
    params_heat = extract_heat_params()

    # Save to CSV
    save_params(params_cool, params_heat)

    print("\n" + "=" * 80)
    print("EXTRACTION COMPLETE")
    print("=" * 80)
