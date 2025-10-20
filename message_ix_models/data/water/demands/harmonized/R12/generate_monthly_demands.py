"""
Generate monthly water demand data for R12 regions using heuristic seasonal patterns.

This script creates ssp2_m_water_demands.csv by distributing annual demand data
across months using simple seasonal variation factors. The water module will
interpolate intermediate years automatically (2015, 2025, 2035, 2045, 2055).

Heuristic approach:
- Urban/rural demands: uniform across months (no strong seasonal pattern)
- Industrial demands: slight summer peak (10% higher in summer months)
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Define seasonal factors for monthly distribution
# These are multiplicative factors that sum to 12 (average = 1.0)
SEASONAL_FACTORS = {
    'urban': {  # Urban demands relatively constant
        'withdrawal': [1.0] * 12,
        'return': [1.0] * 12,
    },
    'rural': {  # Rural demands relatively constant
        'withdrawal': [1.0] * 12,
        'return': [1.0] * 12,
    },
    'industry': {  # Industrial with slight summer peak
        'withdrawal': [0.95, 0.95, 1.0, 1.0, 1.05, 1.1, 1.1, 1.1, 1.05, 1.0, 0.95, 0.9],
        'return': [0.95, 0.95, 1.0, 1.0, 1.05, 1.1, 1.1, 1.1, 1.05, 1.0, 0.95, 0.9],
    },
}

# Normalize to ensure factors sum to 12
for sector in SEASONAL_FACTORS:
    for typ in SEASONAL_FACTORS[sector]:
        factors = SEASONAL_FACTORS[sector][typ]
        total = sum(factors)
        SEASONAL_FACTORS[sector][typ] = [f * 12 / total for f in factors]


def load_annual_data(base_path):
    """Load annual demand data from CSV files."""
    data_files = {
        ('urban', 'withdrawal'): 'ssp2_regional_urban_withdrawal2_baseline.csv',
        ('urban', 'return'): 'ssp2_regional_urban_return2_baseline.csv',
        ('rural', 'withdrawal'): 'ssp2_regional_rural_withdrawal_baseline.csv',
        ('rural', 'return'): 'ssp2_regional_rural_return_baseline.csv',
        ('industry', 'withdrawal'): 'ssp2_regional_manufacturing_withdrawal_baseline.csv',
        ('industry', 'return'): 'ssp2_regional_manufacturing_return_baseline.csv',
    }

    annual_data = {}
    for (sector, typ), filename in data_files.items():
        filepath = base_path / filename
        df = pd.read_csv(filepath)
        df.rename(columns={'Unnamed: 0': 'year'}, inplace=True)
        df.set_index('year', inplace=True)
        annual_data[(sector, typ)] = df

    return annual_data


def get_basin_ids(base_path):
    """Get basin IDs from delineation file."""
    # Go up from demands/harmonized/R12 to data/water, then to delineation
    delineation_file = Path(base_path).parent.parent.parent / 'delineation' / 'basins_by_region_simpl_R12.csv'
    df = pd.read_csv(delineation_file)
    # Extract BCU_name format: "X|REGION"
    basin_ids = df['BCU_name'].tolist()
    return basin_ids


def generate_monthly_data(annual_data, basin_ids, years, output_path):
    """Generate monthly demand data from annual data.

    The water module (demands.py:210) will interpolate intermediate years
    (2015, 2025, 2035, 2045, 2055) automatically using xarray.interp().
    We only generate monthly data for the source years.
    """

    records = []

    for (sector, typ), annual_df in annual_data.items():
        print(f"Processing {sector} {typ}...")

        # Get seasonal factors for this sector/type
        factors = SEASONAL_FACTORS[sector][typ]

        for basin_id in basin_ids:
            # Get annual values for this basin
            if basin_id not in annual_df.columns:
                print(f"  Warning: {basin_id} not in {sector} {typ} data, skipping")
                continue

            for year in years:
                if year not in annual_df.index:
                    continue

                # Annual value in MCM/year
                annual_value = annual_df.loc[year, basin_id]

                # Convert to daily value then apply monthly factors
                # Annual MCM / 365 days = daily MCM
                daily_avg = annual_value / 365.0

                for month in range(1, 13):
                    # Apply seasonal factor
                    monthly_daily = daily_avg * factors[month - 1]

                    records.append({
                        'scenario': 'SSP2',
                        'sector': sector,
                        'type': typ,
                        'pid': basin_id,
                        'year': year,
                        'month': str(month),
                        'value': monthly_daily,
                        'units': 'mcm_per_day'
                    })

    # Create DataFrame and save
    df = pd.DataFrame(records)
    df = df.sort_values(['sector', 'type', 'pid', 'year', 'month'])
    df.to_csv(output_path, index=False)
    print(f"\nGenerated monthly demand data: {output_path}")
    print(f"Total records: {len(df)}")
    print(f"Sectors: {df['sector'].unique()}")
    print(f"Types: {df['type'].unique()}")
    print(f"Years: {sorted(df['year'].unique())}")
    print(f"Basins: {len(df['pid'].unique())}")

    return df


def main():
    """Main execution."""
    # Paths
    base_path = Path(__file__).parent
    output_file = base_path / 'ssp2_m_water_demands.csv'

    # Years from source data (10-year steps)
    # Water module will interpolate 2015, 2025, 2035, 2045, 2055 automatically
    years = [2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110]

    print("=" * 80)
    print("Generating Monthly Water Demand Data for R12")
    print("=" * 80)
    print("\nNote: Water module will interpolate intermediate years automatically")
    print("Source years: 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110")
    print("Interpolated: 2015, 2025, 2035, 2045, 2055")

    # Load annual data
    print("\nLoading annual demand data...")
    annual_data = load_annual_data(base_path)
    print(f"Loaded {len(annual_data)} sector-type combinations")

    # Get basin IDs
    print("\nLoading basin IDs...")
    basin_ids = get_basin_ids(base_path)
    print(f"Found {len(basin_ids)} basins")
    print(f"Sample basins: {basin_ids[:5]}")

    # Generate monthly data
    print("\nGenerating monthly data...")
    df = generate_monthly_data(annual_data, basin_ids, years, output_file)

    # Show sample
    print("\nSample output:")
    print(df.head(20))

    print("\n" + "=" * 80)
    print("Done!")
    print("=" * 80)


if __name__ == "__main__":
    main()
