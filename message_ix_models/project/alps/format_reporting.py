"""Format emissions reporting output for MAGICC compatibility.

Extracts key functionality from prep_submission.py without requiring message_data:
1. Interpolate emissions for missing years (2021-2024)
2. Standardize variable names (NOX → NOx, SO2 → Sulfur)
3. Create emission aggregations

Usage:
    uv run --no-sync python format_reporting.py --file SSP_SSP2_v6.4_baseline.xlsx
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path

from message_ix_models.util import package_data_path


STATIC_INDEX = ["Model", "Scenario", "Region", "Variable", "Unit"]


def interpolate_emissions(df):
    """
    Interpolate emissions for years 2021-2024.

    MAGICC expects continuous time series. Our reporting often has 5-year intervals
    (2020, 2025, 2030...). This fills the gaps with linear interpolation.

    Args:
        df: DataFrame with emissions data

    Returns:
        DataFrame with interpolated values for 2021-2024
    """
    print("  Interpolating emissions from 2020 to 2025...")

    add_years = [2021, 2022, 2023, 2024]

    # Separate emission variables from others
    emission_vars = df[df['Variable'].str.startswith('Emission', na=False)]
    other_vars = df[~df['Variable'].str.startswith('Emission', na=False)]

    if len(emission_vars) == 0:
        print("    No emission variables found - skipping interpolation")
        return df

    # Set index for emissions
    emission_indexed = emission_vars.set_index(STATIC_INDEX).copy()

    # Add missing years with NaN
    for year in add_years:
        if year not in emission_indexed.columns:
            emission_indexed[year] = np.nan

    # Sort columns by year and interpolate
    year_cols = sorted([col for col in emission_indexed.columns if isinstance(col, (int, str)) and str(col).isdigit()], key=lambda x: int(x))
    emission_indexed = emission_indexed[year_cols]

    # Interpolate along the time axis
    emission_interpolated = emission_indexed.interpolate(axis=1, method='linear')

    # Reset index
    emission_interpolated = emission_interpolated.reset_index()

    # Combine with other variables
    result = pd.concat([emission_interpolated, other_vars], ignore_index=True)
    result = result.sort_values(by=STATIC_INDEX)

    n_interpolated = len(emission_vars) * len(add_years)
    print(f"    Interpolated {n_interpolated} emission values for years {add_years}")

    return result


def standardize_variable_names(df):
    """
    Standardize variable naming conventions.

    Different databases use different conventions. This standardizes to IAMC:
    - NOX → NOx
    - SO2 → Sulfur
    - PM_BC → BC
    - PM_OC → OC

    Args:
        df: DataFrame with variable names

    Returns:
        DataFrame with standardized variable names
    """
    print("  Standardizing variable names...")

    replacements = {
        'NOX': 'NOx',
        'SO2': 'Sulfur',
        'PM_BC': 'BC',
        'PM_OC': 'OC',
    }

    changes = 0
    for old, new in replacements.items():
        mask = df['Variable'].str.contains(old, case=True, na=False, regex=False)
        if mask.any():
            df.loc[mask, 'Variable'] = df.loc[mask, 'Variable'].str.replace(old, new, regex=False)
            changes += mask.sum()

    print(f"    Standardized {changes} variable names")

    return df


def create_emission_aggregates(df, species):
    """
    Create hierarchical emission aggregates for a species.

    Ensures parent categories are properly summed from children.
    E.g., Emissions|CO2|Energy = Emissions|CO2|Energy|Demand + Emissions|CO2|Energy|Supply

    Args:
        df: DataFrame with emissions
        species: Emission species (e.g., 'CO2', 'CH4', 'NOx')

    Returns:
        DataFrame with aggregates added
    """
    # Define aggregation hierarchy
    aggregates = {
        f"Emissions|{species}|Energy|Demand|Bunkers": [
            f"Emissions|{species}|Energy|Demand|Bunkers|International Aviation",
            f"Emissions|{species}|Energy|Demand|Bunkers|International Shipping",
        ],
        f"Emissions|{species}|Energy|Demand": [
            f"Emissions|{species}|Energy|Demand|Industry",
            f"Emissions|{species}|Energy|Demand|Residential and Commercial",
            f"Emissions|{species}|Energy|Demand|Transportation",
            f"Emissions|{species}|Energy|Demand|Bunkers",
        ],
        f"Emissions|{species}|Energy": [
            f"Emissions|{species}|Energy|Demand",
            f"Emissions|{species}|Energy|Supply",
        ],
        f"Emissions|{species}|Fossil Fuels and Industry": [
            f"Emissions|{species}|Energy",
            f"Emissions|{species}|Industrial Processes",
        ],
        f"Emissions|{species}|AFOLU": [
            f"Emissions|{species}|AFOLU|Agricultural Waste Burning",
            f"Emissions|{species}|AFOLU|Land",
            f"Emissions|{species}|AFOLU|Agriculture",
        ],
        f"Emissions|{species}": [
            f"Emissions|{species}|AFOLU",
            f"Emissions|{species}|Energy",
            f"Emissions|{species}|Industrial Processes",
            f"Emissions|{species}|Waste",
        ],
    }

    df_indexed = df.set_index(STATIC_INDEX)
    updates = []

    for parent, children in aggregates.items():
        # Find children that exist in the data
        existing_children = [child for child in children if child in df['Variable'].values]

        if not existing_children:
            continue

        # Get child data
        child_data = df[df['Variable'].isin(existing_children)].copy()

        if len(child_data) == 0:
            continue

        # Sum children by Model/Scenario/Region
        child_data['Variable'] = parent

        # Determine unit (should be same for all children)
        units = child_data['Unit'].unique()
        if len(units) > 1:
            print(f"    Warning: Multiple units for {parent}: {units}")
            continue

        aggregated = child_data.groupby(STATIC_INDEX).sum(numeric_only=True).reset_index()
        updates.append(aggregated)

    if updates:
        df_with_updates = pd.concat([df] + updates, ignore_index=True)
        # Remove duplicates, keeping the last (aggregated) version
        df_with_updates = df_with_updates.drop_duplicates(subset=STATIC_INDEX, keep='last')
        return df_with_updates

    return df


def format_reporting_file(input_file: Path, output_file: Path = None, backup: bool = True):
    """
    Format reporting file for MAGICC compatibility.

    Args:
        input_file: Path to input Excel file
        output_file: Path to output file (default: overwrites input with backup)
        backup: Create .backup file before overwriting
    """
    print(f"Reading {input_file.name}...")
    df = pd.read_excel(input_file, sheet_name='data')

    print(f"Original data: {len(df)} rows")

    # Step 1: Interpolate emissions
    df = interpolate_emissions(df)

    # Step 2: Standardize variable names
    df = standardize_variable_names(df)

    # Step 3: Create aggregates for key species
    print("  Creating emission aggregates...")
    key_species = ['CO2', 'CH4', 'N2O', 'NOx', 'Sulfur', 'BC', 'OC', 'CO', 'VOC']
    for species in key_species:
        species_vars = df[df['Variable'].str.startswith(f'Emissions|{species}', na=False)]
        if len(species_vars) > 0:
            df = create_emission_aggregates(df, species)

    print(f"Final data: {len(df)} rows")

    # Handle output
    if output_file is None:
        output_file = input_file
        if backup:
            backup_file = input_file.with_suffix('.pre-format.xlsx')
            print(f"\nCreating backup: {backup_file.name}")
            import shutil
            shutil.copy2(input_file, backup_file)

    print(f"\nSaving formatted file to {output_file.name}...")
    df.to_excel(output_file, sheet_name='data', index=False)

    print("✓ Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Format emissions reporting for MAGICC compatibility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Format SSP baseline file
  uv run --no-sync python format_reporting.py --file SSP_SSP2_v6.4_baseline.xlsx

  # Format without backup
  uv run --no-sync python format_reporting.py --file SSP_SSP2_v6.4_baseline.xlsx --no-backup

  # Format and save to new file
  uv run --no-sync python format_reporting.py --file input.xlsx --output formatted.xlsx
        """
    )

    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Emissions file to format (filename only, looked up in reporting_output/)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output filename (optional, defaults to overwriting input with backup)'
    )

    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create backup file when overwriting'
    )

    args = parser.parse_args()

    print("="*70)
    print("FORMAT EMISSIONS REPORTING FOR MAGICC")
    print("="*70)

    # Locate files using package_data_path
    reporting_dir = package_data_path("report", "legacy", "reporting_output")
    input_file = reporting_dir / args.file

    if not input_file.exists():
        print(f"\n✗ ERROR: File not found: {input_file}")
        print(f"\nAvailable files in {reporting_dir}:")
        for f in reporting_dir.glob("*.xlsx"):
            print(f"  - {f.name}")
        return 1

    output_file = None
    if args.output:
        output_file = reporting_dir / args.output

    try:
        format_reporting_file(
            input_file,
            output_file=output_file,
            backup=not args.no_backup
        )

        print("\n" + "="*70)
        print("SUCCESS - Emissions file formatted!")
        print("="*70)
        print("\nYou can now run MAGICC on the formatted file:")
        print(f"  uv run --no-sync python run_magicc_climate.py --scenario {args.file.replace('.xlsx', '')}")

        return 0

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
