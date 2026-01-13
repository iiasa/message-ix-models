"""Fix NOx unit issues in emissions reporting output for MAGICC compatibility.

Handles three scenarios:
1. Unit mismatch: Mt NO2/yr values are actually in kt (1000x too large)
   → Detects magnitude mismatch and converts to proper Mt
2. Split coverage: Mt NOx/yr for early years, Mt NO2/yr for later years
   → Merges into single complete Mt NO2/yr series
3. Single unit: Only Mt NOx/yr exists
   → Creates duplicate rows with Mt NO2/yr for harmonization

MAGICC's historical database uses Mt NO2/yr. The silicone infiller fails with
AssertionError when rows have incomplete year coverage (NaN values).

Usage:
    uv run --no-sync python fix_nox_units.py --file SSP_SSP2_v6.5_CID_baseline_3500f.xlsx
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path

from message_ix_models.util import package_data_path


STATIC_INDEX = ["Model", "Scenario", "Region", "Variable", "Unit"]


def _get_year_columns(df):
    """Extract numeric year columns from dataframe."""
    return [col for col in df.columns if isinstance(col, (int, float)) or
            (isinstance(col, str) and col.isdigit())]


def _fix_no2_unit_magnitude(df):
    """
    Fix Mt NO2/yr values that are actually in kt (1000x too large).

    The unicc reporting sometimes has Mt NO2/yr values that are ~1000x larger
    than Mt NOx/yr values for the same variable/region. This indicates the
    NO2 values are actually in kt, not Mt.

    Detection: If median(NO2 values) / median(NOx values) > 100, convert.

    Returns:
        tuple: (df, n_fixed)
    """
    year_cols = _get_year_columns(df)
    if not year_cols:
        return df, 0

    nox_no2_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                   (df['Unit'] == 'Mt NO2/yr')
    nox_nox_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                   (df['Unit'] == 'Mt NOx/yr')

    if not nox_no2_mask.any() or not nox_nox_mask.any():
        return df, 0

    no2_rows = df[nox_no2_mask]
    nox_rows = df[nox_nox_mask]

    n_fixed = 0

    # Check each NO2 row for magnitude mismatch with corresponding NOx row
    for idx in no2_rows.index:
        no2_row = df.loc[idx]

        # Find matching NOx row
        matching = nox_rows[
            (nox_rows['Model'] == no2_row['Model']) &
            (nox_rows['Scenario'] == no2_row['Scenario']) &
            (nox_rows['Region'] == no2_row['Region']) &
            (nox_rows['Variable'] == no2_row['Variable'])
        ]

        if len(matching) == 0:
            continue

        nox_row = matching.iloc[0]

        # Get non-NaN values for comparison
        no2_vals = no2_row[year_cols].dropna().values
        nox_vals = nox_row[year_cols].dropna().values

        if len(no2_vals) == 0 or len(nox_vals) == 0:
            continue

        # Compare medians
        no2_median = np.median(no2_vals)
        nox_median = np.median(nox_vals)

        if nox_median == 0:
            continue

        ratio = no2_median / nox_median

        # If NO2 values are ~1000x larger, they're likely in kt not Mt
        if ratio > 100:
            # Convert from kt to Mt (divide by 1000)
            for col in year_cols:
                if not pd.isna(df.loc[idx, col]):
                    df.loc[idx, col] = df.loc[idx, col] / 1000.0
            n_fixed += 1

    return df, n_fixed


def _merge_split_nox_coverage(df):
    """
    Merge split NOx coverage where Mt NOx/yr and Mt NO2/yr have different year ranges.

    The unicc reporting output sometimes has:
    - Mt NOx/yr with data for early years (1990-2025)
    - Mt NO2/yr with data for later years (2030-2100)

    This merges them into a single complete Mt NO2/yr series and removes
    the incomplete Mt NOx/yr rows.

    Returns:
        tuple: (df, n_merged, n_removed)
    """
    year_cols = _get_year_columns(df)
    if not year_cols:
        return df, 0, 0

    nox_no2_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                   (df['Unit'] == 'Mt NO2/yr')
    nox_nox_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                   (df['Unit'] == 'Mt NOx/yr')

    if not nox_no2_mask.any() or not nox_nox_mask.any():
        return df, 0, 0

    no2_rows = df[nox_no2_mask].copy()
    nox_rows = df[nox_nox_mask].copy()

    n_merged = 0
    rows_to_remove = []

    for _, no2_row in no2_rows.iterrows():
        key_match = (
            (nox_rows['Model'] == no2_row['Model']) &
            (nox_rows['Scenario'] == no2_row['Scenario']) &
            (nox_rows['Region'] == no2_row['Region']) &
            (nox_rows['Variable'] == no2_row['Variable'])
        )

        matching_nox = nox_rows[key_match]

        if len(matching_nox) == 0:
            continue

        nox_row = matching_nox.iloc[0]

        no2_values = no2_row[year_cols]
        nox_values = nox_row[year_cols]

        no2_valid = ~pd.isna(no2_values)
        nox_valid = ~pd.isna(nox_values)

        # If NOx has values where NO2 doesn't, merge them
        nox_only_years = nox_valid & ~no2_valid

        if nox_only_years.any():
            no2_idx = df[
                (df['Model'] == no2_row['Model']) &
                (df['Scenario'] == no2_row['Scenario']) &
                (df['Region'] == no2_row['Region']) &
                (df['Variable'] == no2_row['Variable']) &
                (df['Unit'] == 'Mt NO2/yr')
            ].index

            if len(no2_idx) > 0:
                for year in year_cols:
                    if nox_only_years[year]:
                        df.loc[no2_idx[0], year] = nox_values[year]
                n_merged += 1

        # Mark the NOx row for removal
        nox_idx = df[
            (df['Model'] == nox_row['Model']) &
            (df['Scenario'] == nox_row['Scenario']) &
            (df['Region'] == nox_row['Region']) &
            (df['Variable'] == nox_row['Variable']) &
            (df['Unit'] == 'Mt NOx/yr')
        ].index
        rows_to_remove.extend(nox_idx.tolist())

    if rows_to_remove:
        df = df.drop(index=rows_to_remove)

    return df, n_merged, len(rows_to_remove)


def _add_no2_duplicates(df):
    """
    Add NO2-equivalent rows for NOx emissions that only have Mt NOx/yr.

    Returns:
        tuple: (df, n_added)
    """
    nox_nox_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                   (df['Unit'] == 'Mt NOx/yr')

    if not nox_nox_mask.any():
        return df, 0

    nox_rows = df[nox_nox_mask].copy()
    rows_to_duplicate = []

    for idx, row in nox_rows.iterrows():
        has_no2 = (
            (df['Model'] == row['Model']) &
            (df['Scenario'] == row['Scenario']) &
            (df['Region'] == row['Region']) &
            (df['Variable'] == row['Variable']) &
            (df['Unit'] == 'Mt NO2/yr')
        ).any()

        if not has_no2:
            rows_to_duplicate.append(idx)

    if not rows_to_duplicate:
        return df, 0

    duplicates = df.loc[rows_to_duplicate].copy()
    duplicates['Unit'] = 'Mt NO2/yr'

    df = pd.concat([df, duplicates], ignore_index=True)

    return df, len(rows_to_duplicate)


def fix_nox_units(input_file: Path, output_file: Path = None, backup: bool = True):
    """
    Comprehensively fix NOx unit issues for MAGICC compatibility.

    Handles:
    1. Unit magnitude: Converts kt→Mt when NO2 values are ~1000x too large
    2. Split coverage: Merges Mt NOx/yr (early) + Mt NO2/yr (later)
    3. Missing NO2: Adds Mt NO2/yr duplicates for rows with only Mt NOx/yr

    Args:
        input_file: Path to input Excel file
        output_file: Path to output file (default: overwrites input with backup)
        backup: Create .backup file before overwriting (default: True)

    Returns:
        bool: True if any changes were made
    """
    print(f"Reading {input_file.name}...")
    df = pd.read_excel(input_file, sheet_name='data')

    original_rows = len(df)
    print(f"Original data: {original_rows} rows")

    # Diagnose NOx situation
    nox_no2 = df[(df['Variable'].str.startswith('Emissions|NOx', na=False)) &
                 (df['Unit'] == 'Mt NO2/yr')]
    nox_nox = df[(df['Variable'].str.startswith('Emissions|NOx', na=False)) &
                 (df['Unit'] == 'Mt NOx/yr')]

    print(f"\nNOx diagnosis:")
    print(f"  Rows with Mt NO2/yr: {len(nox_no2)}")
    print(f"  Rows with Mt NOx/yr: {len(nox_nox)}")

    changes_made = False

    # Step 1: Fix magnitude mismatch (kt labeled as Mt)
    df, n_magnitude_fixed = _fix_no2_unit_magnitude(df)
    if n_magnitude_fixed > 0:
        print(f"\n[Step 1] Fixed unit magnitude (kt→Mt conversion):")
        print(f"  Converted {n_magnitude_fixed} Mt NO2/yr rows (were actually kt)")
        changes_made = True
    else:
        print(f"\n[Step 1] No magnitude fixes needed")

    # Step 2: Merge split coverage
    df, n_merged, n_removed = _merge_split_nox_coverage(df)
    if n_merged > 0 or n_removed > 0:
        print(f"\n[Step 2] Merged split coverage:")
        print(f"  Merged {n_merged} variable-region pairs")
        print(f"  Removed {n_removed} redundant Mt NOx/yr rows")
        changes_made = True
    else:
        print(f"\n[Step 2] No split coverage to merge")

    # Step 3: Add NO2 duplicates for remaining NOx-only rows
    df, n_added = _add_no2_duplicates(df)
    if n_added > 0:
        print(f"\n[Step 3] Added NO2 duplicates:")
        print(f"  Created {n_added} new Mt NO2/yr rows")
        changes_made = True
    else:
        print(f"\n[Step 3] No NO2 duplicates needed")

    if not changes_made:
        print("\nNo changes needed - file already compatible")
        return False

    print(f"\nFinal data: {len(df)} rows (was {original_rows})")

    # Handle output
    if output_file is None:
        output_file = input_file
        if backup:
            backup_file = input_file.with_suffix('.backup.xlsx')
            print(f"\nCreating backup: {backup_file.name}")
            import shutil
            shutil.copy2(input_file, backup_file)

    print(f"\nSaving corrected file to {output_file.name}...")
    df.to_excel(output_file, sheet_name='data', index=False)

    print("Done!")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Fix NOx unit issues in emissions files for MAGICC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script fixes NOx unit issues that cause MAGICC to fail:

1. MAGNITUDE MISMATCH: unicc reporting sometimes labels kt values as Mt.
   The fix detects when NO2 values are ~1000x larger than NOx values
   and converts them to proper Mt units.

2. SPLIT COVERAGE: Some reporting outputs have NOx with:
   - Mt NOx/yr for early years (1990-2025)
   - Mt NO2/yr for later years (2030-2100)
   This causes NaN during interpolation. The fix merges into one series.

3. MISSING NO2 UNIT: MAGICC expects Mt NO2/yr for harmonization.
   If only Mt NOx/yr exists, duplicates are created.

Examples:
  uv run --no-sync python fix_nox_units.py --file SSP_SSP2_v6.5_CID_baseline_3500f.xlsx
        """
    )

    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Emissions file to fix (filename only, looked up in reporting_output/)'
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

    print("=" * 70)
    print("FIX NOx UNITS FOR MAGICC COMPATIBILITY")
    print("=" * 70)

    reporting_dir = package_data_path("report", "legacy", "reporting_output")
    input_file = reporting_dir / args.file

    if not input_file.exists():
        print(f"\nERROR: File not found: {input_file}")
        print(f"\nAvailable files in {reporting_dir}:")
        for f in sorted(reporting_dir.glob("*.xlsx")):
            print(f"  - {f.name}")
        return 1

    output_file = None
    if args.output:
        output_file = reporting_dir / args.output

    try:
        fixed = fix_nox_units(
            input_file,
            output_file=output_file,
            backup=not args.no_backup
        )

        if fixed:
            print("\n" + "=" * 70)
            print("SUCCESS - NOx issues fixed!")
            print("=" * 70)
            print("\nYou can now run MAGICC:")
            scenario = args.file.replace('.xlsx', '')
            print(f"  uv run --no-sync python run_magicc_climate.py --scenario {scenario}")

        return 0

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
