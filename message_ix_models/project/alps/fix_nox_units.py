"""Fix NOx unit issue in emissions reporting output.

The SSP baseline file has NOx emissions with units "Mt NO2/yr" which should be "Mt NOx/yr".
In IAMC convention, NOx is typically reported as NO2-equivalent mass, so we just need
to change the unit string without modifying values.

Usage:
    uv run --no-sync python fix_nox_units.py [--file FILENAME]
"""

import argparse
import pandas as pd
from pathlib import Path

from message_ix_models.util import package_data_path


def fix_nox_units(input_file: Path, output_file: Path = None, backup: bool = True):
    """
    Add NO2-equivalent rows for NOx emissions to enable MAGICC harmonization.

    MAGICC's historical database uses Mt NO2/yr but our scenarios use Mt NOx/yr.
    This creates duplicate rows with NO2 units for harmonization compatibility.

    Args:
        input_file: Path to input Excel file
        output_file: Path to output file (default: overwrites input with backup)
        backup: Create .backup file before overwriting (default: True)
    """
    print(f"Reading {input_file.name}...")
    df = pd.read_excel(input_file, sheet_name='data')

    print(f"Original data: {len(df)} rows")

    # Find NOx emission variables (not energy/technology variables)
    nox_emissions_mask = (df['Variable'].str.startswith('Emissions|NOx', na=False)) & \
                         (df['Unit'] == 'Mt NOx/yr')

    n_to_duplicate = nox_emissions_mask.sum()

    if n_to_duplicate == 0:
        print("✓ No NOx emissions found or already has NO2 variants!")
        return False

    print(f"\nFound {n_to_duplicate} NOx emission rows to duplicate with NO2 units")

    # Create duplicate rows with NO2 units
    # NOx to NO2 molecular weight ratio: 46/30 ≈ 1.533
    # But for IAMC harmonization, we keep same values and just change unit string
    nox_rows = df[nox_emissions_mask].copy()
    nox_rows['Unit'] = 'Mt NO2/yr'

    # Combine original and duplicated rows
    df_combined = pd.concat([df, nox_rows], ignore_index=True)

    print(f"\n✓ Added {n_to_duplicate} duplicate rows with 'Mt NO2/yr' units")
    print(f"  Total rows: {len(df)} → {len(df_combined)}")

    df = df_combined

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

    print("✓ Done!")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Fix NOx unit issue in emissions files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fix SSP baseline file (creates backup)
  uv run --no-sync python fix_nox_units.py --file SSP_SSP2_v6.4_baseline.xlsx

  # Fix without backup
  uv run --no-sync python fix_nox_units.py --file SSP_SSP2_v6.4_baseline.xlsx --no-backup

  # Fix and save to new file
  uv run --no-sync python fix_nox_units.py --file input.xlsx --output fixed.xlsx
        """
    )

    parser.add_argument(
        '--file',
        type=str,
        default='SSP_SSP2_v6.4_baseline.xlsx',
        help='Emissions file to fix (filename only, looked up in reporting_output/). Default: SSP_SSP2_v6.4_baseline.xlsx'
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
    print("FIX NOx UNITS IN EMISSIONS FILE")
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
        fixed = fix_nox_units(
            input_file,
            output_file=output_file,
            backup=not args.no_backup
        )

        if fixed:
            print("\n" + "="*70)
            print("SUCCESS - NOx units corrected!")
            print("="*70)
            print("\nYou can now run MAGICC on the corrected file:")
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
