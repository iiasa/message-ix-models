"""Run MAGICC climate processing on MESSAGE emissions.

This script:
1. Reads emissions reporting output from legacy reporting
2. Converts regions from R12 to MAGICC format
3. Runs MAGICC climate processor with uncertainty quantification
4. Extracts temperature projections by percentile

Usage:
    uv run --no-sync python run_magicc_climate.py [--scenario SCENARIO_NAME] [--run-type {fast,medium,complete}]

Examples:
    # Run with default test scenario
    uv run --no-sync python run_magicc_climate.py

    # Run with SSP baseline
    uv run --no-sync python run_magicc_climate.py --scenario SSP_SSP2_v6.4_baseline

    # Run with full uncertainty quantification
    uv run --no-sync python run_magicc_climate.py --scenario SSP_SSP2_v6.4_baseline --run-type complete

Requires:
    - Emissions file from generate_emissions_report.py in reporting_output/
    - climate-processor package installed
"""

import argparse
from pathlib import Path

from message_ix_models.util import package_data_path
from message_ix_models.report.legacy.call_climate_processor import (
    read_magicc_output,
    run_climate_processor,
)


def main():
    parser = argparse.ArgumentParser(
        description="Run MAGICC climate processing on MESSAGE emissions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default test scenario (fast mode)
  uv run --no-sync python run_magicc_climate.py

  # Run with SSP baseline (medium mode)
  uv run --no-sync python run_magicc_climate.py --scenario SSP_SSP2_v6.4_baseline

  # Run with full uncertainty (600 configs, slow)
  uv run --no-sync python run_magicc_climate.py --scenario SSP_SSP2_v6.4_baseline --run-type complete
        """
    )

    parser.add_argument(
        '--scenario',
        type=str,
        default='MESSAGE_GLOBIOM_SSP2_v6.1_Main_baseline_baseline_nexus_7p0_high_emissions_test',
        help='Scenario name (Excel filename without .xlsx). Default: test scenario'
    )

    parser.add_argument(
        '--run-type',
        type=str,
        choices=['fast', 'medium', 'complete'],
        default='medium',
        help='MAGICC run type: fast (1 config), medium (100 configs), complete (600 configs). Default: medium'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers for MAGICC. Default: 4'
    )

    args = parser.parse_args()

    scenario_string = args.scenario

    print("="*70)
    print("MAGICC CLIMATE PROCESSING PIPELINE")
    print("="*70)

    input_dir = package_data_path("report", "legacy", "reporting_output")
    output_dir = package_data_path("report", "legacy", "reporting_output", "magicc_output")

    print(f"\nScenario: {scenario_string}")
    print(f"Run type: {args.run_type} ({1 if args.run_type == 'fast' else 100 if args.run_type == 'medium' else 600} configurations)")
    print(f"Workers: {args.workers}")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")

    # Check if input file exists
    input_file = input_dir / f"{scenario_string}.xlsx"
    if not input_file.exists():
        print(f"\n✗ ERROR: Input file not found: {input_file}")
        print(f"\nAvailable files in {input_dir}:")
        for f in input_dir.glob("*.xlsx"):
            print(f"  - {f.name}")
        return 1

    # Step 1: Run MAGICC climate processor
    print("\n" + "="*70)
    print("STEP 1: Running MAGICC Climate Processor")
    print("="*70)

    try:
        run_climate_processor(
            sc=scenario_string,
            input_dir=input_dir,
            output_dir=output_dir,
            run_type=args.run_type,
            magicc_worker_number=args.workers,
        )
        print("\n✓ MAGICC processing completed successfully!")
    except Exception as e:
        print(f"\n✗ MAGICC processing failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Step 2: Read MAGICC temperature output
    print("\n" + "="*70)
    print("STEP 2: Reading MAGICC Temperature Output")
    print("="*70)

    try:
        # Read 50th percentile (median) temperature for 2100
        temp_50th = read_magicc_output(scenario_string, pp=50, output_dir=output_dir)
        print(f"\n✓ Successfully read MAGICC output!")
        print(f"\n  Median (50th percentile) temperature in 2100: {temp_50th:.2f}°C")

        # Optionally read other percentiles
        print("\n  Reading additional percentiles...")
        temp_10th = read_magicc_output(scenario_string, pp=10, output_dir=output_dir)
        temp_90th = read_magicc_output(scenario_string, pp=90, output_dir=output_dir)

        print(f"\n  Temperature range in 2100:")
        print(f"    10th percentile: {temp_10th:.2f}°C")
        print(f"    50th percentile: {temp_50th:.2f}°C (median)")
        print(f"    90th percentile: {temp_90th:.2f}°C")

    except Exception as e:
        print(f"\n✗ Reading MAGICC output failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "="*70)
    print("SUCCESS - Full MAGICC pipeline completed!")
    print("="*70)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
