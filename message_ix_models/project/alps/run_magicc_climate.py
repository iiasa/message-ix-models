"""Run MAGICC climate processing on MESSAGE emissions.

This script:
1. Reads emissions reporting output from legacy reporting
2. Converts regions from R12 to MAGICC format
3. Runs MAGICC climate processor with uncertainty quantification
4. Extracts temperature projections by percentile

Usage:
    python run_magicc_climate.py

Requires:
    - Emissions file from generate_emissions_report.py in reporting_output/
    - climate-processor package installed
"""

from pathlib import Path

from message_ix_models.report.legacy.call_climate_processor import (
    read_magicc_output,
    run_climate_processor,
)

# Test with the emissions output we just generated
scenario_string = "MESSAGE_GLOBIOM_SSP2_v6.1_Main_baseline_baseline_nexus_7p0_high_emissions_test"

print("="*70)
print("MAGICC CLIMATE PROCESSING PIPELINE")
print("="*70)

# Define paths - navigate from alps project directory to data directory
repo_root = Path(__file__).parent.parent.parent.parent
input_dir = repo_root / "message_ix_models/data/report/legacy/reporting_output"
output_dir = input_dir / "magicc_output"

print(f"\nScenario: {scenario_string}")
print(f"Input directory: {input_dir}")
print(f"Output directory: {output_dir}")

# Step 1: Run MAGICC climate processor
print("\n" + "="*70)
print("STEP 1: Running MAGICC Climate Processor")
print("="*70)
print("\nNOTE: Using 'medium' mode (100 configs) for uncertainty quantification.")
print("This will take longer than 'fast' mode but provides proper percentile ranges.")

try:
    run_climate_processor(
        sc=scenario_string,
        input_dir=input_dir,
        output_dir=output_dir,
        run_type="medium",  # Use medium mode for uncertainty quantification (100 configs)
        magicc_worker_number=4,
    )
    print("\n✓ MAGICC processing completed successfully!")
except Exception as e:
    print(f"\n✗ MAGICC processing failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

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
    exit(1)

print("\n" + "="*70)
print("SUCCESS - Full MAGICC pipeline test completed!")
print("="*70)
