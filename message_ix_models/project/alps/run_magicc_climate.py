"""Run MAGICC climate processing on MESSAGE emissions using climate-assessment.

This script:
1. Reads emissions reporting output from legacy reporting
2. Converts regions from R12 to MAGICC format
3. Runs MAGICC via climate-assessment with raw output preservation
4. Saves individual run timeseries (run_id) for surrogate modeling

Requires:
    - Emissions file from emissions-report in reporting_output/
    - climate-assessment package installed
"""

import click
import os
import sys
import tempfile
from pathlib import Path
import pyam

from message_ix_models.util import package_data_path
from climate_assessment.cli import run_workflow as climate_assessment_run_workflow
from climate_processor import MAGICCProcessor, MAGICCRunType


def run_magicc(scenario, run_type='medium', workers=4, input_dir=None, output_dir=None,
               return_all_runs=True):
    """Run MAGICC climate processing on MESSAGE emissions using climate-assessment.

    Args:
        scenario: Scenario name (Excel filename without .xlsx)
        run_type: 'fast' (1 config), 'medium' (100 configs), 'complete' (600 configs), 'isimip3b' (14 configs)
        workers: Number of parallel workers (passed to climate-assessment)
        input_dir: Input directory for emissions files
        output_dir: Output directory for MAGICC results
        return_all_runs: If True, returns all individual run timeseries with run_id (default: True)

    Example:
        mix-models alps run-magicc --scenario MESSAGE_GLOBIOM_SSP2_v6.4_baseline --run-type isimip3b
    """
    scenario_string = scenario

    print("="*70)
    print("MAGICC CLIMATE ASSESSMENT PIPELINE")
    print("="*70)

    if input_dir is None:
        input_dir = package_data_path("report", "legacy", "reporting_output")
    if output_dir is None:
        output_dir = package_data_path("report", "legacy", "reporting_output", "climate_assessment_output")

    # Map run_type to MAGICCRunType enum
    run_type_map = {
        'fast': MAGICCRunType.FAST,
        'medium': MAGICCRunType.MEDIUM,
        'complete': MAGICCRunType.COMPLETE,
        'isimip3b': MAGICCRunType.ISIMIP3B,
    }
    num_configs = {'fast': 1, 'medium': 100, 'complete': 600, 'isimip3b': 14}

    print(f"\nScenario: {scenario_string}")
    print(f"Run type: {run_type} ({num_configs[run_type]} configurations)")
    print(f"Workers: {workers}")
    print(f"Return all runs: {return_all_runs}")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")

    # Check if input file exists
    input_file = input_dir / f"{scenario_string}.xlsx"
    if not input_file.exists():
        print(f"\n✗ ERROR: Input file not found: {input_file}")
        print(f"\nAvailable files in {input_dir}:")
        for f in input_dir.glob("*.xlsx"):
            print(f"  - {f.name}")
        raise SystemExit(1)

    # Step 1: Load data as IamDataFrame and preprocess
    print("\n" + "="*70)
    print("STEP 1: Preprocessing emissions data")
    print("="*70)

    # Load as IamDataFrame (handles unit definitions)
    df = pyam.IamDataFrame(input_file)
    print(f"  Loaded {len(df)} rows from {input_file.name}")

    # Convert R12_World to World
    if "R12_World" in df.region:
        df = df.rename(region={"R12_World": "World"})
        print("  Converted region: R12_World -> World")

    # Create MAGICCProcessor to use its preprocessing
    processor = MAGICCProcessor(
        run_type=run_type_map[run_type],
        magicc_worker_number=workers,
    )

    # Validate required data
    print("  Validating required emissions variables...")
    df = processor.required_data_validator.apply(df)
    print("  ✓ Validation passed")

    # Step 2: Run climate-assessment with proper setup
    print("\n" + "="*70)
    print("STEP 2: Running climate-assessment with MAGICC")
    print("="*70)

    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as data_tempdir, \
         tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as magicc_worker_dir:

        # Set MAGICC environment variables
        magicc_binary = "magicc.exe" if sys.platform.startswith("win") else "magicc"
        os.environ["MAGICC_EXECUTABLE_7"] = str(
            processor.magicc_config.magicc_location / "bin" / magicc_binary
        )
        os.environ["MAGICC_WORKER_NUMBER"] = str(workers)
        os.environ["MAGICC_WORKER_ROOT_DIR"] = magicc_worker_dir
        print("  Set MAGICC environment variables")

        # Filter to World region and required variables, save as CSV
        from climate_processor import MAGICC_MAXIMUM_VARIABLE_SET
        temp_csv = Path(data_tempdir) / "data.csv"
        df.filter(region="World", variable=MAGICC_MAXIMUM_VARIABLE_SET).to_csv(temp_csv)
        print(f"  Saved filtered emissions to: {temp_csv}")

        print(f"  Config: {processor.magicc_config.probabilistic_file_name}")
        print(f"  Model: magicc v7.5.3")
        print(f"  Num configs: {num_configs[run_type]}")
        print(f"  Harmonization: False")
        print(f"  Return all runs: {return_all_runs}")

        try:
            # Call climate-assessment with output to our directory
            workflow_params = dict(processor.magicc_config.run_workflow_input)
            workflow_params['harmonize'] = False
            workflow_params['inputcheck'] = False  # Disable validation
            workflow_params['postprocess'] = False  # Skip emissions postprocessing (Kyoto gases checks)
            workflow_params['return_all_runs'] = return_all_runs  # Return all runs in IAMC format

            climate_assessment_run_workflow(
                input_emissions_file=str(temp_csv),
                outdir=str(output_dir),
                **workflow_params,
            )
            print("\n✓ climate-assessment completed successfully!")
        except Exception as e:
            print(f"\n✗ climate-assessment failed: {e}")
            import traceback
            traceback.print_exc()
            raise SystemExit(1)

    # Step 3: Rename output files to include scenario name
    print("\n" + "="*70)
    print("STEP 3: Renaming output files")
    print("="*70)

    import shutil

    # Rename rawoutput (all runs) if it exists
    rawoutput_file = output_dir / "data_rawoutput.xlsx"
    if rawoutput_file.exists() and return_all_runs:
        renamed_file = output_dir.parent / "magicc_output" / f"{scenario_string}_magicc_all_runs.xlsx"
        renamed_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(rawoutput_file, renamed_file)
        print(f"  Copied: data_rawoutput.xlsx -> {renamed_file.name}")

    # Rename IAMC output (percentiles) if it exists
    iamc_file = output_dir / "data_IAMC_climateassessment.xlsx"
    if iamc_file.exists():
        renamed_file = output_dir.parent / "magicc_output" / f"{scenario_string}_magicc.xlsx"
        renamed_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(iamc_file, renamed_file)
        print(f"  Copied: data_IAMC_climateassessment.xlsx -> {renamed_file.name}")

    # Clean up intermediate files
    print("\n  Cleaning up intermediate files...")
    cleanup_files = [
        "data_rawoutput.xlsx",
        "data_IAMC_climateassessment.xlsx",
        "data_harmonized_infilled.xlsx",
        "data_alloutput.xlsx",
        "data_full_exceedance_probabilities.xlsx",
        "data_meta.xlsx",
    ]
    for filename in cleanup_files:
        filepath = output_dir / filename
        if filepath.exists():
            filepath.unlink()
            print(f"    Removed: {filename}")
    print("  ✓ Cleanup complete")

    # Step 4: Report results
    print("\n" + "="*70)
    print("STEP 4: Results Summary")
    print("="*70)

    if return_all_runs:
        print(f"\n✓ Output contains all {num_configs[run_type]} individual MAGICC runs with run_id column")
        print(f"✓ Saved to: magicc_output/{scenario_string}_magicc_all_runs.xlsx")
    print(f"✓ Percentiles saved to: magicc_output/{scenario_string}_magicc.xlsx")

    print("\n" + "="*70)
    print("SUCCESS - Full MAGICC pipeline completed!")
    print("="*70)


@click.command()
@click.option(
    '--scenario',
    required=True,
    help='Scenario name (Excel filename without .xlsx)'
)
@click.option(
    '--run-type',
    type=click.Choice(['fast', 'medium', 'complete', 'isimip3b']),
    default='medium',
    help='MAGICC run type: fast (1 config), medium (100 configs), complete (600 configs), isimip3b (14 ISIMIP3b-optimized configs)'
)
@click.option(
    '--workers',
    type=int,
    default=4,
    help='Number of parallel workers for MAGICC'
)
@click.option(
    '--input-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Input directory for emissions files (default: data/report/legacy/reporting_output)'
)
@click.option(
    '--output-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Output directory for MAGICC results (default: data/report/legacy/reporting_output/climate_assessment_output)'
)
@click.option(
    '--return-all-runs/--no-return-all-runs',
    default=True,
    help='Return all individual run timeseries with run_id column (default: True)'
)
def main(scenario, run_type, workers, input_dir, output_dir, return_all_runs):
    """CLI wrapper for run_magicc."""
    run_magicc(scenario, run_type, workers, input_dir, output_dir, return_all_runs)


if __name__ == "__main__":
    main()
