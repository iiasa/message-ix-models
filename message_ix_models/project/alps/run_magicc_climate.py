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


def _load_and_validate_emissions(input_file, run_type_enum, workers):
    """Load emissions file and validate for MAGICC processing.

    Args:
        input_file: Path to emissions Excel file
        run_type_enum: MAGICCRunType enum value
        workers: Number of parallel workers

    Returns:
        tuple: (IamDataFrame, MAGICCProcessor)
    """
    print("\n" + "=" * 70)
    print("Loading and validating emissions data")
    print("=" * 70)

    df = pyam.IamDataFrame(input_file)
    print(f"  Loaded {len(df)} rows from {input_file.name}")

    if "R12_World" in df.region:
        df = df.rename(region={"R12_World": "World"})
        print("  Converted region: R12_World -> World")

    processor = MAGICCProcessor(
        run_type=run_type_enum,
        magicc_worker_number=workers,
    )

    print("  Validating required emissions variables...")
    df = processor.required_data_validator.apply(df)
    print("  ✓ Validation passed")

    return df, processor


def _run_climate_assessment_workflow(
    df, processor, output_dir, workers, num_configs, return_all_runs
):
    """Run climate-assessment MAGICC workflow.

    Args:
        df: IamDataFrame with emissions
        processor: MAGICCProcessor instance
        output_dir: Output directory for climate-assessment
        workers: Number of parallel workers
        num_configs: Number of MAGICC configurations
        return_all_runs: Whether to return all individual runs
    """
    print("\n" + "=" * 70)
    print("Running climate-assessment with MAGICC")
    print("=" * 70)

    output_dir.mkdir(parents=True, exist_ok=True)

    with (
        tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as data_tempdir,
        tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as magicc_worker_dir,
    ):
        magicc_binary = "magicc.exe" if sys.platform.startswith("win") else "magicc"
        os.environ["MAGICC_EXECUTABLE_7"] = str(
            processor.magicc_config.magicc_location / "bin" / magicc_binary
        )
        os.environ["MAGICC_WORKER_NUMBER"] = str(workers)
        os.environ["MAGICC_WORKER_ROOT_DIR"] = magicc_worker_dir
        print("  Set MAGICC environment variables")

        from climate_processor import MAGICC_MAXIMUM_VARIABLE_SET

        temp_csv = Path(data_tempdir) / "data.csv"
        df.filter(region="World", variable=MAGICC_MAXIMUM_VARIABLE_SET).to_csv(temp_csv)
        print(f"  Saved filtered emissions to: {temp_csv}")

        print(f"  Config: {processor.magicc_config.probabilistic_file_name}")
        print(f"  Model: magicc v7.5.3")
        print(f"  Num configs: {num_configs}")
        print(f"  Harmonization: False")
        print(f"  Return all runs: {return_all_runs}")

        try:
            workflow_params = dict(processor.magicc_config.run_workflow_input)
            workflow_params["harmonize"] = False
            workflow_params["inputcheck"] = False
            workflow_params["postprocess"] = False
            workflow_params["return_all_runs"] = return_all_runs

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


def _rename_and_cleanup_outputs(scenario, output_dir, suffix=""):
    """Rename climate-assessment outputs and cleanup intermediate files.

    Args:
        scenario: Scenario name for output files
        output_dir: Directory containing climate-assessment outputs
        suffix: Optional suffix for output filenames (e.g., '_reference_isimip3b')

    Returns:
        dict: Paths to renamed output files
    """
    print("\n" + "=" * 70)
    print("Renaming output files and cleaning up")
    print("=" * 70)

    import shutil

    magicc_output_dir = output_dir.parent / "magicc_output"
    magicc_output_dir.mkdir(parents=True, exist_ok=True)

    output_files = {}

    # Rename rawoutput (all runs)
    rawoutput_file = output_dir / "data_rawoutput.xlsx"
    if rawoutput_file.exists():
        renamed_file = magicc_output_dir / f"{scenario}_magicc{suffix}.xlsx"
        shutil.copy2(rawoutput_file, renamed_file)
        output_files["all_runs"] = renamed_file
        print(f"  Copied: data_rawoutput.xlsx -> {renamed_file.name}")

    # Rename IAMC output (percentiles)
    iamc_file = output_dir / "data_IAMC_climateassessment.xlsx"
    if iamc_file.exists():
        renamed_file = magicc_output_dir / f"{scenario}_magicc_percentiles{suffix}.xlsx"
        shutil.copy2(iamc_file, renamed_file)
        output_files["percentiles"] = renamed_file
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

    return output_files


def run_magicc(
    scenario,
    run_type="medium",
    workers=4,
    input_dir=None,
    output_dir=None,
    return_all_runs=True,
    generate_reference=False,
):
    """Run MAGICC climate processing on MESSAGE emissions using climate-assessment.

    Args:
        scenario: Scenario name (Excel filename without .xlsx)
        run_type: 'fast' (1 config), 'medium' (100 configs), 'complete' (600 configs), 'isimip3b' (14 configs)
        workers: Number of parallel workers (passed to climate-assessment)
        input_dir: Input directory for emissions files
        output_dir: Output directory for MAGICC results
        return_all_runs: If True, returns all individual run timeseries with run_id (default: True)
        generate_reference: If True, also run ISIMIP3b (14 runs) as reference distribution for importance weighting

    Example:
        mix-models alps run-magicc --scenario MESSAGE_GLOBIOM_SSP2_v6.4_baseline --run-type medium --generate-reference
    """
    print("=" * 70)
    print("MAGICC CLIMATE ASSESSMENT PIPELINE")
    print("=" * 70)

    # Setup directories
    if input_dir is None:
        input_dir = package_data_path("report", "legacy", "reporting_output")
    if output_dir is None:
        output_dir = package_data_path(
            "report", "legacy", "reporting_output", "climate_assessment_output"
        )

    # Configuration mappings
    run_type_map = {
        "fast": MAGICCRunType.FAST,
        "medium": MAGICCRunType.MEDIUM,
        "complete": MAGICCRunType.COMPLETE,
        "isimip3b": MAGICCRunType.ISIMIP3B,
    }
    num_configs = {"fast": 1, "medium": 100, "complete": 600, "isimip3b": 14}

    print(f"\nScenario: {scenario}")
    print(f"Run type: {run_type} ({num_configs[run_type]} configurations)")
    print(f"Workers: {workers}")
    print(f"Return all runs: {return_all_runs}")
    print(f"Generate reference: {generate_reference}")

    # Validate input file exists
    input_file = input_dir / f"{scenario}.xlsx"
    if not input_file.exists():
        print(f"\n✗ ERROR: Input file not found: {input_file}")
        print(f"\nAvailable files in {input_dir}:")
        for f in input_dir.glob("*.xlsx"):
            print(f"  - {f.name}")
        raise SystemExit(1)

    # Load and validate emissions (once for both main and reference runs)
    df, processor = _load_and_validate_emissions(
        input_file, run_type_map[run_type], workers
    )

    # Run main MAGICC configuration
    _run_climate_assessment_workflow(
        df, processor, output_dir, workers, num_configs[run_type], return_all_runs
    )

    # Rename and cleanup main outputs
    main_outputs = _rename_and_cleanup_outputs(
        scenario, output_dir, suffix="_all_runs" if return_all_runs else ""
    )

    # Generate reference distribution if requested
    reference_outputs = {}
    if generate_reference and run_type != "isimip3b":
        print("\n" + "=" * 70)
        print("Generating ISIMIP3b reference distribution")
        print("=" * 70)
        print(f"  Running MAGICC with ISIMIP3b configuration (14 runs)")
        print(f"  Will be used for importance weighting")

        # Create new processor for ISIMIP3b configuration
        ref_processor = MAGICCProcessor(
            run_type=MAGICCRunType.ISIMIP3B,
            magicc_worker_number=workers,
        )

        # Run ISIMIP3b with same emissions data
        _run_climate_assessment_workflow(
            df,
            ref_processor,
            output_dir,
            workers,
            num_configs["isimip3b"],
            return_all_runs=True,
        )

        # Rename and cleanup reference outputs with special suffix
        reference_outputs = _rename_and_cleanup_outputs(
            scenario, output_dir, suffix="_reference_isimip3b"
        )

    # Report results
    print("\n" + "=" * 70)
    print("Results Summary")
    print("=" * 70)

    if "all_runs" in main_outputs:
        print(f"\n✓ Main run: {num_configs[run_type]} configurations")
        print(f"  {main_outputs['all_runs'].name}")
    if "percentiles" in main_outputs:
        print(f"  {main_outputs['percentiles'].name}")

    if reference_outputs:
        print(f"\n✓ Reference distribution: 14 ISIMIP3b configurations")
        if "all_runs" in reference_outputs:
            print(f"  {reference_outputs['all_runs'].name}")
        if "percentiles" in reference_outputs:
            print(f"  {reference_outputs['percentiles'].name}")

    print("\n" + "=" * 70)
    print("SUCCESS - MAGICC pipeline completed!")
    print("=" * 70)


@click.command()
@click.option(
    "--scenario", required=True, help="Scenario name (Excel filename without .xlsx)"
)
@click.option(
    "--run-type",
    type=click.Choice(["fast", "medium", "complete", "isimip3b"]),
    default="medium",
    help="MAGICC run type: fast (1 config), medium (100 configs), complete (600 configs), isimip3b (14 ISIMIP3b-optimized configs)",
)
@click.option(
    "--workers", type=int, default=4, help="Number of parallel workers for MAGICC"
)
@click.option(
    "--input-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Input directory for emissions files (default: data/report/legacy/reporting_output)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for MAGICC results (default: data/report/legacy/reporting_output/climate_assessment_output)",
)
@click.option(
    "--return-all-runs/--no-return-all-runs",
    default=True,
    help="Return all individual run timeseries with run_id column (default: True)",
)
@click.option(
    "--generate-reference/--no-generate-reference",
    default=False,
    help="Also generate ISIMIP3b reference distribution (14 runs) for importance weighting (default: False)",
)
def main(
    scenario,
    run_type,
    workers,
    input_dir,
    output_dir,
    return_all_runs,
    generate_reference,
):
    """CLI wrapper for run_magicc."""
    run_magicc(
        scenario,
        run_type,
        workers,
        input_dir,
        output_dir,
        return_all_runs,
        generate_reference,
    )


if __name__ == "__main__":
    main()
