"""Wrapper to run MAGICC climate model processor for MESSAGE scenarios.

This module provides functions to:
1. Run MAGICC on emissions reporting output
2. Read MAGICC temperature projections

The wrapper automatically handles region naming conversion from MESSAGE-specific
conventions (e.g., "R12_World") to MAGICC-expected conventions ("World").
"""

import gc
from pathlib import Path

import pandas as pd
from climate_processor import run_magicc


def run_climate_processor(
    sc,
    input_dir=None,
    output_dir=None,
    input_filename=None,
    output_filename=None,
    run_type="fast",
    magicc_worker_number=4,
):
    """Run the MAGICC climate model processor for a given scenario.

    This function prepares the necessary input and output file paths, ensures the
    output directory exists, converts region naming from MESSAGE conventions to
    MAGICC conventions, and invokes the `run_magicc` function from the package
    "climate_processor" to process the scenario data.

    Parameters
    ----------
    sc : message_ix.Scenario or str
        A `message_ix.Scenario` object containing the model and scenario metadata,
        or a string representing the scenario name. The `model` and `scenario`
        attributes are used to generate the input and output filenames.
    input_dir : Path, optional
        Directory containing input emissions xlsx file. If not provided, uses
        message_ix_models/data/report/legacy/reporting_output/
    output_dir : Path, optional
        Directory for MAGICC output. If not provided, uses
        message_ix_models/data/report/legacy/reporting_output/magicc_output/
    input_filename : str, optional
        Input filename. If not provided, uses {model}_{scenario}.xlsx
    output_filename : str, optional
        Output filename. If not provided, uses {model}_{scenario}_magicc.xlsx
    run_type : str, optional
        MAGICC run type: "fast" (1 config), "medium" (100 configs), or "complete" (600 configs).
        Default is "fast" for quick testing.
    magicc_worker_number : int, optional
        Number of parallel processes for MAGICC. Default is 4.

    Notes
    -----
    - The function automatically converts "R12_World" to "World" for MAGICC compatibility
    - Creates output directory if it doesn't exist
    - The `run_magicc` function from climate_processor handles the actual MAGICC execution
    """
    # Determine paths
    if input_dir is None:
        # Use ALPS reporting output directory
        input_dir = (
            Path(__file__).parent.parent.parent
            / "data"
            / "report"
            / "legacy"
            / "reporting_output"
        )
    else:
        input_dir = Path(input_dir)

    if output_dir is None:
        output_dir = input_dir / "magicc_output"
    else:
        output_dir = Path(output_dir)

    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine filenames
    if hasattr(sc, "model") and hasattr(sc, "scenario"):
        model = sc.model
        scenario = sc.scenario
        sc_string = f"{model}_{scenario}"
    else:
        sc_string = str(sc)

    if input_filename is None:
        input_filename = f"{sc_string}.xlsx"
    if output_filename is None:
        output_filename = f"{sc_string}_magicc.xlsx"

    # Read input file and convert region naming
    print(f"Preparing MAGICC input from {input_filename}...")
    input_path = input_dir / input_filename
    df = pd.read_excel(input_path, sheet_name="data")

    # Convert R12_World to World for MAGICC compatibility
    region_mapping = {"R12_World": "World"}
    if "Region" in df.columns:
        original_regions = df["Region"].unique()
        df["Region"] = df["Region"].replace(region_mapping)
        converted_regions = [
            r for r in original_regions if r in region_mapping.keys()
        ]
        if converted_regions:
            print(
                f"  Converted region names: {converted_regions} -> {[region_mapping[r] for r in converted_regions]}"
            )

    # Save converted file to temporary location
    temp_input_path = output_dir / f"_temp_{input_filename}"
    df.to_excel(temp_input_path, sheet_name="data", index=False)
    print(f"  Saved region-converted input to {temp_input_path}")

    # Clean up memory
    del df
    gc.collect()

    # Run MAGICC
    print(f"\nRunning MAGICC climate processor...")
    print(f"  Run type: {run_type}")
    print(f"  Worker processes: {magicc_worker_number}")
    print(f"  Input: {temp_input_path}")
    print(f"  Output: {output_dir / output_filename}")

    run_magicc(
        input_data_file=temp_input_path.name,
        results_file=output_filename,
        input_data_directory=output_dir,  # Using output_dir since we saved temp file there
        results_directory=output_dir,
        logging_directory=output_dir,
        run_type=run_type,
        magicc_worker_number=magicc_worker_number,
    )

    # Clean up temporary file
    if temp_input_path.exists():
        temp_input_path.unlink()
        print(f"  Cleaned up temporary input file")

    print(f"\nMAGICC processing complete! Output: {output_dir / output_filename}")


def read_magicc_output(sc_string, pp=50, output_dir=None):
    """Read and extract a specific value from the MAGICC output file.

    This function reads the MAGICC output file for a given scenario string and extracts
    the surface temperature (GSAT) value for the 50th percentile (or a specified percentile)
    for the "World" region in the year 2100.

    Parameters
    ----------
    sc_string : str
        A string representing the scenario, used to construct the output filename.
        Can be in format "{model}_{scenario}" or just scenario name.
    pp : int, optional
        The percentile to filter in the MAGICC output. Default is 50 (50th percentile).
    output_dir : Path, optional
        Directory containing MAGICC output. If not provided, uses
        message_ix_models/data/report/legacy/reporting_output/magicc_output/

    Returns
    -------
    float
        The surface temperature (GSAT) value for the specified percentile, region ("World"),
        and year (2100).

    Notes
    -----
    - The function assumes the MAGICC output file is in Excel format and contains a column
      named "Variable" with the format `AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{pp}.0th Percentile`.
    - The function filters the data for the "World" region and the year 2100.
    - Memory cleanup is performed using `gc.collect()` to free up resources after reading the data.
    """
    # Determine output directory
    if output_dir is None:
        output_dir = (
            Path(__file__).parent.parent.parent
            / "data"
            / "report"
            / "legacy"
            / "reporting_output"
            / "magicc_output"
        )
    else:
        output_dir = Path(output_dir)

    # Construct filename
    output_data_name = f"{sc_string}_magicc.xlsx"

    # Read the output of MAGICC
    print(f"Reading MAGICC output from {output_dir / output_data_name}...")
    df = pd.read_excel(output_dir / output_data_name)

    # Filter for the requested variable and region
    variable_name = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{pp}.0th Percentile"
    df = df[df["Variable"] == variable_name]
    df = df[df["Region"] == "World"]

    # Extract the value for year 2100
    if df.empty:
        raise ValueError(
            f"No data found for variable '{variable_name}' in region 'World'"
        )

    val = df["2100"].values[0]
    print(f"  Temperature (GSAT) for {pp}th percentile in 2100: {val:.2f}°C")

    # Clean up memory
    del df
    gc.collect()

    return val
