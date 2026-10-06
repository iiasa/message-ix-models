# script to run magicc
import gc

import pandas as pd
from climate_processor import run_magicc

from message_ix_models.util import private_data_path


def run_climate_processor(sc):
    """Run the MAGICC climate model processor for a given scenario.

    This function prepares the necessary input and output file paths, ensures the
    output directory exists, and invokes the `run_magicc` function  from the package
    "climate_processor" to process the scenario data.
    The input and output filenames are generated using the model
    and scenario names from the provided `message_ix.Scenario` object.

    The input data is expected to be located in a directory named `reporting_output`,
    and the output will be saved in a directory named `magicc_output`. If the
    `magicc_output` directory does not exist, it will be created automatically.

    Parameters
    ----------
    sc : message_ix.Scenario
        A `message_ix.Scenario` object containing the model and scenario metadata.
        The `model` and `scenario` attributes are used to generate the input and
        output filenames.

    Notes
    -----
    - The input data file is expected to be named `{model}_{scenario}.xlsx` and must
      exist in the `reporting_output` directory.
    - The output file will be named `{model}_{scenario}_magicc.xlsx` and saved in the
      `magicc_output` directory.
    - The `run_magicc` function is responsible for the actual processing of the data.
    """

    # navigate one folder level up from the private_data_path
    in_file_path = private_data_path()
    in_file_path = in_file_path.parent
    in_file_path = in_file_path / "reporting_output"
    out_file_path = in_file_path / "magicc_output"

    # if folder "magicc_output" does not exist, create it
    if not out_file_path.exists():
        out_file_path.mkdir()

    # nomenclature
    model = sc.model
    scenario = sc.scenario
    input_data_name = f"{model}_{scenario}.xlsx"
    output_data_name = f"{model}_{scenario}_magicc.xlsx"

    run_magicc(
        input_data_file=input_data_name,
        results_file=output_data_name,
        input_data_directory=in_file_path,
        results_directory=out_file_path,
        logging_directory=out_file_path,
    )


def run_climate_processor_from_file(magicc_input_path, model, scenario):
    """Run the MAGICC climate model processor for a given scenario.

    This function prepares the necessary input and output file paths, ensures the
    output directory exists, and invokes the `run_magicc` function  from the package
    "climate_processor" to process the scenario data.
    The input and output filenames are generated using the model
    and scenario names from the provided `message_ix.Scenario` object.

    The input data is expected to be located in a directory named `reporting_output`,
    and the output will be saved in a directory named `magicc_output`. If the
    `magicc_output` directory does not exist, it will be created automatically.

    Parameters
    ----------
    input_magicc_file : str
        The path to the input data file.
    model : str
        The model name.
    scenario : str
        The scenario name.

    Notes
    -----
    - The input data file is expected to be named `{model}_{scenario}.xlsx` and must
      exist in the `reporting_output` directory.
    - The output file will be named `{model}_{scenario}_magicc.xlsx` and saved in the
      `magicc_output` directory.
    - The `run_magicc` function is responsible for the actual processing of the data.
    """

    # navigate one folder level up from the private_data_path
    in_file_path = private_data_path()
    in_file_path = in_file_path.parent
    in_file_path = in_file_path / "reporting_output"
    out_file_path = in_file_path / "magicc_output"

    # if folder "magicc_output" does not exist, create it
    if not out_file_path.exists():
        out_file_path.mkdir()

    # nomenclature
    # read excel from input_magicc_file
    magicc_input = pd.read_csv(magicc_input_path)
    # filter by modela nd scenarios
    magicc_input = magicc_input[
        (magicc_input["Model"] == model) & (magicc_input["Scenario"] == scenario)
    ]
    # write to file
    magicc_input.to_excel(in_file_path / f"{model}_{scenario}.xlsx", index=False)

    input_data_name = f"{model}_{scenario}.xlsx"
    output_data_name = f"{model}_{scenario}_0_magicc.xlsx"

    run_magicc(
        input_data_file=input_data_name,
        results_file=output_data_name,
        input_data_directory=in_file_path,
        results_directory=out_file_path,
        logging_directory=out_file_path,
    )


def read_magicc_output(sc_string, pp=50):
    """Read and extract a specific value from the MAGICC output file.

    This function reads the MAGICC output file for a given scenario string and extracts
    the surface temperature (GSAT) value for the 50th percentile (or a specified
    percentile) for the "World" region in the year 2100.

    The output file is expected to be located in the `magicc_output` directory under
    `reporting_output`. The file is named `{sc_string}_magicc.xlsx`.

    Parameters
    ----------
    sc_string : str
        A string representing the scenario, used to construct the output filename.
    pp : int, optional
        The percentile to filter in the MAGICC output. Default is 50 (50th percentile).

    Returns
    -------
    float
        The surface temperature (GSAT) value for the specified percentile, region
        ("World"), and year (2100).

    Notes
    -----
    - The function assumes the MAGICC output file is in Excel format and contains a
      column named "Variable" with the format
      `AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{pp}.0th
      Percentile`.
    - The function filters the data for the "World" region and the year 2100.
    - Memory cleanup is performed using `gc.collect()` to free up resources after
      reading the data.
    """

    # navigate one folder level up from the private_data_path
    out_file_path = private_data_path().parent / "reporting_output" / "magicc_output"

    # nomenclature
    output_data_name = f"{sc_string}_magicc.xlsx"

    # read the output of MAGICC
    df = pd.read_excel(out_file_path / output_data_name)
    # filter the variable
    # AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|50.0th Percentile
    # and region == "World" and year == 2100
    df = df[
        df["Variable"]
        == (
            f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|"
            f"{pp}.0th Percentile"
        )
    ]
    df = df[df["Region"] == "World"]
    # read the value in the "2100" column
    val = df["2100"].values[0]
    del df
    gc.collect()
    return val
