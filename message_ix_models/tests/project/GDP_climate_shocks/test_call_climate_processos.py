from unittest.mock import patch

import pandas as pd

from message_ix_models.project.GDP_climate_shocks.call_climate_processor import (
    read_magicc_output,
    run_climate_processor_from_file,
)
from message_ix_models.util import private_data_path


def test_read_magicc_output():
    # create a fake dataframe
    variable = (
        "AR6 climate diagnostics|Surface Temperature (GSAT)|"
        "MAGICCv7.5.3|50.0th Percentile"
    )
    df = pd.DataFrame(
        {
            "Variable": [variable],
            "Region": ["World"],
            "2100": [1.5],
        }
    )

    output_path = private_data_path().parent / "reporting_output" / "magicc_output"
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / "test_magicc.xlsx"

    # save the dataframe to a file
    df.to_excel(file_path, index=False)

    # test the function
    val = read_magicc_output("test")
    assert val == 1.5

    # cleanup
    file_path.unlink()


# Not such a meaningful test, as we cannot run run_magicc
def test_run_climate_processor_from_file(tmp_path):
    # create a dummy CSV input
    input_csv = tmp_path / "dummy_input.csv"
    pd.DataFrame(
        {
            "Model": ["m1", "m2"],
            "Scenario": ["s1", "s2"],
            "SomeValue": [1, 2],
        }
    ).to_csv(input_csv, index=False)

    # mock run_magicc to prevent actual processing
    with patch(
        "message_ix_models.project.GDP_climate_shocks.call_climate_processor.run_magicc"
    ) as mock_run:
        run_climate_processor_from_file(str(input_csv), model="m1", scenario="s1")

        # check that the input file for MAGICC was created correctly
        in_file_path = private_data_path().parent / "reporting_output"
        expected_input_file = in_file_path / "m1_s1.xlsx"
        assert expected_input_file.exists()

        # check that run_magicc was called with expected filenames
        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        assert kwargs["input_data_file"] == "m1_s1.xlsx"
        assert kwargs["results_file"] == "m1_s1_0_magicc.xlsx"

    # cleanup
    expected_input_file.unlink()
    magicc_output_dir = in_file_path / "magicc_output"
    if magicc_output_dir.exists():
        for f in magicc_output_dir.iterdir():
            f.unlink()
