from typing import TYPE_CHECKING

import pytest

from message_ix_models.project.ssp.transport import main
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import pathlib


@pytest.fixture(scope="module")
def input_csv_path() -> "pathlib.Path":
    return package_data_path(
        "test",
        "report",
        "SSP_dev_SSP2_v0.1_Blv0.18_baseline_prep_lu_bkp_solved_materials_2025_macro.csv",
    )


@pytest.fixture(scope="module")
def input_xlsx_path(tmp_path_factory, input_csv_path) -> "pathlib.Path":
    import pandas as pd

    result = (
        tmp_path_factory.mktemp("ssp-transport")
        .joinpath(input_csv_path.name)
        .with_suffix(".xlsx")
    )

    pd.read_csv(input_csv_path).to_excel(result, index=False)

    return result


@main.minimum_version
def test_main(tmp_path, input_csv_path) -> None:
    """Code can be called from Python."""
    # Locate a temporary data file
    path_in = input_csv_path
    path_out = tmp_path.joinpath("output.csv")

    # Code runs
    main(path_in=path_in, path_out=path_out)

    # Output path exists
    assert path_out.exists()


@main.minimum_version
def test_cli(tmp_path, mix_models_cli, input_xlsx_path) -> None:
    """Code can be invoked from the command-line."""
    from shutil import copyfile

    # Locate a temporary data file
    input_file = input_xlsx_path
    path_in = tmp_path.joinpath(input_file.name)

    # Copy the input file to the test data directory
    copyfile(input_file, path_in)

    # Code runs
    result = mix_models_cli.invoke(["ssp", "transport", f"{path_in}"])
    assert 0 == result.exit_code, result.output

    # Output path was determined automatically and exists
    path_out = tmp_path.joinpath(path_in.stem + "_out.xlsx")
    assert path_out.exists()

    # Messages were printed about file handling
    for message in (
        "Convert Excel input to ",
        "No PATH_OUT given; write to ",
        "Convert CSV output to ",
    ):
        assert message in result.output
