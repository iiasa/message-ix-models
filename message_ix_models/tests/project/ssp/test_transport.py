from message_ix_models.project.ssp.transport import main
from message_ix_models.util import package_data_path


def test_main(tmp_path) -> None:
    """Code can be called from Python."""
    # Locate a temporary data file
    path_in = package_data_path(
        "test",
        "report",
        "SSP_dev_SSP2_v0.1_Blv0.18"
        "_baseline_prep_lu_bkp_solved_materials_2025_macro.csv",
    )
    path_out = tmp_path.joinpath("output.csv")

    # Code runs
    main(path_in=path_in, path_out=path_out)

    # Output path exists
    assert path_out.exists()


def test_cli(tmp_path, mix_models_cli) -> None:
    """Code can be invoked from the command-line."""
    from shutil import copyfile

    # Locate a temporary data file
    input_file = package_data_path(
        "test",
        "report",
        "SSP_dev_SSP2_v0.1_Blv0.18"
        "_baseline_prep_lu_bkp_solved_materials_2025_macro.xlsx",
    )
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
