from message_ix_models.project.ssp.transport import main
from message_ix_models.util import package_data_path


def test_main(tmp_path) -> None:
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
