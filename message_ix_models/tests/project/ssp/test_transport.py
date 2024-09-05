from message_ix_models.project.ssp.transport import main


def test_main(tmp_path):
    # Locate a temporary data file
    p = tmp_path.joinpath("input.xlsx")

    main(p)
