from subprocess import CalledProcessError, check_call

import pytest

from message_ix_models.tools.res_marg import main


def test_cli() -> None:
    """Run :func:`.res_marg.main` via its command-line interface."""
    command = [
        "python",
        "-m",
        "message_ix_models.tools.res_marg",
        "--version=123",
        "model_name",
        "scenario_name",
    ]

    # Fails: the model name, scenario name, and version do not exit
    with pytest.raises(CalledProcessError):
        check_call(command)


@pytest.mark.xfail(reason="Function does not run on the snapshot")
def test_main(loaded_snapshot) -> None:
    """Run :func:`.res_marg.main` on the snapshot scenarios."""
    scen = loaded_snapshot

    # Function runs
    main(scen, None)
