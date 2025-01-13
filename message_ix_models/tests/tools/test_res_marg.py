import pytest

from message_ix_models.tools.res_marg import main


def test_cli(mix_models_cli) -> None:
    """Run :func:`.res_marg.main` via its command-line interface."""
    command = [
        "--model=model_name",
        "--scenario=scenario_name",
        "--version=123",
        "res-marg",
    ]

    # Fails: the model name, scenario name, and version do not exist
    with pytest.raises(RuntimeError):
        mix_models_cli.assert_exit_0(command)


@pytest.mark.xfail(reason="Function does not run on the snapshot")
def test_main(loaded_snapshot) -> None:
    """Run :func:`.res_marg.main` on the snapshot scenarios."""
    scen = loaded_snapshot

    # Function runs
    main(scen, None)
