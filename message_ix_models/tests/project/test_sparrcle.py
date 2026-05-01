"""Smoke test for the SPARRCLE CLI registration."""

from message_ix_models.project.sparrcle.cli import cli as sparrcle_cli


def test_run_command_registered() -> None:
    assert "run" in sparrcle_cli.commands
    run_cmd = sparrcle_cli.commands["run"]
    opts = {p.name: p for p in run_cmd.params}
    assert "config_path" in opts  # --config
    assert "go" in opts  # --go from make_click_command
    assert "truncate_step" in opts  # --from from make_click_command
