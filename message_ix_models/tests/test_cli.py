"""Basic tests of the command line."""
import pytest
from click.testing import CliRunner

from message_ix_models.cli import main

SUBCOMMANDS = [
    tuple(),
    ("debug",),
]


def _cli_help_id(argvalue):
    return f"mix-models {' '.join(argvalue)} --help"


@pytest.mark.parametrize("subcommand", SUBCOMMANDS, ids=_cli_help_id)
def test_cli_help(subcommand):
    runner = CliRunner()
    result = runner.invoke(main, list(subcommand) + ["--help"])
    assert result.exit_code == 0, result.output
