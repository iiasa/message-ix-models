"""Basic tests of the command line."""
import pytest

SUBCOMMANDS = [
    tuple(),
    ("debug",),
]


def _cli_help_id(argvalue):
    return f"mix-models {' '.join(argvalue)} --help"


@pytest.mark.parametrize("subcommand", SUBCOMMANDS, ids=_cli_help_id)
def test_cli_help(mix_models_cli, subcommand):
    """--help works for every CLI command."""
    mix_models_cli.assert_exit_0(list(subcommand) + ["--help"])


def test_cli_debug(mix_models_cli):
    """The 'debug' CLI command can be invoked."""
    mix_models_cli.assert_exit_0(["debug"])
