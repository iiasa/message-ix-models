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
    result = mix_models_cli.invoke(list(subcommand) + ["--help"])

    assert 0 == result.exit_code, result.exc_info


def test_cli_debug(mix_models_cli):
    """The 'debug' CLI command can be invoked."""
    result = mix_models_cli.invoke(["debug"])
    assert 0 == result.exit_code, result.exc_info
