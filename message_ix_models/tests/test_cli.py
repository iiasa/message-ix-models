"""Basic tests of the command line."""
import ixmp
import pytest
from message_ix.testing import make_dantzig

from message_ix_models import util

COMMANDS = [
    tuple(),
    ("debug",),
    ("report",),
    ("snapshot",),
    ("ssp", "gen-structures"),
    ("techs",),
    ("water-ix",),
]


def _cli_help_id(argvalue):
    return f"mix-models {' '.join(argvalue)} --help"


@pytest.mark.parametrize("command", COMMANDS, ids=_cli_help_id)
def test_cli_help(mix_models_cli, command):
    """--help works for every CLI command."""
    mix_models_cli.assert_exit_0(list(command) + ["--help"])


def test_cli_debug(mix_models_cli):
    """The 'debug' CLI command can be invoked."""
    mix_models_cli.assert_exit_0(["debug"])


def test_cli_export_test_data(monkeypatch, session_context, mix_models_cli, tmp_path):
    """The :command:`export-test-data` command can be invoked."""
    # Create an empty scenario in the temporary local file database
    platform = "local"
    mp = ixmp.Platform(platform)
    scen = make_dantzig(mp)

    # URL
    url = f"ixmp://{platform}/{scen.model}/{scen.scenario}#{scen.version}"

    # Monkeypatch MESSAGE_DATA_PATH in case tests are being performed on a system
    # without message_data installed
    monkeypatch.setattr(util.common, "MESSAGE_DATA_PATH", tmp_path)
    tmp_path.joinpath("data", "tests").mkdir(exist_ok=True, parents=True)

    # File that will be created
    technology = ["coal_ppl"]
    dest_file = util.private_data_path(
        "tests", f"{scen.model}_{scen.scenario}_{'_'.join(technology)}.xlsx"
    )

    # Release the database lock
    mp.close_db()

    # Export works
    result = mix_models_cli.assert_exit_0([f"--url={url}", "export-test-data"])

    # The file is created in the expected location
    assert str(dest_file) in result.output
    assert dest_file.exists()
