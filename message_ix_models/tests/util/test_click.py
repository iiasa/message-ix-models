"""Basic tests of the command line."""
import click
from click.testing import CliRunner

from message_ix_models.cli import main
from message_ix_models.util.click import common_params


def test_default_path_cb(session_context):
    """Test :func:`.default_path_cb`."""
    # Create a hidden command and attach it to the CLI
    name = "_test_default_path_cb"

    @main.command(name=name, hidden=True)
    @common_params("rep_out_path")
    @click.pass_obj
    def _(ctx, rep_out_path):
        print(ctx["rep_out_path"])  # Print the value stored on the Context object

    # Command parameters: --local-data gives the local data path, but the --rep-out-path
    # option is *not* given
    cmd = [f"--local-data={session_context.local_data}", name]

    # …so default_path_cb() should supply "{local_data}/reporting_output".
    expected = session_context.local_data / "reporting_output"

    # Run the command
    result = CliRunner().invoke(main, cmd)

    # The value was stored on, and retrieved from, `ctx`
    assert 0 == result.exit_code
    assert f"{expected}\n" == result.output


def test_store_context():
    """Test :func:`.store_context`."""
    # Create a hidden command and attach it to the CLI
    name = "_test_store_context"

    @main.command(name=name, hidden=True)
    @common_params("ssp")
    @click.pass_obj
    def _(ctx, ssp):
        print(ctx["ssp"])  # Print the value stored on the Context object

    # Run the command with a valid value
    result = CliRunner().invoke(main, [name, "SSP2"])

    # The value was stored on, and retrieved from, `ctx`
    assert 0 == result.exit_code
    assert "SSP2\n" == result.output
