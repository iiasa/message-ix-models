"""Basic tests of the command line."""
from contextlib import contextmanager

import click

from message_ix_models.cli import main
from message_ix_models.util.click import common_params


@contextmanager
def temporary_command(group, func):
    """Context manager that temporarily attaches command `func` to `group`."""
    try:
        group.add_command(func)
        yield
    finally:
        group.commands.pop(func.name)


def test_default_path_cb(session_context, mix_models_cli):
    """Test :func:`.default_path_cb`."""

    # Create a hidden command and attach it to the CLI
    @click.command(name="_test_default_path_cb", hidden=True)
    @common_params("rep_out_path")
    @click.pass_obj
    def func(ctx, rep_out_path):
        print(ctx["rep_out_path"])  # Print the value stored on the Context object

    # Command parameters: --local-data gives the local data path, but the --rep-out-path
    # option is *not* given
    cmd = [f"--local-data={session_context.local_data}", func.name]

    # …so default_path_cb() should supply "{local_data}/reporting_output".
    expected = session_context.local_data / "reporting_output"

    # Run the command
    with temporary_command(main, func):
        result = mix_models_cli.assert_exit_0(cmd)

    # The value was stored on, and retrieved from, `ctx`
    assert f"{expected}\n" == result.output


def test_store_context(mix_models_cli):
    """Test :func:`.store_context`."""

    # Create a hidden command and attach it to the CLI
    @click.command(name="_test_store_context", hidden=True)
    @common_params("ssp")
    @click.pass_obj
    def func(ctx, ssp):
        print(ctx["ssp"])  # Print the value stored on the Context object

    # Run the command with a valid value
    with temporary_command(main, func):
        result = mix_models_cli.assert_exit_0([func.name, "SSP2"])

    # The value was stored on, and retrieved from, `ctx`
    assert "SSP2\n" == result.output
