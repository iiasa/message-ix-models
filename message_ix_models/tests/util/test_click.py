"""Basic tests of the command line."""

import click
import pytest

from message_ix_models.cli import cli_test_group
from message_ix_models.util.click import (
    common_params,
    scenario_param,
    temporary_command,
)


def test_default_path_cb(session_context, mix_models_cli):
    """Test :func:`.default_path_cb`."""

    # Create a hidden command and attach it to the CLI
    @click.command("default_path_cb")
    @common_params("rep_out_path")
    @click.pass_obj
    def func(ctx, rep_out_path):
        print(ctx["rep_out_path"])  # Print the value stored on the Context object

    # Command parameters: --local-data gives the local data path, but the --rep-out-path
    # option is *not* given
    cmd = [f"--local-data={session_context.local_data}", "_test", func.name]

    # …so default_path_cb() should supply "{local_data}/reporting_output".
    expected = session_context.local_data / "reporting_output"

    # Run the command
    with temporary_command(cli_test_group, func):
        result = mix_models_cli.assert_exit_0(cmd)

    # The value was stored on, and retrieved from, `ctx`
    assert result.output.startswith(f"{expected}\n")


def test_regions(mix_models_cli):
    """--regions=… used on both group and a command within the group.

    If the option is not provided to the inner command, the value given to the outer
    group should persist.
    """

    @click.group()
    @common_params("regions")
    def outer(regions):
        pass

    @outer.command()
    @common_params("regions")
    @click.pass_obj
    def inner(context, regions):
        print(context.model.regions)

    # Give the option for the outer group, but not for the inner command
    with temporary_command(cli_test_group, outer):
        result = mix_models_cli.assert_exit_0(
            ["_test", "outer", "--regions=ZMB", "inner"]
        )

    # Value given to the outer group is stored and available to the inner command
    assert "ZMB" == result.output.strip()


@pytest.mark.parametrize(
    "args, command, expected",
    [
        # As a (required, positional) argument
        (dict(param_decls="ssp"), ["LED"], "LED"),
        (dict(param_decls="ssp"), ["FOO"], "'FOO' is not one of 'LED', 'SSP1', "),
        # As an option
        # With no default
        (dict(param_decls="--ssp"), [], "None"),
        # With a limited of values
        (
            dict(param_decls="--ssp", values=["LED", "SSP2"]),
            ["--ssp=SSP1"],
            "'SSP1' is not one of 'LED', 'SSP2'",
        ),
        # With a default
        (dict(param_decls="--ssp", default="SSP2"), [], "SSP2"),
        # With a different name
        (dict(param_decls=["--scenario", "ssp"]), ["--scenario=SSP5"], "SSP5"),
    ],
)
def test_scenario_param(capsys, mix_models_cli, args, command, expected):
    """Tests of :func:`scenario_param`."""

    # scenario_param() can be used as a decorator with `args`
    @click.command
    @scenario_param(**args)
    @click.pass_obj
    def cmd(context):
        """Temporary click Command: print the direct value and Context attribute."""
        print(f"{context.ssp}")

    with temporary_command(cli_test_group, cmd):
        try:
            result = mix_models_cli.assert_exit_0(["_test", "cmd"] + command)
        except RuntimeError as e:
            # `command` raises the expected value or error message
            assert expected in capsys.readouterr().out, e
        else:
            # `command` can be invoked without error, and the function/Context get the
            # expected value
            assert expected == result.output.strip()


def test_store_context(mix_models_cli):
    """Test :func:`.store_context`."""

    # Create a hidden command and attach it to the CLI
    @click.command("store_context")
    @common_params("ssp")
    @click.pass_obj
    def func(ctx, ssp):
        print(ctx["ssp"])  # Print the value stored on the Context object

    # Run the command with a valid value
    with temporary_command(cli_test_group, func):
        result = mix_models_cli.assert_exit_0(["_test", func.name, "SSP2"])

    # The value was stored on, and retrieved from, `ctx`
    assert "SSP2\n" == result.output


def test_urls_from_file(mix_models_cli, tmp_path):
    """Test :func:`.urls_from_file` callback."""

    # Create a hidden command and attach it to the CLI
    @click.command("urls_from_file")
    @common_params("urls_from_file")
    @click.pass_obj
    def func(ctx, **kwargs):
        # Print the value stored on the Context object
        print("\n".join([s.url for s in ctx.core.scenarios]))

    # Create a temporary file with some scenario URLs
    text = """m/s#3
foo/bar#5
baz/qux#123
"""
    p = tmp_path.joinpath("scenarios.txt")
    p.write_text(text)

    # Run the command, referring to the temporary file
    with temporary_command(cli_test_group, func):
        result = mix_models_cli.assert_exit_0(
            ["_test", func.name, f"--urls-from-file={p}"]
        )

    # Scenario URLs are parsed to ScenarioInfo objects, and then can be reconstructed →
    # data is round-tripped
    assert text == result.output
