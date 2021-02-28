"""Command-line interface for MESSAGEix-GLOBIOM model tools.

Every tool and script in this repository is accessible through this CLI. Scripts are
grouped into commands and sub-commands. For help on specific (sub)commands, use --help:

    \b
    mix-models --help
    mix-models cd-links --help
    mix-models cd-links run --help

The top-level options --platform, --model, and --scenario are used by commands that
access specific ixmp or message_ix scenarios; these can also be specified with --url.
"""
import sys
from pathlib import Path

import click

from message_ix_models.util.context import Context


# Main command group. The code in this function is ALWAYS executed, so it should only
# include tasks that are common to all CLI commands.
@click.group(help=__doc__)
@click.option(
    "--url", metavar="ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]", help="Scenario URL."
)
@click.option("--platform", metavar="PLATFORM", help="Configured platform name.")
@click.option(
    "--model", "model_name", metavar="MODEL", help="Model name for some commands."
)
@click.option(
    "--scenario",
    "scenario_name",
    metavar="SCENARIO",
    help="Scenario name for some commands.",
)
@click.option("--version", type=int, help="Scenario version.")
@click.option("--local-data", type=Path, help="Base path for local data.")
# commented pending migration of this code
# @common_params("verbose")
@click.pass_context
def main(click_ctx, **kwargs):
    # commented pending migration of this code
    # # Start timer
    # logging.mark_time(quiet=True)
    #
    # # Log to console
    # logging.setup(level="DEBUG" if kwargs.pop("verbose") else "INFO", console=True)

    # Use the first instance of the message_data.tools.cli.Context object. click carries
    # the object to subcommands decorated with @click.pass_obj
    click_ctx.obj = Context.only()

    # Handle command-line parameters
    click_ctx.obj.handle_cli_args(**kwargs)

    # Close any database connections when the CLI exits
    click_ctx.call_on_close(click_ctx.obj.close_db)


@main.command(hidden=True)
@click.pass_obj
def debug(ctx):
    """Hidden command for debugging."""
    # Print the local data path
    # print(ctx.local_data)


try:
    from message_data.cli import modules_with_cli as message_data_modules_with_cli
except ImportError:
    message_data_modules_with_cli = []

for name in message_data_modules_with_cli:  # pragma: no cover
    name = "message_data." + name
    __import__(name)
    main.add_command(getattr(sys.modules[name], "cli"))

    # TODO use this in the future
    # name = f"message_data.{name}.cli"
    # __import__(name)
    # main.add_command(sys.modules[name].main)
