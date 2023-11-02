"""Command-line interface for MESSAGEix-GLOBIOM model tools.

Every tool and script in this repository is accessible through this CLI. Scripts are
grouped into commands and sub-commands. For help on specific (sub)commands, use --help,
e.g.:

    \b
    mix-models cd-links --help
    mix-models cd-links run --help

The top-level options --platform, --model, and --scenario are used by commands that
access specific message_ix scenarios; these can also be specified with --url.

For more information, see https://docs.messageix.org/projects/models2/en/latest/cli.html
"""
import logging
import sys
from pathlib import Path

import click
from ixmp.cli import main as ixmp_cli

from message_ix_models.util._logging import mark_time
from message_ix_models.util._logging import setup as setup_logging
from message_ix_models.util.click import common_params
from message_ix_models.util.context import Context

log = logging.getLogger(__name__)


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
@common_params("verbose")
@click.pass_context
def main(click_ctx, **kwargs):
    # Start timer
    mark_time(quiet=True)

    # Log to console
    setup_logging(level="DEBUG" if kwargs.pop("verbose") else "INFO", console=True)

    # Store the most recently created instance of message_ix_models.Context. click
    # carries this object to any subcommand decorated with @click.pass_obj.
    # NB this can't be Context.only(). When click.testing.CliRunner is used, there may
    #    already be ≥2 Context instances created elsewhere in the test session before
    #    this function is called to run CLI commands within the test session.
    click_ctx.obj = Context.get_instance(-1)

    # Handle command-line parameters
    click_ctx.obj.handle_cli_args(**kwargs)

    # Close any database connections when the CLI exits
    click_ctx.call_on_close(click_ctx.obj.close_db)


@main.command("export-test-data")
@click.option("--exclude", default="", help="Sheets to exclude.")
@click.option("--nodes", default="R11_AFR,R11_CPA", help="Nodes to include.")
@click.option("--techs", default="coal_ppl", help="Technologies to include.")
@click.pass_obj
def export_test_data_cmd(ctx, exclude, nodes, techs):
    """Prepare data for testing.

    Option values for --exclude, --nodes, and --techs must be comma-separated lists.
    """
    from message_ix_models.testing import export_test_data

    # Store CLI options on the Context
    ctx.export_exclude = list(filter(None, exclude.split(",")))  # Exclude empty string
    ctx.export_nodes = nodes.split(",")
    ctx.export_techs = techs.split(",")

    mark_time()

    # Export
    export_test_data(ctx)

    mark_time()


@main.command(hidden=True)
@click.pass_obj
def debug(ctx):
    """Hidden command for debugging."""
    # Print the local data path
    log.debug(ctx.local_data)


# Attach the ixmp "config" CLI
main.add_command(ixmp_cli.commands["config"])


#: List of submodules providing CLI (sub)commands accessible through `mix-models`.
#: Each of these should contain a function named ``cli`` decorated with @click.command
#: or @click.group.
submodules = [
    "message_ix_models.model.cli",
    "message_ix_models.model.snapshot",
    "message_ix_models.model.structure",
    "message_ix_models.model.water.cli",
    "message_ix_models.project.ssp",
    "message_ix_models.report.cli",
    "message_ix_models.model.material",
]

try:
    import message_data.cli
except ImportError:
    # message_data is not installed or contains some ImportError of its own
    from traceback import format_exception

    # Display information for debugging
    etype, value, tb = sys.exc_info()
    print("", *format_exception(etype, value, tb, limit=-1, chain=False)[1:], sep="\n")
else:  # pragma: no cover  (needs message_data)
    # Also add message_data submodules
    submodules.extend(
        f"message_data.{name}" for name in message_data.cli.MODULES_WITH_CLI
    )

for name in submodules:
    # Import the module and retrieve the click.Command object
    __import__(name)
    cmd = getattr(sys.modules[name], "cli")

    # Avoid replacing message-ix-models CLI with message_data CLI
    if cmd.name in main.commands:  # pragma: no cover  (needs message_data)
        log.warning(f"Skip {repr(cmd.name)} CLI from {repr(name)}; already defined")
        continue

    main.add_command(cmd)
