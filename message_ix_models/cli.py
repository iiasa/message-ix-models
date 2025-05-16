"""Command-line interface for MESSAGEix-GLOBIOM model tools.

Every tool and script in this repository is accessible through this CLI. Scripts are
grouped into commands and sub-commands. For help on specific (sub)commands, use --help,
for instance:

    \b
    mix-models report --help
    mix-models ssp gen-structures --help

The top-level options --platform, --model, and --scenario are used by commands that
access specific MESSAGEix scenarios in a specific ixmp platform/database; these can also
be specified with --url.

For complete documentation, see
https://docs.messageix.org/projects/models/en/latest/cli.html
"""

import logging
import sys
from pathlib import Path

import click
from ixmp.cli import main as ixmp_cli

from message_ix_models.util._logging import flush, mark_time
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
@click.option("--platform", metavar="PLATFORM", help="ixmp platform name.")
@click.option(
    "--model", "model_name", metavar="MODEL", help="Model name for some commands."
)
@click.option(
    "--scenario",
    "scenario_name",
    metavar="SCENARIO",
    help="Scenario name for some commands.",
)
@click.option("--version", type=int, help="Scenario version for some commands.")
@click.option("--local-data", type=Path, help="Base path for local data.")
@common_params("verbose")
@click.pass_context
def main(click_ctx, **kwargs):
    # Start timer
    mark_time(quiet=True)

    # Check for a non-trivial execution of the CLI
    non_trivial = (
        not any(s in sys.argv for s in {"last-log", "--help"})
        and click_ctx.invoked_subcommand != "_test"
        and "pytest" not in sys.argv[0]
    )

    # Log to console: either DEBUG or INFO.
    # Don't start file logging for a non-trivial execution.
    setup_logging(level="DEBUG" if kwargs["verbose"] else "INFO", file=non_trivial)

    if "pytest" not in sys.argv[0]:
        log.debug("CLI invoked with:\n" + "\n  ".join(sys.argv))

    # Store the most recently created instance of message_ix_models.Context. click
    # carries this object to any subcommand decorated with @click.pass_obj.
    # NB this can't be Context.only(). When click.testing.CliRunner is used, there may
    #    already be ≥2 Context instances created elsewhere in the test session before
    #    this function is called to run CLI commands within the test session.
    click_ctx.obj = Context()

    # Handle command-line parameters
    click_ctx.obj.core.handle_cli_args(**kwargs)

    # Close any database connections when the CLI exits
    click_ctx.call_on_close(click_ctx.obj.core.close_db)

    # Ensure all log messages are handled
    click_ctx.call_on_close(flush)


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


@main.command("last-log")
@click.pass_obj
def last_log(ctx):
    """Show the location of the last log file, if any."""
    from platformdirs import user_log_path

    log_dir = user_log_path("message-ix-models")
    if log_files := sorted(log_dir.glob("*T*")):
        print(log_files[-1])


@main.command(hidden=True)
@click.pass_obj
def debug(ctx):
    """Hidden command for debugging."""
    # Print the local data path
    log.debug(ctx.local_data)


@main.group("_test", hidden=True)
def cli_test_group():
    """Hidden group of CLI commands.

    Other code which needs to test CLI behaviour **may** attach temporary/throw-away
    commands to this group and then invoke them using :func:`mix_models_cli`. This
    avoids the need to expose additional commands for testing purposes only.
    """


@cli_test_group.command("log-threads")
@click.argument("k", type=int)
@click.argument("N", type=int)
def _log_threads(k: int, n: int):
    # Emit many log records
    log = logging.getLogger("message_ix_models")
    for i in range(n):
        log.info(f"{k = } {i = }")


# Attach the ixmp "config" CLI
main.add_command(ixmp_cli.commands["config"])


#: List of submodules providing CLI (sub)commands accessible through `mix-models`.
#: Each of these should contain a function named ``cli`` decorated with @click.command
#: or @click.group.
submodules = [
    "message_ix_models.model.buildings.cli",
    "message_ix_models.model.cli",
    "message_ix_models.model.structure",
    "message_ix_models.model.transport.cli",
    "message_ix_models.model.water.cli",
    "message_ix_models.project.circeular.cli",
    "message_ix_models.project.edits.cli",
    "message_ix_models.project.navigate.cli",
    "message_ix_models.project.ssp.cli",
    "message_ix_models.report.cli",
    "message_ix_models.model.material.cli",
    "message_ix_models.testing.cli",
    "message_ix_models.util.pooch",
    "message_ix_models.util.slurm",
]

try:
    import message_data.cli
except ImportError:
    # message_data is not installed or contains some ImportError of its own
    import ixmp

    if ixmp.config.get("no message_data") is not True:
        print(
            "Warning: message_data is not installed or cannot be imported; see the "
            "documentation via --help"
        )

    # commented: Display verbose information for debugging
    # from traceback import format_exception
    #
    # etype, value, tb = sys.exc_info()
    # print(
    #     "",
    #     *format_exception(etype, value, tb, limit=-1, chain=False)[1:],
    #     sep="\n",
    # )
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


if __name__ == "__main__":
    main()
