import logging
from pathlib import Path

import click

from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


def _modules_arg(context, param, value):
    """--module/-m: load extra reporting config from modules."""
    for m in filter(len, value.split(",")):
        name = context.report.register(m)
        log.info(f"Registered reporting from {name}")


@click.command(name="report")
@common_params("dry_run urls_from_file")
@click.option(
    "--config",
    "config_file",
    default="global",
    show_default=True,
    help="Path or stem for reporting config file.",
)
@click.option("--legacy", "-L", is_flag=True, help="Invoke 'legacy' reporting.")
@click.option(
    "--module",
    "-m",
    "modules",
    metavar="MODULES",
    default="",
    help="Add extra reporting for MODULES.",
    callback=_modules_arg,
)
@click.option(
    "--output",
    "-o",
    "cli_output",
    metavar="PATH",
    type=click.Path(writable=True, resolve_path=True, path_type=Path),
    help="Write output to PATH instead of console or default locations.",
)
@click.argument("key", default="message::default")
@click.pass_obj
def cli(context, config_file, legacy, cli_output, key, **kwargs):
    """Postprocess results.

    KEY defaults to the comprehensive report 'message::default', but may also be the
    name of a specific model quantity, e.g. 'output'.

    --config can give either the absolute path to a reporting configuration file, or
    the stem (i.e. name without .yaml extension) of a file in data/report.

    With --urls-from-file, read multiple Scenario identifiers from FILE, and report each
    one. In this usage, --output-path may only be a directory.

    If --verbose is given to the top-level CLI, the full description of the steps to
    calculate KEY is printed, as well as the entire result, if any.
    """
    from copy import deepcopy

    from message_ix_models.util._logging import mark_time

    from . import report
    from .config import Config

    # Update the reporting configuration from command-line parameters
    context.report = Config(
        from_file=config_file, key=key, cli_output=cli_output, _legacy=legacy
    )

    # Prepare a list of Context objects, each referring to one Scenario.
    contexts = []

    # - If --urls-from-file was given, then `context.scenarios` will contain a list of
    #   ScenarioInfo objects that point to the platform and (model, scenario, version)
    #   identifiers.
    # - Otherwise, the user gave identifiers for a single Scenario to the top-level CLI
    #   (--url/--platform/--model/--scenario/--version) and these are stored in
    #   `context.platform_info` and `context.scenario_info`.
    for si in context.scenarios or [context.scenario_info]:
        ctx = deepcopy(context)
        ctx.platform_info = dict(
            name=getattr(
                si, "platform_name", context.platform_info.get("name", "default")
            )
        )
        ctx.scenario_info = dict(si)
        contexts.append(ctx)

    for ctx in contexts:
        mark_time()
        report(ctx)

    mark_time()
