"""Command-line tools specific to the NAVIGATE project."""
import logging
from pathlib import Path

import click
from message_ix_models.util.click import store_context

log = logging.getLogger(__name__)


#: Codes for NAVIGATE T3.5 scenarios. These are abbreviated by removing "NAV_Dem-".
SCENARIOS = [
    "NPi-ref",
    "NPi-act",
    "NPi-tec",
    "NPi-ele",
    "NPi-all",
]

scenario_option = click.Option(
    ["-s", "--scenario", "navigate_scenario"],
    default="ref",
    type=click.Choice(SCENARIOS),
    callback=store_context,
    help="NAVIGATE T3.5 scenario ID.",
)


@click.group("navigate", params=[scenario_option])
@click.pass_obj
def cli(context):
    """NAVIGATE project."""


@cli.command("prep-submission")
@click.argument("f1", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("f2", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.pass_obj
def prep_submission(context, f1, f2):
    """Prepare data for NAVIGATE submission.

    F1 is the path to a reporting output file in .xlsx format.
    F2 is the base path of the NAVIGATE workflow repository.
    """
    from message_data.projects.navigate.report import gen_config
    from message_data.tools.prep_submission import main

    # Fixed values
    context.regions = "R12"

    # Generate a prep_submission.Config object
    config = gen_config(context, f1, f2)
    print(config)

    main(config)
