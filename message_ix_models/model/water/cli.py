from pathlib import Path

import click
#import message_ix

#from . import SCENARIO_INFO
from message_ix_models.util.click import common_params
from message_ix_models.util._logging import mark_time

# allows to activate water module
@click.group('water')
def cli(context):
    """MESSAGEix-Water module."""
    from .utils import read_config

    # Ensure transport model configuration is loaded
    read_config(context)

@cli.command()
@click.option(
    "--version",
    default="geam_ADV3TRAr2_BaseX2_0",
    metavar="VERSION",
    help="Model version to read.",
)
@click.option(
    "--dunno",
    default="geam_ADV3TRAr2_BaseX2_0",
    metavar="VERSION",
    help="temporarily here.",
)
#when have a function
@click.pass_obj
def solve(context):
    """Build and solve model.

    Use the --url option to specify the base scenario.
    """
    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error.
    output_scenario_name = {
        "baseline": "NoPolicy"
    }.get(context.scenario_info["scenario"])

    if context.scenario_info["model"] != "CD_Links_SSP2":
        print("WARNING: this code is not tested with this base scenario!")

    # Clone and set up
    scenario = build(
        context.get_scenario()
        .clone(model="", scenario=output_scenario_name)
    )

    # Solve
    scenario.solve()
