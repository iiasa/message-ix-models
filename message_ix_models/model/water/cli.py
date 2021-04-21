from pathlib import Path

import click
#import message_ix

#from . import SCENARIO_INFO
from message_ix_models.util.click import common_params
from message_ix_models.util._logging import mark_time

# allows to activate water module
@click.group('water')
@click.pass_obj
def cli(context):
    """MESSAGEix-Water module."""
    from .utils import read_config

    # Ensure water model configuration is loaded
    read_config(context)
    context.scenario_info=dict(model="ENGAGE_SSP2_v4.1.7", scenario="baseline_clone_test")
    context.output_scenario = context.scenario_info["scenario"] + '_water'


# @cli.command()
# @click.pass_obj
# def printscen(context):
#     """Print model and scen.
#     """
#     sc = context.get_scenario()
#     print(sc.scenario)

#when have a function
@cli.command()
@click.pass_obj
def solve(context):
    """Build and solve model.

    Use the --url option to specify the base scenario.
    """
    from .build import main as build
    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error.
    # output_scenario_name = {
    #     "baseline": "NoPolicy"
    # }.get(context.scenario_info["scenario"])
    output_scenario_name = context.output_scenario

    if context.scenario_info["model"] != "CD_Links_SSP2":
        print("WARNING: this code is not tested with this base scenario!")

    # Clone and set up
    scen = build(
        context.get_scenario()
        .clone(model="", scenario=output_scenario_name)
    )
    print(scen.model)
    # Solve
    # scenario.solve()
