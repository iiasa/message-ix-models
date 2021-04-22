import logging
from pathlib import Path

import click

# import message_ix

# from . import SCENARIO_INFO
from message_ix_models.util.click import common_params
from message_ix_models.util._logging import mark_time

log = logging.getLogger(__name__)

# allows to activate water module
@click.group("water")
@common_params("regions")
@click.pass_obj
def cli(context, regions):
    """MESSAGEix-Water module.

    Model and scenario are set in this cli function
    together with context and regions"""
    from .utils import read_config

    # Ensure water model configuration is loaded
    read_config(context)
    context.scenario_info = dict(
        model="ENGAGE_SSP2_v4.1.7", scenario="baseline_clone_test"
    )
    context.output_scenario = context.scenario_info["scenario"] + "_water"

    # Handle --regions; use a sensible default for MESSAGEix-Transport
    if regions:
        print("INFO: Regions choice", regions)
    else:
        log.info("Use default --regions=R11")
        regions = "R11"
    context.regions = regions


@cli.command()
@common_params("regions")
@click.pass_obj
def cooling(context, regions):
    """Build and solve model with new cooling technologies.

    Use the --url option to specify the base scenario.
    """
    from .build import main as build

    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error.

    output_scenario_name = context.output_scenario

    if context.scenario_info["model"] != "CD_Links_SSP2":
        print("WARNING: this code is not tested with this base scenario!")

    # Clone and build
    scen = context.get_scenario().clone(model="", scenario=output_scenario_name)

    print(scen.scenario)
    # Build
    build(context, scen)

    # Solve
    scen.solve()
