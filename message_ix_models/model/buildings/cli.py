"""Command-line interface for MESSAGEix-Buildings."""

import click

from message_ix_models.project.navigate.cli import _SCENARIO
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import common_params

from . import Config, build_and_solve, sturm


@click.group("buildings")
@click.pass_obj
def cli(context):
    """MESSAGEix-Buildings model."""


@cli.command("build-solve", params=[_SCENARIO])
@common_params("dest")
@click.option(
    "--climate-scenario",
    type=click.Choice(["BL", "2C"]),
    default="BL",
    help="Model/scenario name of reference climate scenario",
)
@click.option(
    "--iterations",
    "-n",
    "max_iterations",
    type=int,
    default=10,
    help="Maximum number of iterations.",
)
@click.option("--run-access", is_flag=True, help="Run the ACCESS model.")
@click.option(
    "--sturm",
    "sturm_method",
    type=click.Choice(["rpy2", "Rscript"]),
    help="Method to invoke STURM.",
)
@click.pass_obj
def build_and_solve_cmd(context, **kwargs):
    mark_time()

    # Handle CLI arguments
    kwargs.pop("dest", None)
    # Scenario (~input data) to use for STURM. Other possible values include "SSP".
    kwargs.update(sturm_scenario=sturm.scenario_name(kwargs.pop("navigate_scenario")))

    # Update configuration with remaining options/parameters
    context["buildings"] = Config(**kwargs)

    scenario = build_and_solve(context)

    scenario.platform.close_db()
