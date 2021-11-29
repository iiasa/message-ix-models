from typing import Mapping

import click
import logging

# from .build import apply_spec
# from message_data.tools import ScenarioInfo
from message_ix_models import ScenarioInfo
from message_ix_models.model.build import apply_spec
from message_ix_models.util.context import Context
from message_ix_models.util import add_par_data

# from .data import add_data
from .data_util import modify_demand_and_hist_activity
from .data_util import add_emission_accounting
from .util import read_config


log = logging.getLogger(__name__)


def build(scenario):
    """Set up materials accounting on `scenario`."""
    # Get the specification
    spec = get_spec()

    # Apply to the base scenario
    apply_spec(scenario, spec, add_data)  # dry_run=True

    # Adjust exogenous energy demand to incorporate the endogenized sectors
    # Adjust the historical activity of the usefyl level industry technologies
    modify_demand_and_hist_activity(scenario)
    add_emission_accounting(scenario)

    return scenario


# add as needed/implemented
SPEC_LIST = [
    "generic",
    "common",
    "steel",
    "cement",
    "aluminum",
    "petro_chemicals",
    "buildings",
    "power_sector",
]


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for materials accounting."""
    require = ScenarioInfo()
    add = ScenarioInfo()
    remove = ScenarioInfo()

    # Load configuration
    # context = Context.get_instance(-1)
    context = read_config()

    # Update the ScenarioInfo objects with required and new set elements
    for type in SPEC_LIST:
        for set_name, config in context["material"][type].items():
            # for cat_name, detail in config.items():
            # Required elements
            require.set[set_name].extend(config.get("require", []))

            # Elements to add
            add.set[set_name].extend(config.get("add", []))

            # Elements to remove
            remove.set[set_name].extend(config.get("remove", []))

    return dict(require=require, add=add, remove=remove)


# Group to allow for multiple CLI subcommands under "material"
@click.group("material")
def cli():
    """Model with materials accounting."""


@cli.command("create-bare")
@click.option("--regions", type=click.Choice(["China", "R11", "R14"]))
@click.option("--dry_run", "-n", is_flag=True, help="Only show what would be done.")
@click.pass_obj
def create_bare(context, regions, dry_run):
    """Create the RES from scratch."""
    from message_data.model.bare import create_res

    if regions:
        context.regions = regions

    # to allow historical years
    context.period_start = 1980

    # Otherwise it can not find the path to read the yaml files..
    # context.metadata_path = context.metadata_path / "data"

    scen = create_res(context)
    build(scen)

    # Solve
    if not dry_run:
        scen.solve()


@cli.command("build")
@click.option(
    "--datafile",
    default="Global_steel_cement_MESSAGE.xlsx",
    metavar="INPUT",
    help="File name for external data input",
)
@click.option("--tag", default="", help="Suffix to the scenario name")
@click.pass_obj
def solve(context, datafile, tag):
    """Build model.

    Use the --url option to specify the base scenario.
    """
    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error.
    output_scenario_name = {
        "baseline": "NoPolicy",
        "baseline_macro": "NoPolicy",
        "baseline_new": "NoPolicy",
        "baseline_new_macro": "NoPolicy",
        "NPi2020-con-prim-dir-ncr": "NPi",
        "NPi2020_1000-con-prim-dir-ncr": "NPi2020_1000",
        "NPi2020_400-con-prim-dir-ncr": "NPi2020_400",
        # "DIAG-C30-const_E414": "baseline_test",
    }.get(context.scenario_info["scenario"])

    # context.metadata_path = context.metadata_path / "data"
    context.datafile = datafile

    if context.scenario_info["model"] != "CD_Links_SSP2":
        print("WARNING: this code is not tested with this base scenario!")

    # Clone and set up
    scenario = build(
        context.get_scenario().clone(
            model="MESSAGEix-Materials", scenario=output_scenario_name + "_" + tag
        )
    )

    # Set the latest version as default
    scenario.set_as_default()

    # Solve
    # scenario.solve()


@cli.command("solve")
@click.option("--scenario_name", default="NoPolicy")
@click.option("--model_name", default="MESSAGEix-Materials")
@click.option(
    "--datafile",
    default="Global_steel_cement_MESSAGE.xlsx",
    metavar="INPUT",
    help="File name for external data input",
)
@click.pass_obj
# @click.pass_obj
def solve_scen(context, datafile, model_name, scenario_name):
    """Build model.

    Use the --url option to specify the base scenario.
    """
    # Clone and set up
    from message_ix import Scenario

    scenario = Scenario(context.get_platform(), model_name, scenario_name)

    if scenario.has_solution():
        scenario.remove_solution()

    # Solve
    scenario.solve()


@cli.command("report-1")
@click.option(
    "--old_reporting",
    default=False,
    help="If True old reporting is merged with the new variables.",
)
@click.option("--scenario_name", default="NoPolicy")
@click.option("--model_name", default="MESSAGEix-Materials")
# @click.pass_obj
def run_reporting(old_reporting, scenario_name, model_name):
    from message_data.reporting.materials.reporting import report
    from message_ix import Scenario
    from ixmp import Platform

    print(model_name)
    mp = Platform()
    scenario = Scenario(mp, model_name, scenario_name)
    report(scenario, old_reporting)


@cli.command("report-2")
@click.option("--scenario_name", default="NoPolicy")
@click.option("--model_name", default="MESSAGEix-Materials")
# @click.pass_obj
def run_old_reporting(scenario_name, model_name):
    from message_ix import Scenario
    from ixmp import Platform
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as reporting,
    )

    base_model = model_name
    scen_name = scenario_name

    print(model_name)
    print(scenario_name)
    mp = Platform()
    scenario = Scenario(mp, model_name, scenario_name)

    reporting(
        mp, scenario, "False", base_model, scen_name, merge_hist=True, merge_ts=True
    )


from .data_cement import gen_data_cement
from .data_steel import gen_data_steel
from .data_aluminum import gen_data_aluminum
from .data_generic import gen_data_generic
from .data_petro import gen_data_petro_chemicals
from .data_buildings import gen_data_buildings
from .data_power_sector import gen_data_power_sector


DATA_FUNCTIONS = [
    # gen_data_buildings,
    gen_data_steel,
    gen_data_cement,
    gen_data_aluminum,
    gen_data_petro_chemicals,
    gen_data_generic,
    gen_data_power_sector,
]


# Try to handle multiple data input functions from different materials
def add_data(scenario, dry_run=False):
    """Populate `scenario` with MESSAGEix-Materials data."""
    # Information about `scenario`
    info = ScenarioInfo(scenario)

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")
    if {"World", "R12_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R12_GLB' from node list for data generation")
        info.set["node"].remove("R12_GLB")

    for func in DATA_FUNCTIONS:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        add_par_data(scenario, func(scenario), dry_run=dry_run)

    log.info("done")
