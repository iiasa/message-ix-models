import logging

import click

from message_ix_models.model.structure import get_codes
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


# allows to activate water module
@click.group("water-ix")
@common_params("regions")
@click.option("--time", help="Manually defined time")
@click.pass_obj
def cli(context, regions, time):
    """MESSAGEix-Water and Nexus variant."""
    water_ini(context, regions, time)


def water_ini(context, regions, time):
    """Add components of the MESSAGEix-Nexus module

    This function modifies model name & scenario name
    and verifies the region setup
    Parameters
    ----------
    context : `class`:message.Context
        Information about target Scenario.
    regions : str (if not defined already in context.regions)
        Specifies what region definition is used ['R11','R12','ISO3']
    """

    from .utils import read_config

    # Ensure water model configuration is loaded
    read_config(context)
    if not context.scenario_info:
        context.scenario_info.update(
            dict(model="ENGAGE_SSP2_v4.1.7", scenario="baseline_clone_test")
        )
    context.output_scenario = context.scenario_info["scenario"]
    context.output_model = context.scenario_info["model"]

    # Handle --regions; use a sensible default for MESSAGEix-Nexus
    if regions:
        log.info(f"Regions choice {regions}")
        if regions in ["R14", "R32", "RCP"]:
            log.warning(
                "the MESSAGEix-Nexus module might not be compatible"
                "with your 'regions' choice"
            )
    else:
        log.info("Use default --regions=R11")
        regions = "R11"
    # add an attribute to distinguish country models
    if regions in ["R11", "R12", "R14", "R32", "RCP"]:
        context.type_reg = "global"
    else:
        context.type_reg = "country"
    context.regions = regions

    # create a mapping ISO code :
    # region name, for other scripts
    # only needed for 1-country models
    nodes = get_codes(f"node/{context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    if context.type_reg == "country":
        map_ISO_c = {context.regions: nodes[0]}
        context.map_ISO_c = map_ISO_c
        log.info(f"mapping {context.map_ISO_c[context.regions]}")

    # deinfe the timestep
    if not time:
        sc_ref = context.get_scenario()
        time = sc_ref.set("time")
        sub_time = list(time[time != "year"])
        if len(sub_time) == 0:
            context.time = ["year"]

        else:
            context.time = sub_time
    else:
        context.time = list(time)
    log.info(f"Using the following time-step for the water module: {context.time}")

    # setting the time information in context


_RCPS = ["no_climate", "6p0", "2p6", "7p0"]
_REL = ["low", "med", "high"]


@cli.command("nexus")
@click.pass_obj
@click.option("--rcps", default="6p0", type=click.Choice(_RCPS))
@click.option("--rels", default="low", type=click.Choice(_REL))
@click.option(
    "--sdgs",
    is_flag=True,
    help="Defines whether water SDG measures are activated or not",
)
@click.option(
    "--macro",
    is_flag=True,
    help="Defines whether the model solves with macro",
)
@common_params("regions")
def nexus_cli(context, regions, rcps, sdgs, rels, macro=False):
    """
    Add basin structure connected to the energy sector and
    water balance linking different water demands to supply.
    """

    nexus(context, regions, rcps, sdgs, rels, macro)


def nexus(context, regions, rcps, sdgs, rels, macro=False):
    """Add basin structure connected to the energy sector and
    water balance linking different water demands to supply.

    Use the --url option to specify the base scenario.

    Parameters
    ----------
    context : `class`:message.Context
        Information about target Scenario.
    regions : str (if not defined already in context.regions)
        Specifies what region definition is used ['R11','R12','ISO3']
    RCP : str
        Specifies the climate scenario used ['no_climate','6p0','2p6']
    SDG : True/False
        Defines whether water SDG measures are activated or not
    REL: str
        Specifies the reliability of hydrological data ['low','mid','high']
    """
    # add input information to the class context
    context.nexus_set = "nexus"
    if not context.regions:
        context.regions = regions

    context.RCP = rcps
    context.SDG = sdgs
    context.REL = rels

    log.info(f"RCP assumption is {context.RCP}. SDG is {context.SDG}")

    from .build import main as build

    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error
    output_scenario_name = context.output_scenario + "_nexus"
    output_model_name = context.output_model

    # Clone and build
    sc_ref = context.get_scenario()
    scen = sc_ref.clone(
        model=output_model_name, scenario=output_scenario_name, keep_solution=False
    )
    log.info(
        f" clone from {sc_ref.model}.{sc_ref.scenario} to {scen.model}.{scen.scenario}"
    )
    # Exporting the built model (Scenario) to GAMS with an optional case name
    caseName = scen.model + "__" + scen.scenario + "__v" + str(scen.version)

    # Build
    build(context, scen)

    # Set scenario as default
    scen.set_as_default()

    # Solve
    if macro:
        scen.solve(
            model="MESSAGE-MACRO",
            solve_options={"lpmethod": "4", "scaind": "1"},
            case=caseName,
        )
    else:
        scen.solve(solve_options={"lpmethod": "4"}, case=caseName)

    # if options["report"]:
    #     # Also output diagnostic reports
    #     from message_data.model.water import report, run_old_reporting
    #
    #     old reporting
    #     run_old_reporting(scen)

    #     log.info(f"Report plots to {rep.graph['config']['output_dir']}")

    #     mark_time()


@cli.command("cooling")
@common_params("regions")
@click.option("--rcps", type=click.Choice(_RCPS))
@click.option("--rels", type=click.Choice(_REL))
@click.pass_obj
def cooling_cli(context, regions, rcps, rels):
    """Build and solve model with new cooling technologies."""
    cooling(context, regions, rcps, rels)


def cooling(context, regions, rcps, rels):
    """Build and solve model with new cooling technologies.

    Use the --url option to specify the base scenario.

    Parameters
    ----------
    context : `class`:message.Context
        Information about target Scenario.
    regions : str (if not defined already in context.regions)
        Specifies what region definition is used ['R11','R12','ISO3']
    RCP : str
        Specifies the climate scenario used ['no_climate','6p0','2p6']

    """
    context.nexus_set = "cooling"
    context.RCP = rcps
    context.REL = rels

    from .build import main as build

    # Determine the output scenario name based on the --url CLI option. If the
    # user did not give a recognized value, this raises an error.

    output_scenario_name = context.output_scenario + "_cooling"
    output_model_name = context.output_model

    # Clone and build
    scen = context.get_scenario().clone(
        model=output_model_name, scenario=output_scenario_name, keep_solution=False
    )

    print(scen.model)
    print(scen.scenario)

    # Exporting the built model (Scenario) to GAMS with an optional case name
    caseName = scen.model + "__" + scen.scenario + "__v" + str(scen.version)

    # Build
    build(context, scen)

    # Solve
    scen.solve(solve_options={"lpmethod": "4"}, case=caseName)


@cli.command("report")
@click.pass_obj
@click.option(
    "--sdgs",
    is_flag=True,
    help="Defines whether water SDG measures are activated or not",
)
@common_params("output_model")
def report_cli(context, output_model, sdgs):
    """function to run the water report_full from cli to the
    scenario defined by the user with --url

    Parameters
    ----------
    context : `class`:message.Context
        Information about target Scenario.
    output_model : str (optional, otherwise default args used)
        Specifies the model name of the scenarios which are run.
    """

    from message_ix_models.model.water.reporting import report_full

    sc = context.get_scenario()
    report_full(sc, sdgs)
