import logging

import click
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


# allows to activate water module
@click.group("water")
@common_params("regions")
@click.pass_obj
def cli(context, regions):
    water_ini(context, regions)


def water_ini(context, regions):
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


_RCPS = ["no_climate", "6p0", "2p6"]
_REL = ["low", "med", "high"]


@cli.command("nexus")
@click.pass_obj
@click.option("--rcps", type=click.Choice(_RCPS))
@click.option("--rels", type=click.Choice(_REL))
@click.option(
    "--sdgs",
    is_flag=True,
    help="Defines whether water SDG measures are activated or not",
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
    import ixmp
    import message_ix

    from message_data.model.water.reporting import report_full

    sc = context.get_scenario()
    report_full(sc, sdgs)
