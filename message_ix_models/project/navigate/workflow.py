import logging
from pathlib import Path

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.workflow import Workflow

from .report import gen_config

log = logging.getLogger(__name__)


def build_materials(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 2."""
    from message_data.model.material import build

    raise NotImplementedError(
        f"""Requires code on the material-R12-rebase branch. Switch to that branch and run:

$ mix-models --url="ixmp://{scenario.platform.name}/{scenario.url}" --local-data "./data" material build --tag=NAVIGATE
"""  # noqa: E501
    )

    return build(scenario)


def build_transport(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 3."""
    from message_data.model.transport import build

    return build.main(context, scenario, fast=True)


def build_buildings(context: Context, scenario: Scenario) -> Scenario:
    """Workflow steps 5–7."""
    from message_data.model.buildings import cli

    # NB this invokes the CLI command function directly; preferably use a separate
    #    function that is also called by the CLI
    cli.build_and_solve(
        context,
        max_iterations=1,
        sturm_method="Rscript",
        run_access=True,
        navigate_scenario="baseline",
        # Defaults
        climate_scenario="BL",
    )

    # The CLI command function returns nothing; load the scenario for use by subsequent
    # steps
    mp = context.get_platform()
    return Scenario(mp, **context.dest_scenario)


def report(context: Context, scenario: Scenario) -> Scenario:
    """Workflow steps 8–10."""
    from message_data.reporting import (
        _invoke_legacy_reporting,
        log_before,
        prepare_reporter,
        register,
    )
    from message_data.tools import prep_submission

    # Step 8
    register("projects.navigate")
    rep, _ = prepare_reporter(context)

    key = "remove all ts data"
    log_before(context, rep, key)
    rep.get(key)

    key = "navigate bmt"
    log_before(context, rep, key)
    rep.get(key)

    # Display information about the result
    log.info(
        f"File output(s), if any, written under:\n{rep.graph['config']['output_path']}"
    )

    # Step 9
    _invoke_legacy_reporting(context)

    # Step 10
    f1 = Path("~/data/messageix/report/legacy/{s.url}.xlsx").expanduser()
    f2 = Path("~/vc/iiasa/navigate-workflow")
    config = gen_config(context, f1, f2)
    prep_submission.main(config)


def solve(context, scenario):
    scenario.solve()


def generate(context: Context) -> Workflow:
    wf = Workflow(context)

    s = context.navigate_scenario

    # Step 1
    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#7",
    )
    # Step 2
    wf.add_step(
        "M built",
        "base",
        build_materials,
        target="MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE",
    )
    # Step 3
    wf.add_step(
        "MT built",
        "M built",
        build_transport,
        target=f"MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s}",
    )
    # Step 4
    wf.add_step("MT solved", "MT built", solve)
    # Steps 5–7
    wf.add_step(
        "BMT solved",
        "MT solved",
        build_buildings,
        target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
    )
    # Steps 8–10
    wf.add_step("report", "BMT solved", report)

    return wf
