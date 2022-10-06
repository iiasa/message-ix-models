import logging
from pathlib import Path

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import MESSAGE_DATA_PATH
from message_ix_models.workflow import Workflow

from . import SCENARIOS
from .report import gen_config

log = logging.getLogger(__name__)


def build_materials(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 2."""
    from message_data.model.material import build

    raise NotImplementedError(
        f"""Requires code on the material-R12-rebase branch.

Switch to that branch and run:

$ mix-models --url="ixmp://{scenario.platform.name}/{scenario.url}" --local-data "{MESSAGE_DATA_PATH}/data" material build --tag=NAVIGATE
"""  # noqa: E501
    )

    return build(scenario)


def build_transport(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 3."""
    from message_data.model.transport import build

    return build.main(context, scenario, fast=True)


def build_buildings(context: Context, scenario: Scenario) -> Scenario:
    """Workflow steps 5–7."""
    from message_data.model.buildings import Config, build_and_solve, sturm

    # Configure
    context.buildings = Config(
        max_iterations=1,
        sturm_method="Rscript",
        run_access=False,
        sturm_scenario=sturm.scenario_name(context.navigate_scenario),
    )
    return build_and_solve(context)


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

    key = "navigate all"
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

    return scenario


def solve(context, scenario):
    scenario.solve()
    return scenario


def generate(context: Context) -> Workflow:
    wf = Workflow(context)

    # Use the navigate_scenario setting, e.g. from the --scenario CLI option, to
    # construct target scenario names
    s = context.navigate_scenario or "NPi-act"

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
        clone=True,
    )
    # Step 4
    wf.add_step("MT solved", "MT built", solve)

    # Branch for different NAVIGATE T3.5 scenarios
    for s in SCENARIOS:
        # Steps 5–7
        wf.add_step(
            f"BMT {s} solved",
            "MT solved",
            build_buildings,
            target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
            navigate_scenario=s,
        )
        # Steps 8–10
        wf.add_step(f"report {s}", f"BMT {s} solved", report)

    wf.add("report all", *[f"report {s}" for s in SCENARIOS])

    return wf
