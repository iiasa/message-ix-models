import logging
from pathlib import Path

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_data.projects.engage import workflow as engage

from . import SCENARIOS
from .report import gen_config

log = logging.getLogger(__name__)


def build_materials(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 2."""
    from message_data.model.material import build

    p = private_data_path("data")
    raise NotImplementedError(
        f"""Requires code on the material-R12-rebase branch.

Switch to that branch and run:

$ mix-models --url="ixmp://{scenario.platform.name}/{scenario.url}" --local-data "{p}" material build --tag=NAVIGATE
"""  # noqa: E501
    )

    return build(scenario)


def build_transport(context: Context, scenario: Scenario) -> Scenario:
    """Workflow step 3."""
    from message_data.model.transport import build

    return build.main(context, scenario, fast=True)


def build_buildings(
    context: Context, scenario: Scenario, navigate_scenario: str
) -> Scenario:
    """Workflow steps 5–7."""
    from message_data.model.buildings import Config, build_and_solve, sturm

    # Configure
    context.buildings = Config(
        max_iterations=1,
        sturm_method="Rscript",
        run_access=False,
        sturm_scenario=sturm.scenario_name(navigate_scenario),
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

    # Step 8
    register("projects.navigate")
    rep, _ = prepare_reporter(context)

    key = "remove ts data"
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
    # Configuration for legacy reporting; matches values in data/report/navigate.yaml
    # used when running the workflow steps manually.
    context.report["legacy"] = dict(
        merge_hist=True,
        merge_ts=True,
        # Specify output directory
        out_dir=context.get_local_path("report", "legacy"),
        # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a string
        #         containing "True" or "False" instead of an actual bool.
        ref_sol="False",
        run_config="materials.yaml",
    )
    _invoke_legacy_reporting(context)

    return scenario


def prep_submission(context: Context, *scenarios: Scenario):
    """Workflow step 10 (only)."""
    from message_data.tools.prep_submission import main

    # Generate configuration
    config = gen_config(
        context, Path("~/vc/iiasa/navigate-workflow").expanduser(), scenarios
    )
    # Invoke prep_submission
    main(config)

    log.info(f"Merged output written to {config.out_fil}")


def solve(context, scenario):
    scenario.solve()
    return scenario


#: Values from engage/config.yaml, expressed as objects.
#:
#: .. todo:: The low_dem_scen values appear to form a "waterfall", with each
#:    successively lower budget referencing the previous. Investigate how to run only
#:    some budgets without the previous ones.
ENGAGE_CONFIG = {
    # No climate policy
    engage.PolicyConfig(steps=[], label="foo"),
    # Only step 1
    engage.PolicyConfig(
        steps=[1], label=1000, budget=2449, low_dem_scen="EN_NPi2020_1200_step1"
    ),
    # All steps 1–3
    engage.PolicyConfig(label=1000, budget=2449, low_dem_scen="EN_NPi2020_1200_step1"),
    engage.PolicyConfig(label=600, budget=1288, low_dem_scen="EN_NPi2020_700_step1"),
}


def generate(context: Context) -> Workflow:
    """Create the NAVIGATE workflow."""
    wf = Workflow(context)

    # Use the navigate_scenario setting, e.g. from the --scenario CLI option, to
    # construct target scenario names
    s = context.navigate_scenario or "NPi-ref"

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

    # Step IDs for results of step 8
    reported_all = []

    # Branch for different NAVIGATE T3.5 scenarios
    for s in filter(lambda _s: _s != "baseline", SCENARIOS):
        # Steps 5–7
        # TODO incorporate different climate policies
        BMT_solved = f"BMT {s} solved"
        wf.add_step(
            BMT_solved,
            "MT solved",
            build_buildings,  # type: ignore
            target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
            navigate_scenario=s,
        )

        # Step 8–9 for individual scenarios
        BMT_reported = f"{s} reported"
        wf.add_step(BMT_reported, BMT_solved, report)

        # Branch for climate policies
        # NB this must occur here because engage.step_1 requires data which is currently
        #    only available from legacy reporting output.
        for label, config in zip("ABCD", ENGAGE_CONFIG):
            added = engage.add_steps(
                wf, base=BMT_reported, config=config, name=f"{s} {label}"
            )

            # Step 10 for individual scenarios
            wf.add_step(f"prep {s} {label}", added, prep_submission)

            reported_all.append(added)

    # Steps 8–9 for all scenarios
    wf.add_single("report all", *reported_all)

    # Step 10 for all scenarios
    wf.add_single("prep all", prep_submission, "context", *reported_all)

    return wf
