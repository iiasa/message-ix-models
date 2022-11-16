import logging
from pathlib import Path

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_data.projects.engage import workflow as engage

from . import CLIMATE_POLICY, iter_scenario_codes
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
    """Plain solve.

    The ENGAGE workflow steps use :func:`.engage.workflow.solve` instead.
    """
    scenario.solve()
    return scenario


def generate(context: Context) -> Workflow:
    """Create the NAVIGATE workflow."""
    wf = Workflow(context)

    # Use the navigate_scenario setting, e.g. from the --scenario CLI option, to
    # construct target scenario names
    s = context.navigate_scenario or "NAV_Dem-NPi-ref"

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

    BMT_solved = {}  # Step names for results of step 7
    reported_all = []  # Step IDs for results of step 8

    # Shorthand, used twice
    def iter_scenarios(filters):
        """Iterate over scenario codes while unpacking information."""
        for code in iter_scenario_codes(context, filters):
            # Unpack information from the code
            yield (
                code.id.split("NAV_Dem-")[1],  # Short label
                str(code.get_annotation(id="navigate-climate-policy").text),
                str(code.get_annotation(id="navigate-T3.5-policy").text),
            )

    # Steps 5–7 are only run for the "NPi" (baseline) climate policy
    filters = {"navigate-task": "T3.5", "navigate-climate-policy": "NPi"}
    for s, _, T35_policy in iter_scenarios(filters):
        # Name of the step
        name = f"BMT {s} solved"
        BMT_solved[T35_policy] = name

        # Steps 5–7
        wf.add_step(
            name,
            "MT solved",
            build_buildings,  # type: ignore
            target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
            navigate_scenario=s,
        )

    # Now iterate over all scenarios
    filters.pop("navigate-climate-policy")
    for s, climate_policy, T35_policy in iter_scenarios(filters):
        # Skip these for now
        if s.endswith("_d"):
            continue

        # Select right PolicyConfig object
        policy_config = CLIMATE_POLICY[climate_policy]
        # Identify the base scenario
        base = BMT_solved[T35_policy]

        # Add 0 or more steps for climate policies
        # NB this can occur here so long as PolicyConfig.method = "calc" is NOT used by
        #    any of the objects in ENGAGE_CONFIG. If "calc" appears, then engage.step_1
        #    requires data which is currently only available from legacy reporting
        #    output, and the ENGAGE steps must take place after step 9 (running legacy
        #    reporting, below)
        policy_solved = engage.add_steps(wf, base=base, config=policy_config, name=s)

        # Step 8–9 for individual scenarios
        reported = f"{s} reported"
        wf.add_step(reported, policy_solved, report)

        # Step 10 for individual scenarios
        wf.add_step(f"{s} prepped", reported, prep_submission)

        reported_all.append(reported)

    # Steps 8–9 for all scenarios
    wf.add_single("report all", *reported_all)

    # Step 10 for all scenarios
    wf.add_single("prep all", prep_submission, "context", *reported_all)

    return wf
