import logging
from dataclasses import replace
from pathlib import Path
from typing import Optional

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_data.model import buildings
from message_data.projects.engage import workflow as engage

from . import CLIMATE_POLICY, iter_scenario_codes
from .report import gen_config

log = logging.getLogger(__name__)

# Functions for individual workflow steps


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


def build_transport(context: Context, scenario: Scenario, **options) -> Scenario:
    """Workflow step 3."""
    from message_data.model.transport import build
    from message_data.tools.utilities import update_h2_blending

    # NB this could occur earlier in the workflow, e.g. before the "M built" step/
    #    build_materials(), above. However, since that step currently requires switching
    #    to a different branch, we keep it here so it can be run together with all the
    #    other steps on the current branch.
    update_h2_blending(scenario)

    options.setdefault("fast", True)
    return build.main(context, scenario, options)


#: Common settings to use when invoking MESSAGEix-Buildings. The value for
#: "sturm_scenario" is a placeholder, replaced in :func:`build_solve_buildings`.
BUILDINGS_CONFIG = buildings.Config(
    max_iterations=1,
    run_access=False,
    sturm_method="Rscript",
    sturm_scenario="",
)


def build_solve_buildings(
    context: Context,
    scenario: Scenario,
    navigate_scenario: str,
    config: Optional[dict] = None,
) -> Scenario:
    """Workflow steps 5–7."""
    from message_data.model.buildings import sturm

    # Configure
    context.buildings = replace(
        BUILDINGS_CONFIG,
        sturm_scenario=sturm.scenario_name(navigate_scenario),
        **(config or {}),
    )

    return buildings.build_and_solve(context)


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
    context.buildings = BUILDINGS_CONFIG
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


def tax_emission(context, scenario, price: float):
    """Workflow callable for :mod:`.tools.utilities.add_tax_emission`."""
    from message_data.tools.utilities import add_tax_emission

    try:
        scenario.remove_solution()
    except ValueError:
        pass

    add_tax_emission(scenario, price)

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

    # Steps 3–7 are only run for the "NPi" (baseline) climate policy
    filters = {"navigate-task": "T3.5", "navigate-climate-policy": "NPi"}
    for s, _, T35_policy in iter_scenarios(filters):
        # Skip these for now
        if s.endswith("_d"):
            continue

        # Step 3
        wf.add_step(
            f"MT {s} built",
            "M built",
            build_transport,
            target=f"MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s}",
            clone=True,
            navigate_scenario=T35_policy,
        )

        # Step 4
        wf.add_step(f"MT {s} solved", f"MT {s} built", solve)

        # Steps 5–7
        name = f"BMT {s} solved"
        wf.add_step(
            name,
            f"MT {s} solved",
            build_solve_buildings,  # type: ignore
            target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
            navigate_scenario=s,
        )

        # Store the step name as a starting point for climate policy steps, below
        BMT_solved[T35_policy] = name

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

        if not isinstance(policy_config, engage.PolicyConfig):
            assert climate_policy == "Ctax"
            # Carbon tax; not an implementation of an ENGAGE climate policy
            wf.add_step(
                s,
                base,
                tax_emission,
                target=f"MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s}",
                clone=dict(shift_first_model_year=2025),
                price=200.0,
            )
            name = f"{s} solved"
            wf.add_step(name, s, solve)
        else:
            # Add 0 or more steps for climate policies
            # NB this can occur here so long as PolicyConfig.method = "calc" is NOT used
            #    by any of the objects in ENGAGE_CONFIG. If "calc" appears, then
            #    engage.step_1 requires data which is currently only available from
            #    legacy reporting output, and the ENGAGE steps must take place after
            #    step 9 (running legacy reporting, below)
            name = engage.add_steps(wf, base=base, config=policy_config, name=s)

        if name == base or climate_policy == "Ctax":
            policy_solved = name
        else:
            # Re-solve with buildings at the last stage

            # Retrieve options on the solve step added by engage.add_steps()
            try:
                solve_kw = wf.graph[name][0].kwargs["config"].solve
            except KeyError:
                solve_kw = dict()

            # Create a new step with the same name and base, but invoking
            # MESSAGEix-Buildings instead.
            # - Override default buildings.Config.clone = True.
            # - Use the same Scenario.solve() keyword arguments as for the ENGAGE step,
            #   e.g. including "model" and ``solve_options["barcrossalg"]``.
            # - Use the same NAVIGATE scenario as the base scenario.
            policy_solved = f"{name} again"
            wf.add_step(
                policy_solved,
                name,
                build_solve_buildings,
                navigate_scenario=wf.graph[base][0].kwargs["navigate_scenario"],
                config=dict(clone=False, solve=solve_kw),
            )

        # Step 8–9 for individual scenarios
        reported = f"{s} reported"
        wf.add_step(reported, policy_solved, report)

        # Step 10 for individual scenarios
        wf.add_step(f"{s} prepped", reported, prep_submission)

        reported_all.append(reported)

    # Steps 8–9 for all scenarios in a batch
    wf.add_single("report all", *reported_all)

    # Step 10 for all scenarios in a batch
    wf.add_single("prep all", prep_submission, "context", *reported_all)

    return wf
