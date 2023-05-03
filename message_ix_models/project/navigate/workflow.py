import logging
import re
from dataclasses import replace
from pathlib import Path
from typing import Dict, Optional, Tuple

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import identify_nodes, private_data_path, replace_par_data
from message_ix_models.workflow import Workflow

from message_data.model import buildings
from message_data.projects.engage import workflow as engage

from . import CLIMATE_POLICY
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


def strip_policy_data(context: Context, scenario: Scenario) -> Scenario:
    """Remove policy data, i.e. ``bound_emission`` and ``tax_emission``.

    Currently unused.
    """
    with scenario.transact(message=__doc__):
        for name in ("bound_emission", "tax_emission"):
            scenario.remove_par(name, scenario.par(name))

    scenario.set_as_default()

    return scenario


def adjust_materials(
    context: Context, scenario: Scenario, *, WP6_production: str
) -> Scenario:
    """Apply adjustments from WP2 for WP6 scenarios."""
    from message_data.projects.navigate.wp2.util import (
        add_CCS_constraint,
        add_electrification_share,
        add_LED_setup,
        limit_h2,
    )

    if WP6_production == "advanced":
        add_LED_setup(scenario)
        add_electrification_share(scenario)
        limit_h2(scenario, "green")

    # Constrain CCS at 2.0 Gt [units?]
    add_CCS_constraint(scenario, 2.0, "upper")

    return scenario


def build_transport(context: Context, scenario: Scenario, **options) -> Scenario:
    """Workflow step 3."""
    from message_data.model.transport import build
    from message_data.tools.utilities import update_h2_blending

    # NB this could occur earlier in the workflow, e.g. before the "M built" step/
    #    build_materials(), above. However, since that step currently requires switching
    #    to a different branch, we keep it here so it can be run together with all the
    #    other steps on the current branch.
    update_h2_blending(scenario)

    # NB ditto above
    # Correct a known error in the base scenarios used for NAVIGATE: move
    # relation_activity entries for t=hp_gas_i from r=CO2_r_c to r=CO2_ind.
    replace_par_data(
        scenario,
        "relation_activity",
        dict(technology="hp_gas_i", relation="CO2_r_c"),
        dict(relation={"CO2_r_c": "CO2_ind"}),
    )

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


def _strip(scenario: Scenario):
    """Remove certain technologies identified as irrelevant to NAVIGATE."""
    from message_ix_models import Spec
    from message_ix_models.model import build

    # The spec only contains a list of technologies to remove
    tech_expr = re.compile("^RC(spec|therm)_")
    s = Spec()
    s.remove.set["technology"] = [
        t for t in get_codes("technology") if tech_expr.match(t.id)
    ]

    build.apply_spec(scenario, s, fast=True)


def build_solve_buildings(
    context: Context,
    scenario: Scenario,
    navigate_scenario: str,
    config: Optional[dict] = None,
) -> Scenario:
    """Workflow steps 5–7."""
    from message_data.model.buildings import sturm

    _strip(scenario)

    # Configure
    context.buildings = replace(
        BUILDINGS_CONFIG,
        sturm_scenario=sturm.scenario_name(navigate_scenario),
        **(config or {}),
    )

    return buildings.build_and_solve(context)


def report(
    context: Context, scenario: Scenario, other_scenario_info: Optional[Dict]
) -> Scenario:
    """Workflow steps 8–10."""
    from message_data.reporting import (
        _invoke_legacy_reporting,
        log_before,
        prepare_reporter,
        register,
    )

    # Identify other scenario for .navigate.report.callback
    context.navigate.copy_ts = other_scenario_info

    # Step 8
    register("projects.navigate")
    rep, _ = prepare_reporter(context)

    # Remove time-series data preceding the first model period
    key = "navigate remove ts"
    log_before(context, rep, key)
    rep.get(key)

    # Possibly copy time-series data for the period before the first model period; i.e.
    # 2020 in policy scenarios wherein the first_model_year is advanced to 2025.
    # NB this could also be done by making the "navigate remove ts" operation
    #    configurable, and assuming the time series data are already present in the
    #    current scenario, i.e. it has been cloned from `other_scenario_info`.
    key = "navigate copy ts"
    log_before(context, rep, key)
    rep.get(key)

    # Perform reporting for buildings, materials, and transport
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
        run_config="navigate-rc.yaml",
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


def tax_emission(context: Context, scenario: Scenario, price: float):
    """Workflow callable for :mod:`.tools.utilities.add_tax_emission`."""
    from message_data.projects.engage.runscript_main import (
        glb_co2_relation as RELATION_GLOBAL_CO2,
    )
    from message_data.tools.utilities import (
        add_AFOLU_CO2_accounting,
        add_alternative_TCE_accounting,
        add_CO2_emission_constraint,
        add_FFI_CO2_accounting,
        add_tax_emission,
    )

    try:
        scenario.remove_solution()
    except ValueError:
        pass

    add_tax_emission(scenario, price)

    # Identify the node codelist used by `scenario` (in case it is not set on `context`)
    context.model.regions = identify_nodes(scenario)

    # The following preparatory steps mirror those used in .engage.workflow.step_1()
    kw = dict(relation_name=RELATION_GLOBAL_CO2, reg=f"{context.model.regions}_GLB")

    # “Step1.3 Make changes required to run the ENGAGE setup” (per .runscript_main)
    log.info("Add separate FFI and AFOLU CO2 accounting")
    add_FFI_CO2_accounting(scenario, **kw)
    add_AFOLU_CO2_accounting(scenario, **kw)
    log.info("Add alternative TCE accounting")
    add_alternative_TCE_accounting(scenario)

    add_CO2_emission_constraint(scenario, **kw, constraint_value=0.0, type_rel="lower")

    return scenario


def iter_scenarios(
    context, filters
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """Iterate over filtered scenario codes while unpacking information.

    Yields a sequence of 4-tuples:

    1. Short label or identifier.
    2. The value of the ``navigate_climate_policy`` annotation, if any.
    3. The value of the ``navigate_T35_policy`` annotation, if any.
    4. The value of the ``navigate_WP6_production`` annotation, if any.

    Only the scenarios matching `filters` are included; see
    :func:`navigate.iter_scenario_codes`. Certain other scenarios are also skipped:

    - T3.5 scenarios with regionally differentiated carbon prices (indicated by "_d") in
      the name.
    - Older T6.2 scenario codes that appear in the official list but are not annotated.
    """
    from . import iter_scenario_codes

    for code in iter_scenario_codes(context, filters):
        # Short label from the code ID
        info = [re.match("(NAV_Dem|PC|PEP)-(.*)", code.id).group(2)]

        # Values for 3 annotations
        for name in ("climate_policy", "T35_policy", "WP6_production"):
            try:
                info.append(str(code.get_annotation(id=f"navigate_{name}").text))
            except KeyError:
                info.append(None)  # Annotation does not exist on `code`

        # Skip scenarios not implemented
        if info[0].endswith("_d") or (info[2] is info[3] is None):
            continue

        yield info


def generate(context: Context) -> Workflow:
    """Create the NAVIGATE workflow for T3.5, T6.1, and T6.2."""
    wf = Workflow(context)

    # Step 1
    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#7",
    )

    # NB some preparatory changes could be done at this point, but are instead done in
    # step 3; see build_transport():
    # - update_h2_blending().
    # - replace_par_data() to move hp_gas_i entries from CO2_r_c to CO2_ind.
    #
    # This is because the current materials code used to produce the "M built" scenario
    # is not yet merged to `dev` or `main`, and cannot be run at the same time as the
    # current code. So we typically use the "--from=" CLI option to start from the
    # target= scenario specified as the output of step 2, below.

    # Step 2

    M_built = {}
    for WP6_production, label, target in (
        (
            "default",
            "def",
            "MESSAGEix-GLOBIOM 1.1-M-R12-NAVIGATE/baseline_add_material#54",
        ),
        (
            "advanced",
            "adv",
            "MESSAGEix-GLOBIOM 1.1-M-R12-NAVIGATE/baseline_add_material#54",
        ),
        (None, "T3.5", "MESSAGEix-Materials/baseline_default_NAVIGATE"),
    ):
        M_built[WP6_production] = f"M {label} built"
        wf.add_step(M_built[WP6_production], "base", build_materials, target=target)

        # Strip data from tax_emission
        name = f"M {label} adjusted"
        wf.add_step(
            name,
            M_built[WP6_production],
            adjust_materials,
            clone=True,
            target=f"MESSAGEix-GLOBIOM 1.1-M-R12 (NAVIGATE)/{label}",
            WP6_production=WP6_production,
        )
        M_built[WP6_production] = name

    # Mapping from short IDs (`s`) to step names for results of step 7
    baseline_solved = {}

    # Steps 3–7 are only run for the "NPi" (baseline) climate policy
    filters = {
        "navigate_task": {"T3.5", "T6.1", "T6.2"},
        "navigate_climate_policy": "NPi",
    }
    for s, _, T35_policy, WP6_production in iter_scenarios(context, filters):
        base = M_built[WP6_production]

        # Steps 3–4
        variant = "M" + ("T" if context.navigate.transport else "")
        if context.navigate.transport:
            # Step 3
            name = f"{variant} {s} built"
            wf.add_step(
                name,
                M_built[WP6_production],
                build_transport,
                target=f"MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s}",
                clone=True,
                navigate_scenario=T35_policy,
            )
            base = name

        # Step 4
        wf.add_step(f"{variant} {s} solved", base, solve)
        base = f"{variant} {s} solved"

        # Steps 5–7
        variant = "B" + variant
        name = f"{variant} {s} solved"
        wf.add_step(
            name,
            base,
            build_solve_buildings,  # type: ignore
            target=f"MESSAGEix-GLOBIOM 1.1-{variant}-R12 (NAVIGATE)/{s}",
            clone=True,
            navigate_scenario=s,
        )

        # Store the step name as a starting point for climate policy steps, below
        baseline_solved[(T35_policy, WP6_production)] = name

    # Mapping from short IDs (`s`) to 2-tuples with:
    # 1. name of the final step in the policy sequence (if any)
    # 2. args for report(…, other_scenario_info=…) —either None or model/scenario name
    to_report = {}

    # Now iterate over all scenarios
    filters.pop("navigate_climate_policy")
    for s, climate_policy, T35_policy, WP6_production in iter_scenarios(
        context, filters
    ):
        # Identify the base scenario for the subsequent steps
        base = baseline_solved[(T35_policy, WP6_production)]

        # Select the indicated PolicyConfig object
        engage_policy_config = CLIMATE_POLICY.get(climate_policy)

        if isinstance(engage_policy_config, engage.PolicyConfig):
            # Add 0 or more steps for ENGAGE-style climate policy workflows
            # NB this can occur here so long as PolicyConfig.method = "calc" is NOT used
            #    by any of the objects in CLIMATE_POLICY. If "calc" appears, then
            #    engage.step_1 requires data which is currently only available from
            #    legacy reporting output, and the ENGAGE steps must take place after
            #    step 9 (running legacy reporting, below)
            name = engage.add_steps(wf, base=base, config=engage_policy_config, name=s)
        elif climate_policy == "Ctax":
            # Carbon tax; not an implementation of an ENGAGE climate policy
            wf.add_step(
                s,
                base,
                tax_emission,
                target=f"MESSAGEix-GLOBIOM 1.1-{variant}-R12 (NAVIGATE)/{s}",
                clone=dict(shift_first_model_year=2025),
                price=1000.0,
            )
            name = f"{s} solved"
            wf.add_step(name, s, solve)
        elif climate_policy == "20C T6.2":
            # Model and scenario name for the scenario produced by the base step
            # TODO this may not work if the previous step is a passthrough; make more
            #      robust
            info = wf.graph[base][0].scenario_info.copy()

            # Policy configuration
            # - Only ENGAGE step 3; i.e. not step 1 or 2.
            # - tax_emission_scenario changes the behaviour of engage.step_3: CO2 prices
            #   are retrieved from the indicated scenario.
            # - reserve_margin and solve are as in .navigate.__init__
            pc = engage.PolicyConfig(
                "20C",
                steps=[3],
                reserve_margin=False,
                solve=dict(model="MESSAGE", solve_options=dict(barcrossalg=2)),
                tax_emission_scenario=dict(
                    model=info["model"], scenario="PEP-2C-Default_ENGAGE_20C_step-2"
                ),
            )
            name = engage.add_steps(wf, base=base, config=pc, name=s)
        else:
            raise ValueError(climate_policy)

        if name == base:
            # No steps added, e.g. for "NPi" where engage.PolicyConfig.steps == []
            # Store the reporting information
            to_report[s] = (name, None)
            continue

        # At least 1 policy step was added; re-solve the buildings model(s) to pick up
        # price changes

        # Prior two steps added by engage.add_steps() or for Ctax
        step_m1 = wf.graph[name][0]
        step_m2 = wf.graph[wf.graph[name][2]][0]

        # Retrieve options on the solve step added by engage.add_steps()
        try:
            solve_kw = step_m1.kwargs["config"].solve
        except KeyError:
            solve_kw = dict()
        # Retrieve the target scenario from second-last step
        # TODO remove the need to look up step_m2 by allowing a callback to give the
        #      new model/scenario name
        target = step_m2.scenario_info

        # Create a new step with the same name and base, but invoking
        # MESSAGEix-Buildings instead.
        # - Override default buildings.Config.clone = True.
        # - Use the same Scenario.solve() keyword arguments as for the ENGAGE step, e.g.
        #   including "model" and ``solve_options["barcrossalg"]``.
        # - Use the same NAVIGATE scenario as the base scenario.
        re_solved = f"{name} again"
        wf.add_step(
            re_solved,
            name,
            build_solve_buildings,
            # Clone before this step
            target="{model}/{scenario}+B".format(**target),
            clone=True,
            # Keyword arguments for build_solve_buildings
            navigate_scenario=wf.graph[base][0].kwargs["navigate_scenario"],
            config=dict(solve=solve_kw),
        )

        # Store info to set up reporting, include other scenario from which to copy time
        # series data for y=2020
        to_report[s] = (re_solved, wf.graph[base][0].scenario_info)

    # Step names for results of step 9
    all_reported = []

    for s, (base, other_scenario_info) in to_report.items():
        # Step 8–9 for individual scenarios
        name = f"{s} reported"
        wf.add_step(name, base, report, other_scenario_info=other_scenario_info)
        all_reported.append(name)

        # Step 10 for individual scenarios
        wf.add_step(f"{s} prepped", name, prep_submission)

    # Steps 8–9 to report all scenarios
    wf.add_single("all reported", *all_reported)

    # Step 10 for all scenarios as a batch
    wf.add_single("all prepped", prep_submission, "context", *all_reported)

    return wf
