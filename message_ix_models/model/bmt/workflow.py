# The workflow mainly contains the steps to build bmt baseline,
# as well as the steps to apply policy scenario settings. See bmt-workflow.svg.
# Example cli command:
# mix-models bmt run --from="base" "glasgow+" --dry-run

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.utils import build_PM
from message_ix_models.model.buildings.build import build_B as build_B
from message_ix_models.model.material.data_util import add_macro_materials
from message_ix_models.workflow import Workflow

# from message_ix_models.model.transport.build import build as build_T

log = logging.getLogger(__name__)

# Functions for individual workflow steps


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    # Use default CPLEX options and update with custom settings
    solve_options = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    scenario.solve(model, solve_options=solve_options, gams_args=["--cap_comm=1"])
    scenario.set_as_default()

    return scenario


def _set_as_default(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Set the scenario as the default version and return it."""
    scenario.set_as_default()
    log.info("Set as default: ixmp://%s/%s", scenario.platform.name, scenario.url)
    return scenario


def _run_transport_report(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Run transport reporting on the scenario (same as transport workflow report)."""
    from message_ix_models.model.transport.key import report as k_report
    from message_ix_models.model.transport.report import callback as transport_callback
    from message_ix_models.report import prepare_reporter

    if transport_callback not in context.report.callback:
        context.report.register(transport_callback)
    rep, _ = prepare_reporter(context, scenario=scenario)
    rep.get(k_report.all)
    return scenario


def report(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Report the scenario (transport, materials, legacy that contains buildings)."""
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore

    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    report_config_check = scenario.par(
        "demand", filters={"commodity": "transport pax UREAM"}
    )
    run_config = (
        "materials_daccs_bmt_run_config.yaml"
        if report_config_check is not None and len(report_config_check) > 0
        else "materials_daccs_run_config.yaml"
    )
    log.info("Legacy report will use run_config=%s", run_config)

    def _legacy_report(scen):
        iamc_report_hackathon.report(
            mp=scen.platform,
            scen=scen,
            merge_hist=True,
            run_config=run_config,
        )

    # 1. Transport reporting (only if transport is built)
    if report_config_check is not None and len(report_config_check) > 0:
        try:
            _run_transport_report(context, scenario)
        except Exception as e:
            log.warning("Transport reporting skipped: %s", e)
    else:
        log.info("Transport reporting skipped (no transport pax demand).")

    # message_data/tools/post_processing/iamc_report_hackathon.py#L320-L342
    # legacy report merges scenario ts into each table by root
    # (3 main tables: Final Energy, Emissions, Energy Service)
    # TODO: so one needs to make sure that the transport report is mergable to
    # legacy report, which is basically already covered in the transport
    # test_report.py and transport parts in the 3 main tables of legacy report
    # are deactivated so that no double counting happens. In the next report PR,
    # ideally the B and T reporting can be handled in a way similar to
    # message_data/blob/navigate5.3/.../navigate/report.py#L290-L298

    # 2. Materials reporting
    try:
        scenario.check_out(timeseries_only=True)
    except ValueError:
        log.debug(f"Scenario {scenario.model}/{scenario.scenario} already checked out")
    df = _materials_report(scenario, region="R12_GLB", upload_ts=True)
    scenario.commit("Add materials reporting")
    del df

    # 3. Legacy reporting
    _legacy_report(scenario)

    return scenario


def prep_for_macro(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Prepare scenario for macro calibration.

    It adjusts (1) cat_year: removes initializeyear_macro and
    baseyear_macro so macro years are not active yet; and (2) sector set:
    removes rc_spec, rc_therm, and transport.
    Then solves with MESSAGE only. Run after cloning with shift_first_model_year=2030.
    """
    log.info("Preparing scenario for macro calibration.")
    scenario.set_as_default()

    # cat_year: drop macro init/base years
    df = scenario.set(
        "cat_year", {"type_year": ["initializeyear_macro", "baseyear_macro"]}
    )
    if df is not None and len(df) > 0:
        with scenario.transact("Remove init and baseyear for macro"):
            scenario.remove_set("cat_year", df)

    # sector: drop rc_spec, rc_therm, transport for BMT macro calibration
    sectors_to_remove = ["rc_spec", "rc_therm", "transport"]
    existing = set(scenario.set("sector").tolist())
    to_remove = [s for s in sectors_to_remove if s in existing]
    if to_remove:
        with scenario.transact("Remove rc_spec, rc_therm, transport from sector set"):
            scenario.remove_set("sector", to_remove)

    solve(context, scenario, model="MESSAGE")

    return scenario


def add_macro(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Update macro calibration file, add MACRO to the scenario,
    and solve with MACRO.
    """
    macro_file = getattr(context, "macro", None)
    if macro_file is None:
        bmt = getattr(context, "bmt", None)
        macro_file = bmt.get("macro") if isinstance(bmt, dict) else None
    if macro_file is None:
        ssp = getattr(context, "ssp", "SSP2")
        macro_file = f"macro_calibration_input_{ssp}_bmt.xlsx"
    log.info("Calibrating macro: updating %s and adding MACRO", macro_file)

    # basically even if the macro file contains more commodities than the scenario,
    # the update_macro_calib_file and add_macro_materials will only use the commodities
    # that are in the scenario
    scenario = add_macro_materials(scenario, macro_file)
    scenario.set_as_default()
    log.info("MACRO calibrated, now solving with MACRO")
    solve(context, scenario, model="MESSAGE-MACRO")

    return scenario


# Main BMT workflow


def generate(context: Context) -> Workflow:
    """Create the BMT-run workflow."""
    from message_ix_models.model.bmt.config import apply_bmt_config
    from message_ix_models.model.transport import workflow as transport

    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    apply_bmt_config(context)
    print(repr(context.asdict()))

    # Define model name
    model_name = context.bmt["model_name"]

    wf.add_step(
        "M",
        None,
        target="ixmp://ixmp-dev/SSP_SSP2_v6.5/baseline_DEFAULT_step_14",
        # target = f"{model_name}/baseline",
    )

    wf.add_step(
        "M cloned",
        "M",
        target=f"{model_name}/baseline_M",
        clone=dict(keep_solution=True),
    )

    wf.add_step(
        "M reported",
        "M cloned",
        report,
    )

    # Retrieve a 'Code' object with 'Annotations' that identify a particular
    # MESSAGEix-Transport configuration. For reference:
    # - .model.transport.config.CL_SCENARIO
    # - .model.transport.config.ScenarioCodeAnnotations
    # - .model.transport.config.Config.code
    # scenario_code = CL_SCENARIO.get()[f"M {context.ssp}"]

    # CL_SCENARIO (see .model.transport.config) is built from SDMX codelist
    # data/sdmx/IIASA_ECE_CL_TRANSPORT_SCENARIO(1.3.0).xml. From that version each
    # scenario has two codes: id "SSP{n}" (extra_modules=[]) and
    # "M SSP{n}" (extra_modules=["material"]).
    # Use the code without "M " to build transport without the material module.
    # scenario_code = CL_SCENARIO.get()[context.ssp]

    # Add step(s) on top of "M cloned" that build MESSAGEix-Transport. For reference:
    # - .model.transport.workflow.add_steps
    # - .model.workflow.from_codelist, which makes a similar call
    #
    # `name` is the name of the final step
    name = transport.add_steps(wf, "M reported", context.transport.code)

    # Clone to the URL desired for this workflow, at a step named "MT built".
    # After cloning, set the scenario as default so it is the one used by later steps.
    url = f"{model_name}/baseline_MT"
    wf.add_step("MT built", name, action=_set_as_default, target=url, clone=True)
    # TODO: check if Paul's action already has something to set as default

    # NB .model.transport.workflow.generate sets context.solve including
    #    model="MESSAGE", i.e. excluding MACRO, which is not expected to work on
    #    MESSAGEix-Transport.

    wf.add_step(
        "MT solved",
        "MT built",
        solve,
    )

    # Transport report step (from .model.transport.workflow: callback + "transport all")
    wf.add_step(
        "MT reported",
        "MT solved",
        report,
    )

    wf.add_step(
        "BMT built",
        "MT solved",
        build_B,
        target=f"{model_name}/baseline_BMT",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "BMT solved",
        "BMT built",
        solve,
    )

    wf.add_step(
        "BMTX built",
        "BMT solved",
        build_PM,
        target=f"{model_name}/baseline_BMTX",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "BMTX baseline solved",
        "BMTX built",
        solve,
    )

    wf.add_step(
        "BMT reported",
        "BMT solved",
        report,
    )

    wf.add_step(
        "BMTX prep macro",
        # "BMTX baseline solved",
        "BMT reported",
        prep_for_macro,
        target=f"{model_name}/baseline_BMTX_message",
        clone=dict(shift_first_model_year=2030),
        # make sure the scenario before this step is reported
    )
    wf.add_step(
        "BMTX baseline macro",
        "BMTX prep macro",
        add_macro,
        # the add_macro generate another target scenario with the suffix _macro
        # baseline_BMTX_message_macro
        target=f"{model_name}/baseline_BMTX_message_macro",
        # the scenario will not be cloned to the target,
        # but the next step will report from the target in the previous step
    )

    wf.add_step(
        "BMTX baseline macro reported",
        "BMTX baseline macro",
        report,
    )

    return wf
