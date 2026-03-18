"""Construct the Buildings-Materials-Transport workflow."""

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.utils import build_PM
from message_ix_models.model.buildings.build import main as build_B
from message_ix_models.model.material.data_util import add_macro_materials
from message_ix_models.workflow import Workflow

# from message_ix_models.model.transport.build import build as build_T

log = logging.getLogger(__name__)

# Functions for individual workflow steps


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    from message_ix.common import DEFAULT_CPLEX_OPTIONS

    # Use default CPLEX options and update with custom settings
    solve_options = DEFAULT_CPLEX_OPTIONS.copy()
    solve_options.update(
        {
            "advind": 0,
            "lpmethod": 4,
            "threads": 4,
            "epopt": 1e-6,
            "scaind": -1,
            # "predual": 1,
            "barcrossalg": 0,
        }
    )

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
    """Report the scenario (transport, materials, legacy that contains buildings."""
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore

    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    run_config = "materials_daccs_bmt_run_config.yaml"
    # TODO: check if the scenario has transport tech, if it does not,
    # then skip the transport reporting, and use another config
    # the building reporting is now embedded in the legacy reporting

    def _legacy_report(scen):
        iamc_report_hackathon.report(
            mp=scen.platform,
            scen=scen,
            merge_hist=True,
            run_config=run_config,
        )

    # 1. Transport reporting
    try:
        _run_transport_report(context, scenario)
    except Exception as e:
        log.warning("Transport reporting skipped: %s", e)

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

    _materials_report(scenario, region="R12_GLB", upload_ts=True)
    scenario.commit("Add materials reporting")

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
    """Update MACRO calibration file; add MACRO to the scenario; solve with MACRO."""
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


def generate(context: Context) -> Workflow:
    """Create the BMT workflow.

    The workflow includes the following named steps:

    - "M": load a base scenario that already contains :doc:`/material/index`.
    - "M cloned": clone the above.
    - Add sectoral structure and data:

      - "BM built": build :doc:`/buildings/index` using :func:`.build_B`.
      - "BM solved": solve the above scenario.
      - "BM reported": report the solved scenario.
      - "BMT built": build :doc:`MESSAGEix-Transport </transport/index>` using
        :mod:`.transport.build`.
      - "BMT solved": solve the above scenario.
      - "BMTX built": add power-sector configuration for MESSAGEix-Materials using
        :func:`.build_PM`.
      - "BMTX baseline solved": solve the above scenario.

    Through pending or future pull requests, the workflow will be extended to include:

    - Policies such as emission budgets.
    - Dynamic price-demand feedback.

    .. todo:: Include a prepared version of :file:`bmt-workflow.svg` here.
    """
    from message_ix_models.model.bmt.config import load_buildings_config
    from message_ix_models.model.transport import workflow as transport
    from message_ix_models.model.transport.config import CL_SCENARIO

    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Load BMT buildings config (paths, with_materials) for build_B
    context.buildings = load_buildings_config()

    # Define model name
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 2.2-BMT-R12"
    # Template for formatting URLs
    url = model_name + "/baseline_"
    base_url = "ixmp://ixmp-dev/SSP_SSP2_v6.5/baseline_DEFAULT_step_14"

    # Common keyword argument for cloning
    c = dict(keep_solution=False)

    name = wf.add_step("M", None, target=base_url)
    name = wf.add_step("M cloned", name, target=f"{url}M", clone=c)
    name = wf.add_step("M reported", name, report)

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
    scenario_code = CL_SCENARIO.get()[context.ssp]

    # Add step(s) on top of "M cloned" that build MESSAGEix-Transport. For reference:
    # - .model.transport.workflow.add_steps
    # - .model.workflow.from_codelist, which makes a similar call
    #
    # `name` is the name of the final step
    name = transport.add_steps(wf, name, scenario_code)

    # Clone to the URL desired for this workflow, at a step named "MT built".
    # After cloning, set the scenario as default so it is the one used by later steps
    name = wf.add_step("MT built", name, _set_as_default, target=f"{url}MT", clone=True)
    # TODO: check if Paul's action already has something to set as default

    # NB .model.transport.workflow.generate sets context.solve including
    #    model="MESSAGE", i.e. excluding MACRO, which is not expected to work on
    #    MESSAGEix-Transport.

    name = wf.add_step("MT solved", name, solve)

    # Transport report step (from .model.transport.workflow: callback + "transport all")
    name = wf.add_step("MT reported", name, report)
    name = wf.add_step("BMT built", name, build_B, target=f"{url}BMT", clone=c)
    name = wf.add_step("BMT solved", name, solve)
    name = wf.add_step("BMTX built", name, build_PM, target=f"{url}BMTX", clone=False)
    name = wf.add_step("BMTX baseline solved", name, solve)
    name = wf.add_step("BMT reported", "BMT solved", report)

    name = wf.add_step(
        "BMTX prep macro",
        "BMTX baseline solved",
        prep_for_macro,
        target=f"{url}BMTX_message",
        clone=dict(shift_first_model_year=2030),
    )

    # the add_macro generate another target scenario with the suffix _macro
    # baseline_BMTX_message_macro
    wf.add_step("BMTX baseline macro", name, add_macro, target=f"{url}DEFAULT")

    return wf
