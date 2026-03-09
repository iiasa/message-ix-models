"""Construct the Buildings-Materials-Transport workflow."""

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.utils import build_PM
from message_ix_models.model.buildings.build import main as build_B
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
    # the building reporting is now embedded in the legacy reporting

    def _legacy_report(scen):
        iamc_report_hackathon.report(
            mp=scen.platform,
            scen=scen,
            merge_hist=True,
            run_config=run_config,
        )

    # 1. Transport reporting
    # try:
    #     _run_transport_report(context, scenario)
    # except Exception as e:
    #     log.warning("Transport reporting skipped: %s", e)

    # https://github.com/iiasa/message_data/blob/bmt_dev/message_data/tools/
    # post_processing/iamc_report_hackathon.py#L320-L342
    # legacy report merges scenario ts into each table by root (Final Energy, Energy
    # Service, Transport|Storck)
    # TODO: so one needs to make sure that the transport report is mergable to legacy
    # report which is basically already covered in the transport test_report.py
    # also deactivate the transport part in the legacy report and check what Paul did in
    # NAVIGATE workflow

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
    name = transport.add_steps(wf, "M cloned", scenario_code)

    # Clone to the URL desired for this workflow, at a step named "MT built".
    # After cloning, set the scenario as default so it is the one used by later steps
    name = wf.add_step("MT built", name, _set_as_default, target=f"{url}MT", clone=True)
    # TODO: check if Paul's action already has something to set as default

    # NB .model.transport.workflow.generate sets context.solve including
    #    model="MESSAGE", i.e. excluding MACRO, which is not expected to work on
    #    MESSAGEix-Transport.

    name = wf.add_step("MT solved", name, solve)

    # Transport report step (from .model.transport.workflow: callback + "transport all")
    name = wf.add_step("MT reported", name, _run_transport_report)
    name = wf.add_step("BMT built", name, build_B, target=f"{url}BMT", clone=c)
    name = wf.add_step("BMT solved", name, solve)
    name = wf.add_step("BMTX built", name, build_PM, target=f"{url}BMTX", clone=False)
    name = wf.add_step(
        "BMTX baseline solved", name, solve, target=f"{url}BMTX", clone=False
    )
    # NB At this point, clone and shift firstmodelyear to 2030

    wf.add_step("BMTX baseline reported", name, report)

    return wf
