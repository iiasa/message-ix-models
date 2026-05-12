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

    scenario.solve(model, solve_options=solve_options, gams_args=["--cap_comm=0"])
    scenario.set_as_default()

    return scenario


def report(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Report the scenario."""
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

    try:
        scenario.check_out(timeseries_only=True)
    except ValueError:
        log.debug(f"Scenario {scenario.model}/{scenario.scenario} already checked out")

    _materials_report(scenario, region="R12_GLB", upload_ts=True)
    scenario.commit("Add materials reporting")

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
    name = wf.add_step("BM built", name, build_B, target=f"{url}BM_20260308", clone=c)
    name = wf.add_step("BM solved", name, solve)

    # This step requires the message_data branch `bmt_ssp`
    name = wf.add_step("BM reported", name, report)

    name = wf.add_step("BMT built", name, target=f"{url}BMT", clone=c)
    # calling build in transport workflow
    # and move build transport before buildings (to be added in next PR)

    name = wf.add_step("BMT solved", name, solve)
    name = wf.add_step("BMTX built", name, build_PM, target=f"{url}BMTX", clone=False)
    name = wf.add_step(
        "BMTX baseline solved", name, solve, target=f"{url}BMTX", clone=False
    )
    # NB At this point, clone and shift firstmodelyear to 2030

    return wf
