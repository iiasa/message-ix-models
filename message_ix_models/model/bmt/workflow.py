# The workflow mainly contains the steps to build bmt baseline,
# as well as the steps to apply policy scenario settings. See bmt-workflow.svg.
# Example cli command:
# mix-models bmt run --from="base" "glasgow+" --dry-run

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.utils import build_PM
from message_ix_models.model.buildings.build import build_B as build_B
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


# Main BMT workflow


def generate(context: Context) -> Workflow:
    """Create the BMT-run workflow."""
    from message_ix_models.model.bmt.config import load_buildings_config

    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Load BMT buildings config (paths, with_materials) for build_B
    context.buildings = load_buildings_config()

    # Define model name
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 2.2-BMT-R12"

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
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "BM built",
        "M cloned",
        build_B,
        target=f"{model_name}/baseline_BM_20260308",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "BM solved",
        "BM built",
        solve,
    )

    wf.add_step(
        "BM reported",
        "BM solved",
        report,  # require the message-data branch bmt_ssp
    )

    wf.add_step(
        "BMT built",
        "BM reported",
        target=f"{model_name}/baseline_BMT",
        clone=dict(keep_solution=False),
    )
    # calling build in transport workflow
    # and move build transport before buildings (to be added in next PR)

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
        clone=False,
    )

    wf.add_step(
        "BMTX baseline solved",
        "BMTX built",
        solve,
        target=f"{model_name}/baseline_BMTX",
        clone=False,  # clone true here and shift fmy to 2030
    )

    return wf
