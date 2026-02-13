# The workflow mainly contains the steps to build bmt baseline,
# as well as the steps to apply policy scenario settings. See bmt-workflow.svg.
# Example cli command:
# mix-models bmt run --from="base" "glasgow+" --dry-run

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.utils import build_PM
from message_ix_models.model.buildings.build import build_B as build_B
from message_ix_models.model.material.build import build_M as build_M
from message_ix_models.model.transport.build import main as transport_build
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
    solve_options.update({
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    })

    scenario.solve(model, solve_options=solve_options, gams_args=["--cap_comm=0"])
    scenario.set_as_default()

    return scenario

def report(context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Report the scenario."""
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore
    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    run_config = "materials_daccs_bmt_run_config.yaml"

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
    
    df = _materials_report(scenario, region="R12_GLB", upload_ts=True)
    scenario.commit("Add materials reporting")

    _legacy_report(scenario)
    
    return scenario


def build_T(context: Context, scenario: message_ix.Scenario, **kwargs) -> message_ix.Scenario:
    """Build MESSAGEix-Transport with material module enabled.
    
    This wrapper function modifies the context to include the "material" module
    in the transport config, then calls the main transport build function.
    """
    from message_ix_models.model.transport.config import Config
    
    # Ensure transport config exists in context
    if "transport" not in context:
        # Create transport config from context if it doesn't exist
        context.transport = Config.from_context(context)
    
    if context.transport._code is None:
        # Get SSP from context if available, default to "SSP2"
        ssp = getattr(context, "ssp", "SSP2")
        # Ensure it is in the format "SSP2", "SSP3", etc. (code IDs in CL_TRANSPORT_SCENARIO)
        if not ssp.startswith("SSP"):
            ssp = f"SSP{ssp}"
        context.transport.code = ssp
    
    # Add "material" module to the transport config
    context.transport.use_modules("material")
    
    # Call the main transport build function with modified context
    return transport_build(context, scenario, **kwargs)


# Main BMT workflow

def generate(context: Context) -> Workflow:
    """Create the BMT-run workflow."""
    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

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
        "B built",
        "M cloned",
        build_B,
        target=f"{model_name}/baseline_BM",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "BM solved",
        "B built",
        solve,
    )

    wf.add_step(
        "BM reported",
        "BM solved",
        report,
    )

    wf.add_step(
        "BMT built",
        "BM reported",
        build_T,
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
        clone=False,
    )

    wf.add_step(
        "BMTX baseline solved",
        "BMTX built",
        solve,
        target=f"{model_name}/baseline_BMTX",
        clone=False, # clone true here and shift fmy to 2030
    )

    return wf
