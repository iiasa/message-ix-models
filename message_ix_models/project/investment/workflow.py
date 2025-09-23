# The workflow contains the steps to add cost of capital in MESSAGE,
# as well as the steps to apply coc-related scenario settings. See coc-workflow.svg.
# Example cli command:
# mix-models coc run --from="base" "glasgow+" --dry-run

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.project.investment.fe_regression import main as fe_regression
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)


# Functions for individual workflow steps


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    message_ix.models.DEFAULT_CPLEX_OPTIONS = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    # scenario.solve(model, gams_args=["--cap_comm=0"])
    scenario.solve(model)
    scenario.set_as_default()

    return scenario


def check_context(
    context: Context,
    scenario: message_ix.Scenario,
) -> message_ix.Scenario:
    context.print_contents()

    return scenario


def log_input(context, scenario):
    """Log the location of fixed effects regression data."""
    from message_ix_models.util import private_data_path

    reg_data_path = private_data_path("coc", "Reg_data.xlsx")
    log.info(f"Fixed effects regression data located at: {reg_data_path}")
    return scenario


# Main CoC workflow
def generate(context: Context) -> Workflow:
    """Create the CoC workflow."""
    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Define model name
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 2.0-M-R12 Investment"

    wf.add_step(
        "base",
        None,
        log_input,
        target=f"{model_name}/baseline",
    )

    wf.add_step(
        "wacc_cf reg generated",
        "base",
        fe_regression,
        target=f"{model_name}/baseline",
    )

    # Tress for all SSPs and all hi/lo CF(cliamte finance) scenarios to be added later

    wf.add_step(
        "cf generated",
        "wacc_cf reg generated",
        check_context,
        target=f"{model_name}/baseline",
    )

    wf.add_step(
        "wacc generated",
        "cf generated",
        check_context,
        target=f"{model_name}/baseline",
    )

    wf.add_step(
        "inv_cost generated",
        "wacc generated",
        check_context,
        target=f"{model_name}/baseline",
    )

    wf.add_step(
        "coc built",
        "inv_cost generated",
        check_context,
        target=f"{model_name}/coc_added",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "coc_scen solved",
        "coc built",
        solve,
        target=f"{model_name}/coc_added",
    )

    wf.add_step(
        "coc_scen reported",
        "coc_scen solved",
        check_context,
        target=f"{model_name}/coc_added",
    )

    return wf
