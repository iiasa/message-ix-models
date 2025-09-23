# The workflow contains the steps to add cost of capital in MESSAGE,
# as well as the steps to apply coc-related scenario settings. See coc-workflow.svg.
# Example cli command:
# mix-models coc run --from="base" "glasgow+" --dry-run

import importlib.util
import logging
from pathlib import Path

import message_ix

from message_ix_models import Context
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)


# Import FE_Regression from 1_FE_Regression.py to keep Shuting Fan's code
def _load_fe_regression():
    """Load FE_Regression function from 1_FE_Regression.py file."""
    current_dir = Path(__file__).parent
    file_path = current_dir / "1_FE_Regression.py"

    if not file_path.exists():
        log.warning(f"1_FE_Regression.py not found at {file_path}")
        return None

    # Create a module spec
    spec = importlib.util.spec_from_file_location("fe_regression_module", file_path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Return the main function (which is the FE_Regression function)
    return getattr(module, "main", None)


# Import Generate_CF from 2_Generate_CF.py to keep Shuting Fan's code
def _load_generate_cf():
    """Load Generate_CF function from 2_Generate_CF.py file."""
    current_dir = Path(__file__).parent
    file_path = current_dir / "2_Generate_CF.py"

    if not file_path.exists():
        log.warning(f"2_Generate_CF.py not found at {file_path}")
        return None

    # Create a module spec
    spec = importlib.util.spec_from_file_location("generate_cf_module", file_path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Return the main function (which is the Generate_CF function)
    return getattr(module, "main", None)


# Import Get_WACC from 3_WACC_Projection.py to keep Shuting Fan's code
def _load_generate_wacc():
    """Load Get_WACC function from 3_WACC_Projection.py file."""
    current_dir = Path(__file__).parent
    file_path = current_dir / "3_WACC_Projection.py"

    if not file_path.exists():
        log.warning(f"3_WACC_Projection.py not found at {file_path}")
        return None

    # Create a module spec
    spec = importlib.util.spec_from_file_location("get_wacc_module", file_path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Return the main function (which is the Get_WACC function)
    return getattr(module, "main", None)


# Load the functions
fe_regression = _load_fe_regression()
generate_cf = _load_generate_cf()
generate_wacc = _load_generate_wacc()


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
    scen_name = "Baseline_SSP2"

    wf.add_step(
        "base",
        None,
        log_input,
        target=f"{model_name}/{scen_name}",
    )

    wf.add_step(
        "wacc_cf reg generated",
        "base",
        fe_regression,
        target=f"{model_name}/{scen_name}",
    )

    # Tress for all SSPs and all hi/lo CF(cliamte finance) scenarios to be added later

    wf.add_step(
        "cf generated",
        "wacc_cf reg generated",
        generate_cf,
        target=f"{model_name}/{scen_name}",
    )

    wf.add_step(
        "wacc generated",
        "cf generated",
        generate_wacc,
        target=f"{model_name}/{scen_name}",
    )

    wf.add_step(
        "inv_cost generated",
        "wacc generated",
        check_context,
        target=f"{model_name}/{scen_name}",
    )

    wf.add_step(
        "coc built",
        "inv_cost generated",
        check_context,
        target=f"{model_name}/{scen_name}_coc_added",
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
