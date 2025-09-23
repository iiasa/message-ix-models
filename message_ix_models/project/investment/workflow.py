# The workflow contains the steps to add cost of capital in MESSAGE,
# as well as the steps to apply coc-related scenario settings. See coc-workflow.svg.
# Example cli command:
# mix-models coc run --from="base" "coc reported" --dry-run

import importlib.util
import logging
from pathlib import Path

import ixmp
import message_ix
from message_data.tools.post_processing import iamc_report_hackathon

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


# Import Generate_inv_cost from 4_Generate_inv_cost.py to keep Shuting Fan's code
def _load_generate_inv_cost():
    """Load Generate_inv_cost function from 4_Generate_inv_cost.py file."""
    current_dir = Path(__file__).parent
    file_path = current_dir / "4_Generate_inv_cost.py"

    if not file_path.exists():
        log.warning(f"4_Generate_inv_cost.py not found at {file_path}")
        return None

    # Create a module spec
    spec = importlib.util.spec_from_file_location("generate_inv_cost_module", file_path)
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    # Return the main function (which is the Generate_inv_cost function)
    return getattr(module, "main", None)


# Load the functions
fe_regression = _load_fe_regression()
generate_cf = _load_generate_cf()
generate_wacc = _load_generate_wacc()
generate_inv_cost = _load_generate_inv_cost()


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


def build_coc(context, scenario):
    """Add new inv_cost parameters with disaggregated cost of capital."""
    from pathlib import Path

    import pandas as pd

    log = logging.getLogger(__name__)

    # Load the inv_cost.csv file from the current directory
    current_dir = Path(__file__).parent
    inv_cost_path = current_dir / "inv_cost.csv"
    # TODO: maybe it is better to move the generated intermediate files to the data
    # folder

    log.info(f"Loading investment cost data from {inv_cost_path}")
    inv_cost_df = pd.read_csv(inv_cost_path)

    # Prepare the data for MESSAGEix
    inv_cost_par = inv_cost_df[
        ["node_loc", "technology", "year_vtg", "value", "unit"]
    ].copy()

    # Add the parameters to the scenario using transact
    with scenario.transact("Add investment cost parameters with CoC decomposition"):
        scenario.add_par("inv_cost", inv_cost_par)

    log.info("Investment cost parameters successfully added to scenario")

    return scenario


def report(
    context: Context,
    scenario: message_ix.Scenario,
) -> message_ix.Scenario:
    # Get platform name from context
    platform_name = context.platform_info["name"]

    mp = ixmp.Platform(platform_name, jvmargs=["-Xmx14G"])
    run_config = "materials_daccs_run_config.yaml"
    # the legacy reporting requires message-data branches based on ssp-dev branch

    iamc_report_hackathon.report(
        mp=mp,
        scen=scenario,
        merge_hist=True,
        run_config=run_config,
    )

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
        generate_inv_cost,
        target=f"{model_name}/{scen_name}",
    )

    # TODO: a short loop for all SSPs and all CF scenarios below
    wf.add_step(
        "base cloned",
        "inv_cost generated",
        target=f"{model_name}/{scen_name}_coc_added",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "coc built",
        "base cloned",
        build_coc,
        target=f"{model_name}/{scen_name}_coc_added",
    )

    wf.add_step(
        "coc solved",
        "coc built",
        solve,
        target=f"{model_name}/{scen_name}_coc_added",
    )

    wf.add_step(
        "coc reported",
        "coc solved",
        report,
        target=f"{model_name}/coc_added",
    )

    return wf
