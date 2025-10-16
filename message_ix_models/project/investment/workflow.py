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


def _extract_ssp_name(scenario_name):
    """Extract SSP name from scenario name for file naming."""
    import re

    # Look for SSPx or sspx pattern (case insensitive)
    match = re.search(r"(ssp\d+)", scenario_name.lower())
    if match:
        return match.group(1)

    # Fallback: return the original name in lowercase
    return scenario_name.lower()


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


def log_coordination():
    """Simple coordination function that just logs completion."""
    log.info("All prerequisite steps completed - proceeding to next phase")
    # Return None to indicate this step doesn't produce a scenario
    return None


def retrive_ori_inv_cost(context, scenario):
    """
    Retrieve the original investment cost data from the current scenario.
    Generate a CSV file for this specific scenario.
    Log the location of fixed effects regression data."""

    power_tec = [
        "coal_ppl",
        "ucoal_ppl",
        "coal_adv",
        "coal_adv_ccs",
        "igcc",
        "igcc_ccs",
        "foil_ppl",
        "loil_ppl",
        "loil_cc",
        "gas_ppl",
        "gas_ct",
        "gas_cc",
        "gas_cc_ccs",
        "bio_ppl",
        "bio_istig",
        "bio_istig_ccs",
        "geo_ppl",
        "solar_res1",
        "solar_res2",
        "solar_res3",
        "solar_res4",
        "solar_res5",
        "solar_res6",
        "solar_res7",
        "solar_res8",
        "solar_res_RT1",
        "solar_res_RT2",
        "solar_res_RT3",
        "solar_res_RT4",
        "solar_res_RT5",
        "solar_res_RT6",
        "solar_res_RT7",
        "solar_res_RT8",
        "csp_sm1_res1",
        "csp_sm1_res2",
        "csp_sm1_res3",
        "csp_sm1_res4",
        "csp_sm1_res5",
        "csp_sm1_res6",
        "csp_sm1_res7",
        "wind_res1",
        "wind_res2",
        "wind_res3",
        "wind_res4",
        "wind_ref1",
        "wind_ref2",
        "wind_ref3",
        "wind_ref4",
        "wind_ref5",
        "nuc_lc",
        "nuc_hc",
        "nuc_fbr",
    ]

    from message_ix_models.util import package_data_path, private_data_path

    output_dir = package_data_path("investment")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get scenario name from the current scenario
    scen_name = scenario.scenario

    try:
        # Retrieve investment cost data from the current scenario
        inv_cost_ori = scenario.par("inv_cost", filters={"technology": power_tec})

        # Extract SSP name for filename
        ssp_name = _extract_ssp_name(scen_name)

        # Generate scenario-specific filename
        output_path = output_dir / f"{ssp_name}_inv_cost_ori.csv"
        inv_cost_ori.to_csv(output_path, index=False)
        log.info(
            f"Original investment cost data for {scen_name} saved to: {output_path}"
        )

    except Exception as e:
        log.error(f"Failed to process scenario {scen_name}: {e}")
        raise

    reg_data_path = private_data_path("coc", "Reg_data.xlsx")
    log.info(f"Fixed effects regression data located at: {reg_data_path}")
    return scenario


def build_coc(context, scenario):
    """Add new inv_cost parameters with disaggregated cost of capital."""

    import pandas as pd

    from message_ix_models.util import package_data_path

    log = logging.getLogger(__name__)

    # Load the inv_cost.csv file from the data directory
    data_dir = package_data_path("investment")

    # Get the scenario name (which is the combined scenario name)
    scenario_name = scenario.scenario

    # Try to find the investment cost file for this specific scenario
    # The file should be named: inv_cost_{scenario_name}.csv
    inv_cost_path = data_dir / f"inv_cost_{scenario_name}.csv"

    if not inv_cost_path.exists():
        inv_cost_files = list(data_dir.glob("inv_cost_*.csv"))
        if inv_cost_files:
            # Use the first file found (could be made configurable)
            inv_cost_path = inv_cost_files[0]
            log.info(f"Using individual investment cost file: {inv_cost_path}")
        else:
            raise FileNotFoundError(f"No investment cost files found in {data_dir}")

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

    # Define model name and scenario list
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 2.0-M-R12 Investment"
    # List of all starting scenarios
    scen_names = [
        "ssp1_1000f",
        "ssp2_1000f",
        "ssp3_1000f",
        "ssp4_1000f",
        "ssp5_1000f",
    ]

    # Add individual base scenario steps (no dependencies)
    for scen_name in scen_names:
        wf.add_step(
            f"base {scen_name}",
            target=f"{model_name}/{scen_name}",
        )

    # Retrieve investment cost from each scenario individually
    for scen_name in scen_names:
        wf.add_step(
            f"inv_cost_ori retrieved {scen_name}",
            f"base {scen_name}",
            retrive_ori_inv_cost,
            target=f"{model_name}/{scen_name}",
        )

    inv_cost_ori_steps = []
    for scen_name in scen_names:
        inv_cost_ori_steps.append(f"inv_cost_ori retrieved {scen_name}")
    wf.add_step(
        "inv_cost_ori retrieved",
        inv_cost_ori_steps,  # type: ignore[arg-type]
        log_coordination,
        dummy=True,
    )

    # Fixed effects regression - processes all scenarios and creates combined output
    # Run regression for each scenario individually, but the function processes
    # all CSV files
    wf.add_step(
        "wacc_cf reg generated",
        "inv_cost_ori retrieved",
        fe_regression,
        dummy=True,
    )

    # Climate finance generation - processes all scenarios and creates combined output
    # Level of total CF is also generated in this step
    wf.add_step(
        "cf generated",
        "wacc_cf reg generated",
        generate_cf,
        dummy=True,
    )

    # WACC generation - processes all scenarios and creates combined output
    wf.add_step(
        "wacc generated",
        "cf generated",
        generate_wacc,
        dummy=True,
    )

    # Investment cost generation - processes all scenarios and creates combined output
    wf.add_step(
        "inv_cost generated",
        "wacc generated",
        generate_inv_cost,
        dummy=True,
    )

    # Individual scenario steps for the clone and subsequent operations
    # These will read from the combined outputs created above
    cf_names = [
        "locf",
        "hicf_his",
        "hicf_fair",
    ]

    for scen_name in scen_names:
        for cf_name in cf_names:
            # Create combined scenario name
            combined_scen_name = f"{scen_name}_{cf_name}"

            # Load the starting scenario for the next steps
            wf.add_step(
                f"base specified {combined_scen_name}",
                "inv_cost generated",
                target=f"{model_name}/{scen_name}",
            )

            # Clone base scenario
            wf.add_step(
                f"base cloned {combined_scen_name}",
                f"base specified {combined_scen_name}",
                target=f"{model_name}/{combined_scen_name}",
                clone=dict(keep_solution=False),
            )

            # Build CoC parameters
            wf.add_step(
                f"coc built {combined_scen_name}",
                f"base cloned {combined_scen_name}",
                build_coc,
                # target=f"{model_name}/{combined_scen_name}",
            )

            # Solve the scenario
            wf.add_step(
                f"coc solved {combined_scen_name}",
                f"coc built {combined_scen_name}",
                solve,
                # target=f"{model_name}/{combined_scen_name}",
            )

            # Generate reports
            wf.add_step(
                f"coc reported {combined_scen_name}",
                f"coc solved {combined_scen_name}",
                report,
                # target=f"{model_name}/{combined_scen_name}",
            )

    # Collect all coc reported steps
    coc_reported_steps = []
    for scen_name in scen_names:
        for cf_name in cf_names:
            combined_scen_name = f"{scen_name}_{cf_name}"
            coc_reported_steps.append(f"coc reported {combined_scen_name}")
    wf.add_step(
        "coc reported",
        coc_reported_steps,  # type: ignore[arg-type]
        log_coordination,
        # dummy=True,
    )

    return wf
