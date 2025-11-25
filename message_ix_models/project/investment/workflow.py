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
    """Extract SSP name from scenario name for file naming.

    Returns the SSP number (e.g., 'ssp2') if found, otherwise returns None.
    """
    import re

    # Look for SSPx or sspx pattern (case insensitive)
    match = re.search(r"(ssp\d+)", scenario_name.lower())
    if match:
        return match.group(1)

    # Return None if no SSP pattern found (caller will default to ssp2)
    return None


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


def load_base_scenario(context: Context, scenario: message_ix.Scenario | None = None) -> message_ix.Scenario:
    """Load the base scenario specified in context.dest_scenario, ignoring any input scenario.
    """
    # The workflow step has already set context.dest_scenario from the target parameter
    # This function updates context.scenario_info to match, then load the scenario
    if context.dest_scenario:
        # Update scenario_info from dest_scenario to load the correct scenario
        context.scenario_info.update(context.dest_scenario)
    base_scenario = context.get_scenario()
    log.info(f"Loaded base scenario (ignoring previous context): {base_scenario.url}")
    return base_scenario


def retrive_ori_inv_cost(platform_name=None, scenario_name=None, model_name=None):
    """
    Retrieve the original investment cost data from scenarios.
    Generate CSV files for each scenario in the list.
    Log the location of fixed effects regression data.

    Parameters
    ----------
    platform_name : str, optional
        Platform name to use when loading scenarios. Defaults to "ixmp-dev".
    scenario_name : list of str, optional
        List of scenario names to process.
    model_name : str, optional
        Model name to use when loading scenarios. Defaults to
        "MESSAGEix-GLOBIOM 2.0-M-R12 Investment".
    """
    import ixmp

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

    # Set defaults
    if platform_name is None:
        platform_name = "ixmp-dev"
    if model_name is None:
        model_name = "MESSAGEix-GLOBIOM 2.0-M-R12 Investment"
    if scenario_name is None:
        raise ValueError("scenario_name must be provided as a list")

    # Ensure scenario_name is a list
    if not isinstance(scenario_name, list):
        scenario_name = [scenario_name]

    # Get platform
    mp = ixmp.Platform(platform_name)

    output_dir = package_data_path("investment")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each scenario in the list
    for scen_name in scenario_name:
        try:
            # Load scenario
            scenario = message_ix.Scenario(mp, model_name, scen_name)
            log.info(f"Loaded scenario: {model_name}/{scen_name}")

            # Retrieve investment cost data from the scenario
            inv_cost_ori = scenario.par("inv_cost", filters={"technology": power_tec})

            # Extract SSP name for filename (default to ssp2 if no SSP found)
            ssp_name = _extract_ssp_name(scen_name)
            if ssp_name is None:
                # No SSP pattern found, default to ssp2
                ssp_name = "ssp2"
                log.info(
                    f"No SSP pattern found in {scen_name}, using ssp2 for filename"
                )

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


def build_coc(context, scenario):
    """Add new inv_cost parameters with disaggregated cost of capital."""

    import pandas as pd
    import re

    from message_ix_models.util import package_data_path

    log = logging.getLogger(__name__)

    # Load the inv_cost.csv file from the data directory
    data_dir = package_data_path("investment")

    # Get the scenario name (which is the combined scenario name)
    scenario_name = scenario.scenario

    # Extract SSP and CF parts from scenario name to match file names
    # Extract SSP part (ssp1, ssp2, etc.)
    ssp_match = re.search(r"(ssp\d+)", scenario_name.lower())
    if not ssp_match:
        raise ValueError(
            f"Could not extract SSP name from scenario: {scenario_name}. "
            f"Expected pattern like 'ssp1_*' or 'ssp2_*'"
        )
    ssp_name = ssp_match.group(1)
    
    # Dynamically extract CF patterns from available files
    inv_cost_files = list(data_dir.glob("inv_cost_*.csv"))
    cf_patterns = set()
    
    # Extract CF names from filenames (pattern: inv_cost_ssp*_{cf_name}.csv)
    for file_path in inv_cost_files:
        filename = file_path.stem  # Get filename without extension
        # Match pattern: inv_cost_ssp\d+_(.*)
        match = re.match(r"inv_cost_ssp\d+_(.+)", filename)
        if match:
            cf_patterns.add(match.group(1))
    
    if not cf_patterns:
        raise FileNotFoundError(
            f"No investment cost files found matching pattern 'inv_cost_ssp*_*.csv' in {data_dir}"
        )
    
    cf_patterns_sorted = sorted(cf_patterns, key=len, reverse=True)
    
    # Extract CF part from scenario name by matching against available patterns
    cf_name = None
    scenario_lower = scenario_name.lower()
    
    for cf_pattern in cf_patterns_sorted:
        if cf_pattern.lower() in scenario_lower:
            cf_name = cf_pattern
            break
    
    if not cf_name:
        raise ValueError(
            f"Could not extract CF name from scenario: {scenario_name}. "
            f"Available CF patterns: {sorted(cf_patterns)}. "
            f"Scenario name should contain one of these patterns."
        )
    
    # Construct the expected filename
    expected_filename = f"inv_cost_{ssp_name}_{cf_name}.csv"
    inv_cost_path = data_dir / expected_filename

    if not inv_cost_path.exists():
        raise FileNotFoundError(
            f"Investment cost file not found for scenario '{scenario_name}'. "
            f"Expected: {expected_filename} in {data_dir}. "
            f"Extracted SSP: {ssp_name}, CF: {cf_name}"
        )

    log.info(
        f"Loading investment cost data from {inv_cost_path.name} "
        f"for scenario {scenario_name} (matched: ssp={ssp_name}, cf={cf_name})"
    )
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
    model_orig = "MESSAGEix-GLOBIOM 2.0-M-R12 Investment"
    platform_name = "ixmp-dev"
    model_name = f"ixmp://{platform_name}/{model_orig}"

    # List of all starting scenario names
    scen_names = [
        "ssp1_1000f",
        "ssp2_1000f",
        "ssp3_1000f",
        "ssp4_1000f",
        "ssp5_1000f",
    ]

    # Retrieve investment cost from all scenarios
    # Pass the entire list of scenario names to the function
    # First create a dummy base step that loads a scenario
    # (needed because genno passes function as scenario when base=None)
    dummy_target = f"{model_name}/{scen_names[0]}"  # Use first scenario as dummy target
    wf.add_step(
        "base",
        target=dummy_target,
    )
    # Now add the actual step with the dummy base
    wf.add_step(
        "inv_cost_ori retrieved",
        "base",
        lambda _, __: retrive_ori_inv_cost(
            platform_name=platform_name,
            scenario_name=scen_names,
            model_name=model_orig,
        ),
    )

    # Fixed effects regression - processes all scenarios and creates combined output
    # Run regression for each scenario individually, but the function processes
    # all CSV files
    wf.add_step(
        "wacc_cf reg generated",
        "inv_cost_ori retrieved",
        lambda _, __: fe_regression() if fe_regression else None,
    )

    # Climate finance generation - processes all scenarios and creates combined output
    # Level of total CF is also generated in this step
    wf.add_step(
        "cf generated",
        "wacc_cf reg generated",
        lambda _, __: generate_cf() if generate_cf else None,
    )

    # WACC generation - processes all scenarios and creates combined output
    wf.add_step(
        "wacc generated",
        "cf generated",
        lambda _, __: generate_wacc() if generate_wacc else None,
    )

    # Investment cost generation - processes all scenarios and creates combined output
    wf.add_step(
        "inv_cost generated",
        "wacc generated",
        lambda _, __: generate_inv_cost() if generate_inv_cost else None,
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

            # Load the specified starting scenario instead of the one from previous dummy step
            wf.add_step(
                f"base specified {combined_scen_name}",
                "inv_cost generated",
                load_base_scenario,
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

    # Group each SSP
    ssp_names = []
    for scen_name in scen_names:
        ssp_name = _extract_ssp_name(scen_name)
        if ssp_name and ssp_name not in ssp_names:
            ssp_names.append(ssp_name)

    ssp_grouping_steps = []
    for ssp_name in ssp_names:
        ssp_cf_steps = []
        for scen_name in scen_names:
            if _extract_ssp_name(scen_name) == ssp_name:
                for cf_name in cf_names:
                    combined_scen_name = f"{scen_name}_{cf_name}"
                    ssp_cf_steps.append(f"coc reported {combined_scen_name}")

        ssp_step_name = f"coc reported {ssp_name}"
        wf.add_step(
            ssp_step_name,
            ssp_cf_steps,  # type: ignore[arg-type]
            lambda _, __: log_coordination(),
        )
        ssp_grouping_steps.append(ssp_step_name)

    # Final step that collects all SSP-level steps
    wf.add_step(
        "coc reported",
        ssp_grouping_steps,  # type: ignore[arg-type]
        lambda _, __: log_coordination(),
    )

    return wf
