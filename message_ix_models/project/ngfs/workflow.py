# The workflow mainly contains the steps to build ngfs scenarios,
# as well as the steps to apply policy scenario settings. See ngfs-workflow.svg.
# Example cli command:
# mix-models ngfs run --from="base" "glasgow+" --dry-run

import logging
from pickle import TRUE

import message_ix # type: ignore

from message_ix_models import Context
from message_ix_models.workflow import Workflow
from message_ix_models.project.ngfs import interpolate_c_price
from message_ix_models.project.engage.workflow import step_1, step_2, step_3, PolicyConfig
from message_ix_models.model.material.util import update_macro_calib_file
from message_ix_models.model.material.data_util import add_macro_materials
# TODO: think about if it makes sense to integrate the interpolate_c_price function into the scenario runner

from genno import Key # type: ignore

log = logging.getLogger(__name__)

# Functions for individual workflow steps


def _get_ngfs_config(context):
    """Load and cache NGFS config from config.yaml.
    
    The config is stored in context.ngfs_config to avoid reloading.
    
    Parameters
    ----------
    context : Context
        Context object where config will be cached
        
    Returns
    -------
    dict
        The model config dictionary from config.yaml
    """
    if not hasattr(context, 'ngfs_config') or context.ngfs_config is None:
        from message_ix_models.util import private_data_path
        import yaml
        
        config_file = private_data_path('projects', 'ngfs', 'config.yaml')
        with open(config_file) as f:
            context.ngfs_config = yaml.safe_load(f)['MESSAGEix-GLOBIOM 2.2-NGFS-R12']
    
    return context.ngfs_config

def report(context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Report the scenario."""
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore
    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    run_config = "materials_daccs_run_config.yaml"
    # run_config = "materials_daccs_bmt_run_config.yaml" # TODO: replace with bmt config later

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

def placeholder(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario

def make_scenario_runner(context):
    """Create and configure a ScenarioRunner instance for NGFS workflows.
    
    This function sets up the necessary context attributes and creates a ScenarioRunner
    instance that is used to build and run NGFS policy scenarios. It also pre-populates
    the policy_baseline scenario if it doesn't already exist.
    """
    from message_data.model.scenario_runner import ScenarioRunner
    
    # Get biomass_trade setting from context, default to False if not set
    try:
        biomass_trade = context.biomass_trade
    except AttributeError:
        biomass_trade = False

    context.ssp = 'SSP2'
    context.run_reporting_only = False
    context.policy_data_file = 'ngfs_p6_policy_data.xlsx'
    context.policy_config = 'ngfs_p6_config.yaml'
    context.region_id = "R12"

    # Load config (cached in context for reuse)
    model_config = _get_ngfs_config(context)

    # Slack data is selected based on the slack scenario from config
    sr = ScenarioRunner(
        context,
        slack_data=model_config['policy_slacks'][model_config['slack_scn']][context.ssp],
        biomass_trade=biomass_trade,
    )

    # Pre-populate policy_baseline scenario if it does not exist
    if "policy_baseline" not in sr.scen:
        sr.scen["policy_baseline"] = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline",
            cache=False,
        )

    return sr

def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE-MACRO"
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

# TODO: merge add_NPi2030 and add_NPi2025 into one function, args to config
def add_NPi2025(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Add NPi2025 to the scenario."""

    sr = make_scenario_runner(context)
    sr.add(
        "NPi2025", 
        "baseline_DEFAULT_2025",
        # must start with this scenario name (hard-coded in the general scenario runner)
        mk_INDC=True, 
        slice_year=2020, 
        policy_year=2025, 
        target_kind="Target",
        run_reporting = False,
        solve_typ="MESSAGE-MACRO", # TODO: set to MESSAGE-MACRO when workflow test finished
    )
    
    sr.run_all()
    
    return sr.scen["NPi2025"]

def add_NPi2030(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Add NPi2030 to the scenario."""

    sr = make_scenario_runner(context)
    sr.add(
        "NPi2030", 
        "baseline_DEFAULT",
        # must start with this scenario name (hard-coded in the general scenario runner)
        mk_INDC=True, 
        slice_year=2025, 
        policy_year=2030, 
        target_kind="Target",
        run_reporting = False,
        solve_typ="MESSAGE-MACRO", # TODO: set to MESSAGE-MACRO when workflow test finished
    )
    
    # sr.add(
    #     "npi_low_dem_scen", 
    #     "NPi2030", 
    #     slice_year=2025, 
    #     tax_emission=150,
    #     run_reporting = False,
    #     solve_typ="MESSAGE-MACRO", # TODO: set to MESSAGE-MACRO when workflow test finished
    # )

    sr.run_all()
    
    return sr.scen["NPi2030"]

def add_NDC2035(context, scenario):
    """Add NDC policies to the scenario.
    """
    sr = make_scenario_runner(context)  

    sr.add(
        "INDC2035",
        "INDC2030i_weak",
        mk_INDC=True,
        slice_year=2030,
        policy_year=2035,
        target_kind="Target",
        copy_demands="baseline_low_dem_scen", # replaced with npi_low_dem_scen after it is solved
        run_reporting = False,
        solve_typ="MESSAGE-MACRO", # TODO: set to MESSAGE-MACRO when workflow test finished
    )    

    sr.run_all()

    return sr.scen["INDC2035"]

def add_NDC2030(context, scenario):
    """Add NDC policies to the scenario.
    """
    sr = make_scenario_runner(context)  

    sr.add(
        "INDC2030i_weak",
        "baseline_DEFAULT",
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        copy_demands="baseline_low_dem_scen", # replaced with npi_low_dem_scen after it is solved
        run_reporting = False,
        solve_typ="MESSAGE-MACRO", # TODO: set to MESSAGE-MACRO when workflow test finished
    )    
    
    # sr.add(
    #     "indci_low_dem_scen", 
    #     "INDC2030i_weak", 
    #     slice_year=2030, 
    #     tax_emission=250,
    #     run_reporting = False,
    #     solve_typ="MESSAGE", # TODO: set to MESSAGE-MACRO when workflow test finished
    # )

    sr.run_all()

    return sr.scen["INDC2030i_weak"]

def add_glasgow(context, scenario, level, start_scen, target_scen, slice_yr):
    """Add Glasgow policies to the scenario.
    """
    sr = make_scenario_runner(context)
   
    # Prepare add() arguments
    add_kwargs = {
        "mk_INDC": True,
        "slice_year": slice_yr,
        "run_reporting": False,
        "solve_typ": "MESSAGE-MACRO",  # TODO: args go to config too?
    }
    if level.lower() == "full":
        add_kwargs["copy_demands"] = "baseline_low_dem_scen"
    
    sr.add(
        target_scen,
        start_scen,
        **add_kwargs
    )    
    
    sr.run_all()

    # Return the target scenario that was created
    return sr.scen[target_scen]

def add_NPiREF(context, scenario):
    """Add NPi forever.
    """
    # TODO:not using _post_target for now
    # sr = make_scenario_runner(context)
    # sr.add("NPiREF", 
    #        "NPi2030", 
    #        pst=False,
    #        slice_year=2030,
    # ) 

    # sr.run_all()
    # return sr.scen["NPiREF"]

    # Get scenario name from scenario object (after clone, this is the target scenario name)
    scen = scenario.scenario
    
    # Load prc_terminal and prc_start_scen from config
    model_config = _get_ngfs_config(context)
    scen_config = model_config.get(scen, {})
    
    prc_terminal = scen_config.get('prc_terminal')
    prc_start_scen = scen_config.get('prc_start_scen')
      
    log.info(f"add_NPiREF: Using prc_terminal={prc_terminal}, prc_start_scen={prc_start_scen} for scenario '{scen}'")
    sc_ref = message_ix.Scenario(scenario.platform, scenario.model, prc_start_scen)

    df_price = interpolate_c_price(sc_ref, price_2100=prc_terminal)

    with scenario.transact("Interpolate C-price"):
        scenario.add_par("tax_emission", df_price)
        log.info(f"Added interpolated carbon prices to terminal year {prc_terminal} USD/tC")

    solve(context, scenario)
    scenario.set_as_default()

    return scenario

def add_NDC_forever(context, scenario):
    """Add NDC forever.
    """
    # TODO:not using _post_target for now

    # Get scenario name from context or scenario object
    scen = context.scenario_info.get("scenario", scenario.scenario)
    log.info(f"add_NDC_forever: Using scenario name '{scen}' for config lookup")
    
    # Load prc_terminal and prc_start_scen from config
    model_config = _get_ngfs_config(context)
    scen_config = model_config.get(scen, {})
    
    prc_terminal = scen_config.get('prc_terminal')
    prc_start_scen = scen_config.get('prc_start_scen')
    
    log.info(f"add_NDC_forever: Using prc_terminal={prc_terminal}, prc_start_scen={prc_start_scen}")
    sc_ref = message_ix.Scenario(scenario.platform, scenario.model, prc_start_scen)

    df_price = interpolate_c_price(sc_ref, price_2100=prc_terminal)

    with scenario.transact("Interpolate C-price"):
        scenario.add_par("tax_emission", df_price)
        log.info(f"Added interpolated carbon prices to terminal year {prc_terminal} USD/tC")

    solve(context, scenario)
    scenario.set_as_default()

    return scenario

def step_0(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Prepare step 0 of the EN 3 steps."""

    # A step to get a scenario ready to enter the EN 3 steps
    # For now only adding the lower bound of global CO2 emissions to limit high penetration of negative emissions
    from message_ix_models.tools import add_CO2_emission_constraint
    from message_ix_models.util import identify_nodes

    context.model.regions = identify_nodes(scenario)

    # Get scenario name 
    scen = context.scenario_info.get("scenario", scenario.scenario)
    if scen.endswith("_base"):
        scen = scen[:-5]  # Remove "_base" suffix to get base scenario name
    
    # Get global_co2_bnd from config
    global_co2_bnd = _get_ngfs_config(context).get(scen, {}).get('global_co2_bnd')
    
    # If not found in config, default to 0.0
    if global_co2_bnd is None:
        constraint_value = 0.0
        log.info(f"No global_co2_bnd found in config for scenario '{scen}', using default: 0.0")
    else:
        # If it is a string (like "500.0 / (44.0 / 12.0)"), evaluate it
        if isinstance(global_co2_bnd, str):
            try:
                constraint_value = eval(global_co2_bnd)
            except Exception as e:
                raise ValueError(
                    f"Could not evaluate global_co2_bnd expression '{global_co2_bnd}' "
                    f"for scenario '{scen}': {e}"
                )
        else:
            # If it is already a number, use it directly
            constraint_value = float(global_co2_bnd)
        
        log.info(
            f"Using global_co2_bnd={constraint_value:.2f} for scenario '{scen}' "
            f"(from config: {global_co2_bnd})"
        )


    add_CO2_emission_constraint.main(
        scenario, 
        reg=f"{context.model.regions}_GLB",
        relation_name="CO2_Emission_Global_Total", 
        constraint_value=constraint_value, 
        type_rel="lower"
    )

    scenario.set_as_default()
    return scenario

def step_1_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Apply budget constraint (step_1 of the EN 3 steps) and solve the scenario.
    
    This function gets the scenario name from context, loads the budget from config,
    validates it, applies the budget constraint using step_1, and then solves the scenario.
    """
    # Get scenario name from context and extract base name (remove _EN1 suffix if present)
    scen = context.scenario_info.get("scenario", scenario.scenario)
    if scen.endswith("_EN1"):
        scen = scen[:-4]  # Remove "_EN1" suffix to get base scenario name
    
    # Get budget from config
    budget_value = _get_ngfs_config(context).get(scen, {}).get('budget')
       
    # Use "non-calc" mode:
    # - `label` is ignored
    # - `budget` is passed directly to add_budget() as the bound value
    policy_config = PolicyConfig(label=str(budget_value), budget=float(budget_value))
    
    step_1(context, scenario, policy_config)
    solve(context, scenario)
    
    return scenario

def step_2_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Apply emission trajectory (step_2 of the EN 3 steps) and solve the scenario.
    
    This function applies the emission trajectory constraint using step_2, which
    retrieves the CO2 emission trajectory from the solved scenario and applies it
    as bound_emission. Then it solves the scenario.
    
    Note: step_2 requires the scenario to have a solution (from step_1) to retrieve
    the emission trajectory from.
    """

    if not hasattr(context, 'run_reporting_only'):
        context.run_reporting_only = False # step_2 uses its own scenario_runner under engage, but does not matter here
    
    # TODO: discuss with Paul, step_2 does not actually use any PolicyConfig attributes?
    # Create an empty PolicyConfig to satisfy the function signature
    policy_config = PolicyConfig()
    
    step_2(context, scenario, policy_config)
    message_ix.models.DEFAULT_CPLEX_OPTIONS = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }
    scenario.solve(model="MESSAGE")
    
    return scenario

def step_3_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Apply tax_emission prices (step_3 of the EN 3 steps) and solve the scenario.
    
    This function loads step_3_type_emission from config, applies tax_emission prices
    using to the specified type_emission (TCE_non-CO2 by default), and then solves the scenario.
    """
    # Get scenario name from context and extract base name (remove _EN2 suffix if present)
    scen = context.scenario_info.get("scenario", scenario.scenario)
    if scen.endswith("_EN2"):
        scen = scen[:-4]  # Remove "_EN2" suffix to get base scenario name
    elif scen.endswith("_EN3"):
        scen = scen[:-4]  # Remove "_EN3" suffix to get base scenario name
    
    # Get step_3_type_emission from config
    step_3_type_emission = _get_ngfs_config(context).get(scen, {}).get('step_3_type_emission')
    if step_3_type_emission is None:
        step_3_type_emission = ["TCE_non-CO2"]
    elif isinstance(step_3_type_emission, str):
        step_3_type_emission = [step_3_type_emission]
    elif isinstance(step_3_type_emission, list):
        step_3_type_emission = step_3_type_emission
   
    policy_config = PolicyConfig(step_3_type_emission=step_3_type_emission)
    
    step_3(context, scenario, policy_config)
    solve(context, scenario)
    
    return scenario

def calibrate_macro(context, scenario):
    """Runs macro calibration process on a scenario."""

    log.info("Calibrating macro to scenario.")
    # update cost_ref and price_ref with new solution
    model_config = _get_ngfs_config(context)
    update_macro_calib_file(
        scenario, eval(model_config["macro_data_file"])
    )

    # After solving, add macro calibration
    scenario = add_macro_materials(
        scenario, eval(model_config["macro_data_file"])
    )

    scenario.solve(model="MESSAGE-MACRO")
    scenario.set_as_default()

    return scenario

# NGFS P6 scenarios:
_scen_all = [
    "h_ndc", 
    "h_ndc_2035",
    "d_delfrag", 
    "d_delfrag_2035",
    "h_cpol", 
    "o_1p5c", 
    "o_2c", 
    "d_strain",
    "d_strain_2025",
    ]

_scen_en_steps = [
    "o_1p5c", 
    "o_2c", 
    "d_delfrag",
    "d_delfrag_2035",
    ]

def generate(context: Context) -> Workflow:
    """Create the NGFS workflow."""
    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Define model name
    model_name = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 2.2-NGFS-R12"

    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/SSP_SSP2_v6.5/baseline",
        # fmy of the whole workflow afterwards starts from 2030
        # TODO: replace with bmt baseline later
        # target = f"{model_name}/baseline",
    )

    wf.add_step(
        "base_2025",
        None,
        target="ixmp://ixmp-dev/SSP_SSP2_v6.4/baseline_DEFAULT_step_14",
    )

    wf.add_step(
        "base cloned",
        "base",
        target=f"{model_name}/baseline_DEFAULT",
        clone=dict(keep_solution=True),
    )

    wf.add_step(
        "base_2025 cloned",
        "base_2025",
        target=f"{model_name}/baseline_DEFAULT_2025",
        clone=dict(keep_solution=True, shift_first_model_year=2025),
    )

    wf.add_step(
        "base_2025 reported",
        "base_2025 cloned",
        report,
    )

    wf.add_step(
        "base reported",
        "base cloned",
        placeholder,
    )

    wf.add_step(
        "NPi2030 solved",
        "base reported",
        add_NPi2030,
        target=f"{model_name}/NPi2030",
    )

    wf.add_step(
        "h_cpol solved",
        "NPi2030 solved",
        add_NPiREF,
        target=f"{model_name}/h_cpol",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "d_strain solved",
        "h_cpol solved",
        add_glasgow,
        target=f"{model_name}/d_strain_2030_glasgow_partial",
        target_scen = "d_strain_2030_glasgow_partial",
        slice_yr = 2025,
        start_scen = "h_cpol",
        level = "Partial",
    )

    wf.add_step( # Not working for now
        "base_2025 macro solved",
        "base_2025 reported",
        calibrate_macro,
        target=f"{model_name}/base_2025_macro",
        clone=dict(keep_solution=True),
    )

    wf.add_step( # Not working for now
        "NPi2025 solved",
        "base_2025 macro solved",
        add_NPi2025,
        target=f"{model_name}/NPi2025",
    )

    wf.add_step(
        "h_cpol_2025 solved",
        "NPi2025 solved", 
        add_NPiREF,
        target=f"{model_name}/h_cpol_2025",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "d_strain_2025 solved",
        "h_cpol_2025 solved",
        add_glasgow,
        target=f"{model_name}/d_strain_2025_glasgow_partial",
        target_scen = "d_strain_2025_glasgow_partial",
        slice_yr = 2025,
        start_scen = "h_cpol_2025",
        level = "Partial",
    )

    wf.add_step(
        "NDC2030 solved",
        "base reported",
        add_NDC2030,
        target=f"{model_name}/INDC2030i_weak",
    )

    wf.add_step(
        "NDC2030 reported",
        "NDC2030 solved",
        report,
    )

    wf.add_step(
        "h_ndc solved",
        "NDC2030 solved",
        add_NDC_forever,
        target=f"{model_name}/h_ndc",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "NDC2035 solved",
        "NDC2030 solved",
        add_NDC2035,
        target=f"{model_name}/INDC2035",
        clone=dict(keep_solution=False, shift_first_model_year=2035),
    )

    wf.add_step(
        "NDC2035 reported",
        "NDC2035 solved",
        report,
    )

    wf.add_step(
        "h_ndc_2035 solved",
        "NDC2035 reported",
        add_NDC_forever,
        target=f"{model_name}/h_ndc_2035",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "glasgow_partial_2030 solved",
        "base reported",
        add_glasgow,
        target=f"{model_name}/glasgow_partial_2030",
        target_scen = "glasgow_partial_2030",
        slice_yr = 2025,
        start_scen = "baseline_DEFAULT",
        level = "Partial",
    )

    wf.add_step(
        "glasgow_partial_2030 reported",
        "glasgow_partial_2030 solved",
        report,
    )

    wf.add_step(
        "o_2c base built",
        "glasgow_partial_2030 solved",
        step_0,
        target=f"{model_name}/o_2c_base",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "glasgow_full_2030 solved",
        "base reported",
        add_glasgow,
        target=f"{model_name}/glasgow_full_2030",
        start_scen = "baseline_DEFAULT",
        target_scen = "glasgow_full_2030",
        slice_yr = 2025,
        level = "Full",
    )

    wf.add_step(
        "glasgow_full_2030 reported",
        "glasgow_full_2030 solved",
        report,
    )

    wf.add_step(
        "o_1p5c base built",
        "glasgow_full_2030 solved",
        step_0,
        target=f"{model_name}/o_1p5c_base",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "d_delfrag_2030_2035 solved",
        "h_cpol solved",
        add_glasgow,
        target=f"{model_name}/d_delfrag_2030_glasgow_partial",
        target_scen = "d_delfrag_2030_glasgow_partial",
        slice_yr = 2030,
        start_scen = "h_cpol",
        level = "Partial",
        clone=dict(keep_solution=True, shift_first_model_year=2035),
    )

    wf.add_step(
        "d_delfrag_2030_2035 reported",
        "d_delfrag_2030_2035 solved",
        report,
    )

    wf.add_step(
        "d_delfrag base built",
        "d_delfrag_2030_2035 reported",
        step_0,
        target=f"{model_name}/d_delfrag_base",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "d_delfrag_2035_2040 solved",
        "h_cpol solved",
        add_glasgow,
        target=f"{model_name}/d_delfrag_2035_glasgow_partial",
        target_scen = "d_delfrag_2035_glasgow_partial",
        slice_yr = 2035,
        start_scen = "h_cpol",
        level = "Partial",
        clone=dict(keep_solution=True, shift_first_model_year=2040),
    )

    wf.add_step(
        "d_delfrag_2035_2040 reported",
        "d_delfrag_2035_2040 solved",
        report,
    )

    wf.add_step(
        "d_delfrag_2035 base built",
        "d_delfrag_2035_2040 reported",
        step_0,
        target=f"{model_name}/d_delfrag_2035_base",
        clone=dict(keep_solution=False),
    )

    for scen in _scen_en_steps:
        wf.add_step(
            f"{scen} EN1",
            f"{scen} base built",
            step_1_and_solve,
            target=f"{model_name}/{scen}_EN1",
            clone=dict(keep_solution=False),
        )

        wf.add_step(
            f"{scen} EN2",
            f"{scen} EN1",
            step_2_and_solve,
            target=f"{model_name}/{scen}_EN2",
            clone=dict(keep_solution=True), # must have solution (to retrieve emission trajectories from)
        )

        wf.add_step(
            f"{scen} solved",
            f"{scen} EN2",
            step_3_and_solve,
            target=f"{model_name}/{scen}",
            clone=dict(keep_solution=True), # must have solution (to retrieve prices from)
        )

    for scen in _scen_all:
        wf.add_step(
            f"{scen} reported",
            f"{scen} solved",
            report,
        )

    reported_keys = [Key(f"{scen} reported") for scen in _scen_all]
    wf.add("prep all", *reported_keys, placeholder, "context")

    return wf
