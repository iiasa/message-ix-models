# The workflow mainly contains the steps to build elevate gp scenarios,
# as well as the steps to apply policy scenario settings. See elevate_gp_workflow.svg.
# Example cli command:
# mix-models elevate run --from="base" "prep all" --dry-run

import logging

import message_ix  # type: ignore

from message_ix import make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.tools.policy import (
    add_NPi2030, 
    add_NDC2030, 
    add_glasgow, 
    add_forever_constant,
)
from message_ix_models.project.engage.workflow import (
    PolicyConfig,
    step_1,
    step_2,
    step_3,
    step_4,
)
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)

# Single source of truth for the ELEVATE GP model name (config key and scenario target prefix)
# ELE_MODEL_NAME = "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12 GP"
ELE_MODEL_NAME = "to_be_deleted"

# Functions for individual workflow steps

def report(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Report the scenario."""
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore

    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    run_config = "materials_daccs_run_config.yaml"

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


def placeholder(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    solve_options = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    scenario.solve(model, solve_options=solve_options)
    scenario.set_as_default()

    return scenario

# Constant carbon price (USD/tCO2) per region from 2030 to 2110 for CP-D0
# Picked from lookup run
CP_C0_PRICE = {
    "R12_AFR": 7.33,
    "R12_CHN": 7.33,
    "R12_EEU": 91.667,
    "R12_FSU": 7.33,
    "R12_LAM": 18.33,
    "R12_MEA": 7.33,
    "R12_NAM": 18.33,
    "R12_PAO": 7.33,
    "R12_PAS": 18.33,
    "R12_RCPA": 7.33,
    "R12_SAS": 7.33,
    "R12_WEU": 14.667,
}

# def step_0(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
#     """Prepare step 0 of the EN 3 steps."""

#     # A step to get a scenario ready to enter the EN 3 steps
#     # For now only add the lower bound of global CO2 emissions
#     # to limit high penetration of negative emissions.
#     from message_ix_models.tools import add_CO2_emission_constraint
#     from message_ix_models.util import identify_nodes

#     context.model.regions = identify_nodes(scenario)

#     # Get scenario name
#     scen = context.scenario_info.get("scenario", scenario.scenario)
#     if scen.endswith("_base"):
#         scen = scen[:-5]  # Remove "_base" suffix to get base scenario name

#     # Get global_co2_bnd from config
#     global_co2_bnd = _get_ngfs_config(context).get(scen, {}).get("global_co2_bnd")

#     # If not found in config, default to 0.0
#     if global_co2_bnd is None:
#         constraint_value = 0.0
#         log.info(
#             "No global_co2_bnd found in config for scenario '%s', using default: 0.0",
#             scen,
#         )
#     else:
#         # If it is a string (like "500.0 / (44.0 / 12.0)"), evaluate it
#         if isinstance(global_co2_bnd, str):
#             try:
#                 constraint_value = eval(global_co2_bnd)
#             except Exception as e:
#                 raise ValueError(
#                     f"Could not evaluate global_co2_bnd expression '{global_co2_bnd}' "
#                     f"for scenario '{scen}': {e}"
#                 )
#         else:
#             # If it is already a number, use it directly
#             constraint_value = float(global_co2_bnd)

#         log.info(
#             f"Using global_co2_bnd={constraint_value:.2f} for scenario '{scen}' "
#             f"(from config: {global_co2_bnd})"
#         )

#     add_CO2_emission_constraint.main(
#         scenario,
#         reg=f"{context.model.regions}_GLB",
#         relation_name="CO2_Emission_Global_Total",
#         constraint_value=constraint_value,
#         type_rel="lower",
#     )

#     scenario.set_as_default()
#     return scenario


# def step_1_and_solve(
#     context: Context, scenario: message_ix.Scenario
# ) -> message_ix.Scenario:
#     """Apply budget constraint (step_1 of the EN 3 steps) and solve the scenario.

#     This function gets the scenario name from context, loads the budget from config,
#     validates it, applies the budget constraint using step_1,
#     and then solves the scenario.
#     """
#     # Get scenario name from context and extract base name
#     # (remove _EN1 suffix if present).
#     scen = context.scenario_info.get("scenario", scenario.scenario)
#     if scen.endswith("_EN1"):
#         scen = scen[:-4]  # Remove "_EN1" suffix to get base scenario name

#     # Get budget from config
#     budget_value = _get_ngfs_config(context).get(scen, {}).get("budget")

#     # Use "non-calc" mode:
#     # - `label` is ignored
#     # - `budget` is passed directly to add_budget() as the bound value
#     policy_config = PolicyConfig(label=str(budget_value), budget=float(budget_value))

#     step_1(context, scenario, policy_config)
#     solve(context, scenario)

#     return scenario


# def step_2_and_solve(
#     context: Context, scenario: message_ix.Scenario
# ) -> message_ix.Scenario:
#     """Apply emission trajectory (step_2 of the EN 3 steps) and solve the scenario.

#     This function applies the emission trajectory constraint using step_2, which
#     retrieves the CO2 emission trajectory from the solved scenario and applies it
#     as bound_emission. Then it solves the scenario.

#     Note: step_2 requires the scenario to have a solution (from step_1) to retrieve
#     the emission trajectory from.
#     """

#     if not hasattr(context, "run_reporting_only"):
#         # step_2 uses its own scenario_runner under engage,
#         # but this flag does not matter here.
#         context.run_reporting_only = False

#     # TODO: discuss with Paul, step_2 does not actually use any PolicyConfig attributes?
#     # Create an empty PolicyConfig to satisfy the function signature
#     policy_config = PolicyConfig()

#     step_2(context, scenario, policy_config)
#     message_ix.models.DEFAULT_CPLEX_OPTIONS = {
#         "advind": 0,
#         "lpmethod": 4,
#         "threads": 4,
#         "epopt": 1e-6,
#         "scaind": -1,
#         # "predual": 1,
#         "barcrossalg": 0,
#     }
#     scenario.solve(model="MESSAGE")

#     return scenario


# def step_3_and_solve(
#     context: Context, scenario: message_ix.Scenario
# ) -> message_ix.Scenario:
#     """Apply tax_emission prices (step_3 of the EN 3 steps) and solve the scenario.

#     This function loads step_3_type_emission from config, applies tax_emission prices
#     using the specified type_emission (TCE_non-CO2 by default),
#     and then solves the scenario.
#     """
#     # Get scenario name from context and extract base name
#     # (remove _EN2 suffix if present).
#     scen = context.scenario_info.get("scenario", scenario.scenario)
#     if scen.endswith("_EN2"):
#         scen = scen[:-4]  # Remove "_EN2" suffix to get base scenario name
#     elif scen.endswith("_EN3"):
#         scen = scen[:-4]  # Remove "_EN3" suffix to get base scenario name

#     # Get step_3_type_emission from config
#     step_3_type_emission = (
#         _get_ngfs_config(context).get(scen, {}).get("step_3_type_emission")
#     )
#     if step_3_type_emission is None:
#         step_3_type_emission = ["TCE_non-CO2"]
#     elif isinstance(step_3_type_emission, str):
#         step_3_type_emission = [step_3_type_emission]
#     elif isinstance(step_3_type_emission, list):
#         step_3_type_emission = step_3_type_emission

#     policy_config = PolicyConfig(step_3_type_emission=step_3_type_emission)

#     step_3(context, scenario, policy_config)
#     solve(context, scenario)

#     return scenario


# def step_4_and_solve(
#     context: Context, scenario: message_ix.Scenario
# ) -> message_ix.Scenario:
#     """Lock in regional TCE emission path to deliver regional carbon prices."""

#     step_4(context, scenario)
#     solve(context, scenario)

#     return scenario


# ELEVATE GP scenarios:
_scen_all = [
    # "ELV-SSP2-650P-400F",
    # "ELV-SSP2-1150F",
    "ELV-SSP2-CP-D0",
    "ELV-SSP2-NDC-D0",
    "ELV-SSP2-LTS",
    "ELV-SSP2-NDC-LTS",
    # "ELV-SSP2-GP",
    # "ELV-SSP2-GPL",
    # "ELV-SSP2-GPB",
]

_scen_en_steps: list[str] = [
    # "ELV-SSP2-650P-400F",
    # "ELV-SSP2-1150F",
    # "ELV-SSP2-GPB",
]


def generate(context: Context) -> Workflow:
    """Create the ELEVATE GP workflow."""
    wf = Workflow(context)
    # Prepare general context attributes
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # Prepare context to be passed to ScenarioRunner class
    context.run_reporting_only = False
    context.policy_data_file = "elevate_gp_policy_data.xlsx"
    context.policy_config_path = ("projects", "elevate_gp", "config.yaml")
    context.region_id = "R12"

    # Full scenario target prefix (platform + model name)
    model_name = f"ixmp://ixmp-dev/{ELE_MODEL_NAME}"

    wf.add_step(
        "base",
        None,
        target="ixmp://ixmp-dev/SSP_SSP2_v6.6/baseline", 
    )

    wf.add_step(
        "base cloned",
        "base",
        target=f"{model_name}/baseline_DEFAULT",
        clone=dict(keep_solution=True),
    )

    wf.add_step(
        "base reported",
        "base cloned",
        report,
    )

    wf.add_step(
        "NPi2030 solved",
        "base reported", 
        add_NPi2030,
        target=f"{model_name}/NPi2030",
    )

    wf.add_step(
        "CP-D0 solved",
        "NPi2030 solved",
        add_forever_constant,
        specified_price=CP_C0_PRICE,
        solve_type="MESSAGE-MACRO",
        target=f"{model_name}/ELV-SSP2-CP-D0",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "ELV-SSP2-CP-D0 reported",
        "CP-D0 solved",
        report,
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
        "NDC-D0 solved",
        "NDC2030 solved",
        add_forever_constant,
        target=f"{model_name}/ELV-SSP2-NDC-D0",
        solve_type="MESSAGE-MACRO",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "ELV-SSP2-NDC-D0 reported",
        "NDC-D0 solved",
        report,
    )

    wf.add_step(
        "glasgow_full_2030 solved",
        "base reported",
        add_glasgow,
        target=f"{model_name}/glasgow_full_2030",
        start_scen="baseline_DEFAULT",
        target_scen="glasgow_full_2030",
        slice_yr=2025,
        level="Full",
    )

    wf.add_step(
        "ELV-SSP2-LTS reported",
        "glasgow_full_2030 solved",
        report,
    )

    wf.add_step(
        "NDC-LTS solved",
        "NDC2030 reported",
        add_glasgow,
        target=f"{model_name}/NDC-LTS_glasgow_full",
        target_scen="NDC-LTS_glasgow_full",
        slice_yr=2030,
        start_scen="INDC2030i_weak",
        level="Full",
        clone=dict(keep_solution=False, shift_first_model_year=2035),
    )

    wf.add_step(
        "ELV-SSP2-NDC-LTS reported",
        "NDC-LTS solved",
        report,
    )

    # for scen in _scen_en_steps:
    #     wf.add_step(
    #         f"{scen} EN1",
    #         f"{scen} base built",
    #         step_1_and_solve,
    #         target=f"{model_name}/{scen}_EN1",
    #         clone=dict(keep_solution=False),
    #     )

    #     wf.add_step(
    #         f"{scen} EN2",
    #         f"{scen} EN1",
    #         step_2_and_solve,
    #         target=f"{model_name}/{scen}_EN2",
    #         # Must have solution to retrieve emission trajectories.
    #         clone=dict(keep_solution=True),
    #     )

    #     wf.add_step(
    #         f"{scen} EN2 reported",
    #         f"{scen} EN2",
    #         report,
    #     )

    #     wf.add_step(
    #         f"{scen} EN3",
    #         f"{scen} EN2",
    #         step_3_and_solve,
    #         target=f"{model_name}/{scen}_EN3",
    #         # Must have solution to retrieve prices.
    #         clone=dict(keep_solution=True),
    #     )

    #     wf.add_step(
    #         f"{scen} EN3 reported",
    #         f"{scen} EN3",
    #         report,
    #     )

    #     wf.add_step(
    #         f"{scen} solved",
    #         f"{scen} EN3",
    #         step_4_and_solve,
    #         target=f"{model_name}/{scen}",
    #         # Must have solution to retrieve prices.
    #         clone=dict(keep_solution=True),
    #     )

    # for scen in _scen_all:
    #     wf.add_step(
    #         f"{scen} reported",
    #         f"{scen} solved",
    #         report,
    #     )

    reported_keys = [f"{scen} reported" for scen in _scen_all]
    wf.add("prep all", reported_keys)

    return wf