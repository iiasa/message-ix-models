# The workflow mainly contains the steps to build ngfs scenarios,
# as well as the steps to apply policy scenario settings. See ngfs-workflow.svg.
# Example cli command:
# mix-models ngfs run --from="base" "glasgow+" --dry-run

import logging

import message_ix

from message_ix_models import Context
from message_ix_models.workflow import Workflow


log = logging.getLogger(__name__)

# Functions for individual workflow steps


def placeholder(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario

def make_scenario_runner(context):

    from message_data.model.scenario_runner import ScenarioRunner
    from message_ix_models.util import private_data_path
    import yaml

    try:
        biomass_trade = context.biomass_trade
    except AttributeError:
        biomass_trade = False

    context.ssp = 'SSP2'
    context.run_reporting_only = False
    context.policy_data_file = 'ngfs_p6_policy_data.xlsx'
    context.policy_config = 'ngfs_p6_config.yaml'
    context.region_id = "R12"

    # Load slack data from config.yaml
    config_file = private_data_path('projects', 'ngfs', 'config.yaml')
    with open(config_file) as f:
        model_config = yaml.safe_load(f)['MESSAGEix-GLOBIOM 2.2-NGFS-R12']

    sr = ScenarioRunner(
        context,
        slack_data=model_config['policy_slacks'][model_config['slack_scn']][context.ssp],
        biomass_trade=biomass_trade,
    )

    # Pre-populate policy_baseline 
    if "policy_baseline" not in sr.scen:
        sr.scen["policy_baseline"] = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline",
            cache=False,
        )

    return sr


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
    )
    
    sr.run_all()
    
    return sr.scen["NPi2030"]

def add_NDC2030(context, scenario):
    """Add NDC policies to the scenario.
    """
    sr = make_scenario_runner(context)  

    sr.add(
        "INDC2030i",
        "baseline_DEFAULT",
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
    )    
    
    sr.run_all()

    return sr.scen["INDC2030i"]


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




# NGFS P6 scenarios:
_scen_all = [
    "ndc", 
    "frag", 
    "cpol", 
    "1p5c", 
    "2c", 
    "delayed"
    ]

_scen_en_steps = [
    "1p5c", 
    "2c", 
    "delayed",
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
        "base cloned",
        "base",
        placeholder,
        target=f"{model_name}/baseline_DEFAULT",
        clone=dict(keep_solution=True),
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
        "cpol solved",
        "NPi2030 solved",
        placeholder,
        target=f"{model_name}/cpol",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "NDC2030 solved",
        "base reported",
        add_NDC2030,
        target=f"{model_name}/INDC2030i",
    )

    wf.add_step(
        "ndc solved",
        "NDC2030 solved",
        placeholder,
        target=f"{model_name}/ndc",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "glasgow_partial_2030 solved",
        "base reported",
        placeholder,
        target=f"{model_name}/glasgow_partial_2030",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "frag solved",
        "glasgow_partial_2030 solved",
        placeholder,
        target=f"{model_name}/frag",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "2c built",
        "glasgow_partial_2030 solved",
        placeholder,
        target=f"{model_name}/2c_built",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "delayed built",
        "glasgow_partial_2030 solved",
        placeholder,
        target=f"{model_name}/delayed_built",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "glasgow_full_2030 solved",
        "base reported",
        placeholder,
        target=f"{model_name}/glasgow_full_2030",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "1p5c built",
        "glasgow_full_2030 solved",
        placeholder,
        target=f"{model_name}/1p5cbuilt",
        clone=dict(keep_solution=False),
    )

    for scen in _scen_en_steps:
        wf.add_step(
            f"{scen} EN1",
            f"{scen} built",
            placeholder,
            target=f"{model_name}/{scen}_EN1",
            clone=dict(keep_solution=False),
        )

        wf.add_step(
            f"{scen} EN2",
            f"{scen} EN1",
            placeholder,
            target=f"{model_name}/{scen}_EN2",
            clone=dict(keep_solution=False),
        )

        wf.add_step(
            f"{scen} EN3",
            f"{scen} EN2",
            placeholder,
            target=f"{model_name}/{scen}_EN3",
            clone=dict(keep_solution=False),
        )

        wf.add_step(
            f"{scen} solved",
            f"{scen} EN3",
            placeholder,
            target=f"{model_name}/{scen}",
            clone=dict(keep_solution=False),
        )

    for scen in _scen_all:
        wf.add_step(
            f"{scen} reported",
            f"{scen} solved",
            placeholder,
        )

    return wf
