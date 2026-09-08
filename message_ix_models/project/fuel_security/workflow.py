"""
Workflow for developing baseline scenarios and bilateralizing them for fuel security project
"""
import logging
import os
from ixmp import Platform

# Import tools
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.fuel_security.liquefaction_calibration import *
from message_ix_models.project.fuel_security.adjust_reexports import *
from message_ix_models.project.fuel_security.bilateralize_scenario import *

from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_ix_models.project.fuel_security.policy import (
    add_NPi2030,
    add_NDC2030,
)

log = logging.getLogger(__name__)


def _set_default(context, scenario):
    """Mark `scenario` as the default version for its (model, scenario) name.

    ixmp's Scenario.clone() does not do this automatically for a clone into a new
    (model, scenario) name pair (see ixmp.core.scenario.Scenario.clone docstring).
    Without it, downstream code that loads "baseline_DEFAULT" by name only (e.g.
    message_data.model.scenario_runner.make_scenario_runner) silently resolves to
    a stale default version instead of the scenario produced by this workflow run.
    """
    scenario.set_as_default()
    return scenario

def _bilateralize(context, scenario):
    """Bilateralize trade technologies on the scenario produced upstream."""
    return bilateralize_scenario(
        project_name="fuel_security",
        config_name="config.yaml",
        scenario=scenario,
        target_scenario=f"{scenario.scenario}_bilateral"
    )

def _FSU_restriction(context, scenario, friction_endyear):
    """Run FSU restriction scenario."""
    return run_friction_scenario(
        base_scenario=scenario,
        friction_endyear=friction_endyear
    )

# Generate workflow
def generate(context: Context) -> Workflow:
    """
    Generate workflow for fuel security project
    """
    wf = Workflow(context)

    # Context attributes
    context.ssp = "SSP2"
    context.model.regions = "R12"
    
    context.run_reporting_only = False
    context.policy_data_file = "fuel_security_policy_data.xlsx"
    context.policy_config_path = ("projects", "fuel_security", "config.yaml")
    context.region_id = "R12"
    
    # Workflow steps
    wf.add_step(
        "Base",
        None,
        target = "ixmp://ixmp-dev/SSP_SSP2_v6.6/baseline" 
    ) # Load baseline scenario from SSP_SSP2_v6.6

    wf.add_step(
        "INDC2030i_forever",
        None,
        target = "ixmp://ixmp-dev/SSP_SSP2_v6.6/INDC2030i_forever",
    ) # Load INDC2030i_forever scenario from SSP_SSP2_v6.6

    wf.add_step(
        "Base cloned",
        "Base",
        _set_default,
        target = "fuel_security/baseline_DEFAULT", # This has to be named baseline_DEFAULT to match policy tool requirement
        clone = dict(keep_solution = True)
    ) # Clone baseline scenario to fuel_security/baseline_DEFAULT

    wf.add_step(
        "Clone INDC2030i_forever",
        "INDC2030i_forever",
        _set_default,
        target = "fuel_security/INDC2030i_forever",
        clone = dict(keep_solution = True)
    ) # Clone INDC2030i_forever to fuel_security/INDC2030i_forever

    wf.add_step(
        "Add and solve NPi2030",
        "Base cloned",
        add_NPi2030,
        target = "fuel_security/NPi2030"
    ) # Add and solve NPi2030 onto baseline_DEFAULT to create fuel_security/NPi2030

    wf.add_step(
        "Baseline bilateralized",
        "Base cloned",
        _bilateralize,
        target="fuel_security/baseline_bilateral"
    ) # Bilateralize fuel_security/baseline_DEFAULT to create fuel_security/baseline_bilateral
    
    wf.add_step(
        "NPi2030 bilateralized",
        "Add and solve NPi2030",
        _bilateralize,
        target="fuel_security/NPi2030_bilateral"
    ) # Bilateralize NPi2030 to create fuel_security/NPi2030_bilateral

    wf.add_step(
        "INDC2030i_forever bilateralized",
        "Clone INDC2030i_forever",
        _bilateralize,
        target="fuel_security/INDC2030i_forever_bilateral"
    ) # Bilateralize INDC2030i_forever to create fuel_security/INDC2030i_forever_bilateral
        
    wf.add_step(
        "FSU2100",
        "Baseline bilateralized",
        _FSU_restriction,
        friction_endyear=2100,
        target="fuel_security/baseline_FSU2100"
    ) # Run FSU2100 scenario on baseline bilateralized to create fuel_security/baseline_FSU2100
\
    return wf
