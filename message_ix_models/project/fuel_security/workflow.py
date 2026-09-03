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

from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_ix_models.project.fuel_security.policy import (
    add_NPi2030,
)

log = logging.getLogger(__name__)

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

    # Set up target scenario
    model_name = "ixmp://ixmp-dev/fuel_security"

    # Workflow steps
    wf.add_step(
        "Base",
        None,
        target = "ixmp://ixmp-dev/SSP_SSP2_v5.1/baseline" # TODO update this to 6.6   
    )

    wf.add_step(
        "Base cloned",
        "Base",
        target = "fuel_security/baseline_DEFAULT", # This has to be named baseline_DEFAULT to match policy tool requirement
        clone = dict(keep_solution = False)
    )

    wf.add_step(
        "Add and solve NPi2030",
        "Base cloned",
        add_NPi2030,
        target = "fuel_security/NPi2030"
    )
    
    return wf
