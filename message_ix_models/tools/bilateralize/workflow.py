# -*- coding: utf-8 -*-
"""
Workflow for bilateralizing trade flows in MESSAGEix

This includes:
1. Prepare edit files for bilateralization
2. Move data from bare files to a dictionary to update a MESSAGEix scenario
3. Update MESSAGEix scenario(s) with bilateralized dictionary and
   solve scenario using the ixmp database or save as a GDX data
   file for direct solve in GAMS.
"""

# Import packages
from message_ix_models.tools.bilateralize.bare_to_scenario import bare_to_scenario
from message_ix_models.tools.bilateralize.load_and_solve import load_and_solve
from message_ix_models.tools.bilateralize.prepare_edit import prepare_edit_files

# Project setup
project_name = None  # Name of the project (e.g., 'newpathways')
config_name = None  # Name of the config file (e.g., 'config.yaml')

# Prepare edit files for bilateralization
prepare_edit_files(project_name=project_name, config_name=config_name)

# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name=project_name, config_name=config_name)

# Update MESSAGEix scenario(s) with bilateralized dictionary and solve scenario
load_and_solve(
    project_name=project_name,
    config_name=config_name,
    scenario=None,  # Specifies MESSAGEix scenario, or will use project yaml
    trade_dict=trade_dict,
    solve=True,
)
