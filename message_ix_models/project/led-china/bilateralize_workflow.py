# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for LED Chinese Energy Security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

# Import scenario and models
config, config_path = load_config(project_name = 'led-china', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']

# Generate edit files
prepare_edit_files(project_name = 'led-china', 
                   config_name = 'config.yaml',
                   P_access = True)
                   
# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'led-china', 
                              config_name = 'config.yaml')

# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(base_model)
    print(base_scen)

    load_and_solve(trade_dict = trade_dict,
                   solve = True,
                   project_name = 'led-china', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'china_security',
                   target_scen = base_scen)