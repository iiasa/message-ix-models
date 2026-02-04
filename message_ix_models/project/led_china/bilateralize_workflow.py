# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for LED Chinese Energy Security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

import os

# Import scenario and models
config, config_path = load_config(project_name = 'led_china', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Clear bare files
print("Clearing bare files")
for tec in config['covered_trade_technologies']:
    for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
        if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
            os.remove(os.path.join(data_path, tec, "bare_files", file))
    for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
        if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
            os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))
    
# Generate edit files
prepare_edit_files(project_name = 'led_china', 
                   config_name = 'config.yaml',
                   P_access = True)

# Add constraints to the dictionary
constraint_pars = ["initial_activity_lo", "initial_activity_up",
                   "growth_activity_lo", "growth_activity_up",
                   "soft_activity_lo", "soft_activity_up"]
#for tec in config['constraint_values'].keys():
#    for par in constraint_pars:
#        df = pd.read_csv(os.path.join(data_path, tec, "edit_files", par + ".csv"))
#        if par in ["initial_activity_lo", "initial_activity_up"]:
#            df["value"] = 2
#        if par in ["growth_activity_lo", "growth_activity_up"]:
#            constraint_value = config['constraint_values'][tec][par]
#            if isinstance(constraint_value, dict):
#                for region in constraint_value.keys():
#                    df.loc[df["node_loc"] == region, "value"] = constraint_value[region]
#            else:
#                df["value"] = constraint_value
#        df.to_csv(os.path.join(data_path, tec, "bare_files", par + ".csv"), index=False)

# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'led_china', 
                              config_name = 'config.yaml',
                              p_drive_access = True)
# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(f"Base model: {base_model}/{base_scen}")
    print(f"Target model: china_security/{model_scen}")

    load_and_solve(trade_dict = trade_dict,
                   solve = True,
                   project_name = 'led_china', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'china_security',
                   target_scen = model_scen,
                   gams_args = ['--foresight=1'])
