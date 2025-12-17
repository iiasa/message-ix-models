# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for ALPS HHI Analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

import os

# Import scenario and models
config, config_path = load_config(project_name = 'alps_hhi', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Clear bare files
print("Clearing bare files")
for tec in config['covered_trade_technologies']:
    if os.path.exists(os.path.join(data_path, tec, "bare_files")):
        for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
            if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
                os.remove(os.path.join(data_path, tec, "bare_files", file))
    if os.path.exists(os.path.join(data_path, tec, "bare_files", "flow_technology")):
        for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
            if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
                os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))
        
# Generate edit files
prepare_edit_files(project_name = 'alps_hhi', 
                   config_name = 'config.yaml',
                   P_access = True)

# Add constraints to the dictionary
print("Add constraints to the dictionary")
constraint_pars = ["initial_activity_lo", "initial_activity_up",
                   "growth_activity_lo", "growth_activity_up"]
                   #"soft_activity_lo", "soft_activity_up"]
constraint_tec = config['constrained_tec']

for con in constraint_pars:
    print(f"...{con}")
    for tec in constraint_tec:
        print(f"......{tec}")
        df = pd.read_csv(os.path.join(package_data_path("alps_hhi", "scenario_updates", tec), con + ".csv"))
        df.to_csv(os.path.join(data_path, tec, "bare_files", con + ".csv"), index = False)
        
# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'alps_hhi', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

# Remove pipeline inputs to trade technologies
print(f"Detaching pipeline capacity and adding investment costs directly to trade")
for t in config['covered_trade_technologies']:
    if "piped" in t:
        print(f"...{t}")
        #pipeline_input = trade_dict[t]['trade']['input']['commodity'].unique()
        #pipeline_input = [i for i in pipeline_input if 'pipeline_capacity' in i]
        #print(f"......{pipeline_input}")
        #use_input = trade_dict[t]['trade']['input'].copy()
        #use_input = use_input[use_input['commodity'].isin(pipeline_input) == False]
        #trade_dict[t]['trade']['input'] = use_input

        use_inv = trade_dict[t]['flow']['inv_cost']
        #use_inv['technology'] = use_inv['technology'].str.replace('pipe', 'piped_exp')
        use_inv['value'] /= 10
        trade_dict[t]['flow']['inv_cost'] = use_inv

        #if t in ['loil_piped', 'foil_piped', 'crudeoil_piped']:
        #    use_inv['technology'] = use_inv['technology'].str.replace('oil_piped', t)

        #trade_dict[t]['trade']['inv_cost'] = use_inv

        #use_tlt = trade_dict[t]['trade']['technical_lifetime']
        #use_tlt['value'] = 30 # Assume 30 year lifetime for pipelines
        #trade_dict[t]['trade']['technical_lifetime'] = use_tlt

# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(f"Base model: {base_model}/{base_scen}")
    print(f"Target model: alps_hhi/{model_scen}")

    load_and_solve(trade_dict = trade_dict,
                   solve = True,
                   project_name = 'alps_hhi', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'alps_hhi',
                   target_scen = model_scen)