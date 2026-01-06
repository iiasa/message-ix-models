# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for ALPS HHI Analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

import os
from ixmp import Platform

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

# Add scenario updates for project
print("Add scenario updates for project")
for tec in config['covered_trade_technologies']:
    print(f"...{tec}")
    if os.path.exists(package_data_path("alps_hhi", "scenario_updates", tec)):
        for file in os.listdir(package_data_path("alps_hhi", "scenario_updates", tec)):
            base_file = package_data_path("alps_hhi", "scenario_updates", tec, file)
            if ".csv" in str(base_file):
                dest_file = os.path.join(data_path, tec, "bare_files", file)
                shutil.copy2(base_file, dest_file)
                print(f"Copied file from scenario_updates to bare: {file}")
                
# Add constraints to the dictionary
print("Add constraints to the dictionary")
constraint_pars = ["initial_activity_lo", "initial_activity_up",
                   "growth_activity_lo", "growth_activity_up"]
constraint_tec = config['constrained_tec']

for con in constraint_pars:
    print(f"...{con}")
    for tec in constraint_tec:
        print(f"......{tec}")
        df = pd.read_csv(os.path.join(package_data_path("alps_hhi", "scenario_updates", tec), con + ".csv"))

        df['omit'] = 0
        df['omit'] = np.where((df['technology'].isin(['LNG_shipped_exp_weu',
                                                     'gas_piped_exp_weu']) == False) &\
                              (df['node_loc'] != 'R12_WEU'),
                             1, 0)
        df = df[df['omit'] == 0]
        df = df.drop(columns = ['omit'])
        
        df.to_csv(os.path.join(data_path, tec, "bare_files", con + ".csv"), index = False)

# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'alps_hhi', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(f"Base model: {base_model}/{base_scen}")
    print(f"Target model: alps_hhi/{model_scen}")

    print("Setting up scenario")
    load_and_solve(trade_dict = trade_dict,
                   solve = False,
                   project_name = 'alps_hhi', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'alps_hhi',
                   target_scen = model_scen)

    print("Updating extraction constraints")
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model='alps_hhi', scenario=model_scen)
    hhi_scenario = base_scenario.clone('alps_hhi', model_scen)
    hhi_scenario.set_as_default()

    updf = hhi_scenario.par('growth_activity_up')
    updf = updf[(updf['technology'].str.contains('gas_extr_mpen'))]
    updf = updf[updf['node_loc'].isin(['R12_WEU'])]

    remdf = updf.copy()
    updf['value'] = 0.01

    with hhi_scenario.transact("update growth activity up to gas_extr_mpen"):
        hhi_scenario.remove_par('growth_activity_up', remdf)
        hhi_scenario.add_par('growth_activity_up', updf)

    print("Solve scenario")
    hhi_scenario.solve()
    mp.close_db()