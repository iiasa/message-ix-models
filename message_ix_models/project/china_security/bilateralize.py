# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for Chinese energy security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.china_security.adjust_reexports import *


import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'china_security', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Clear bare files
print("Clearing edit and bare files")
for type in ["edit", "bare"]:
    for tec in config['covered_trade_technologies']:
        if os.path.exists(os.path.join(data_path, tec, f"{type}_files")):
           for file in os.listdir(os.path.join(data_path, tec, f"{type}_files")):
                if os.path.isfile(os.path.join(data_path, tec, f"{type}_files", file)):
                   os.remove(os.path.join(data_path, tec, f"{type}_files", file))
        if os.path.exists(os.path.join(data_path, tec, f"{type}_files", "flow_technology")):
            for file in os.listdir(os.path.join(data_path, tec, f"{type}_files", "flow_technology")):
                if os.path.isfile(os.path.join(data_path, tec, f"{type}_files", "flow_technology", file)):
                    os.remove(os.path.join(data_path, tec, f"{type}_files", "flow_technology", file))
        
# Generate edit files
prepare_edit_files(project_name = 'china_security', 
                   config_name = 'config.yaml',
                   P_access = True)

# Add scenario updates for project
print("Add scenario updates for project")
for tec in config['update_tec']:
    print(f"...{tec}")
    if os.path.exists(package_data_path("china_security", "scenario_updates", tec)):
        for file in os.listdir(package_data_path("china_security", "scenario_updates", tec)):
            base_file = package_data_path("china_security", "scenario_updates", tec, file)
            if ".csv" in str(base_file):
                dest_file = os.path.join(data_path, tec, "bare_files", file)
                shutil.copy2(base_file, dest_file)
                print(f"Copied file from scenario_updates to bare: {file}")
                
# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'china_security', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(f"Base model: {base_model}/{base_scen}")
    print(f"Target model: china_security/{model_scen}")

    print("Setting up scenario")
    load_and_solve(trade_dict = trade_dict,
                   solve = False,
                   project_name = 'china_security', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'china_security',
                   target_scen = model_scen)

    print("Updating extraction constraints")
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model='china_security', scenario=model_scen)
    out_scenario = base_scenario.clone('china_security', model_scen)
    out_scenario.set_as_default()
            
    print("Add balance equality sets")
    be_df = out_scenario.par("output", filters = {"technology": config['covered_trade_technologies']})
    be_df = be_df[be_df['level'].isin(['piped', 'shipped'])]
    be_df = be_df[['commodity', 'level']].drop_duplicates()

    with out_scenario.transact("add balance equality sets"):
        out_scenario.add_set("balance_equality", be_df)

    print("Adjust re-exports for lightoil and fueloil")
    adjust_reexports(base_scenario = out_scenario,
                     trade_commodity_list = ['lightoil', 'fueloil'],
                     base_level = 'secondary')
    
    print("Solve scenario")
    out_scenario.solve(solve_options={'barcrossalg':'2'})
    mp.close_db()