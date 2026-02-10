# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.gas_security.liquefaction_calibration import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'gas_security', config_name = 'config.yaml')
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
prepare_edit_files(project_name = 'gas_security', 
                   config_name = 'config.yaml',
                   P_access = True)

# Add scenario updates for project
print("Add scenario updates for project")
for tec in config['constrained_tec']:
    print(f"...{tec}")
    if os.path.exists(package_data_path("gas_security", "scenario_updates", tec)):
        for file in os.listdir(package_data_path("gas_security", "scenario_updates", tec)):
            base_file = package_data_path("gas_security", "scenario_updates", tec, file)
            if ".csv" in str(base_file):
                dest_file = os.path.join(data_path, tec, "bare_files", file)
                shutil.copy2(base_file, dest_file)
                print(f"Copied file from scenario_updates to bare: {file}")
                
# Move data from bare files to a dictionary to update a MESSAGEix scenario
trade_dict = bare_to_scenario(project_name = 'gas_security', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

# Additional liquefaction calibration
liquefaction_parameters = update_liquefaction_input(message_regions = "R12",
                                                     project_name = 'gas_security',
                                                     config_name = 'config.yaml')
# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(f"Base model: {base_model}/{base_scen}")
    print(f"Target model: gas_security/{model_scen}")

    print("Setting up scenario")
    load_and_solve(trade_dict = trade_dict,
                   solve = False,
                   project_name = 'gas_security', 
                   config_name = 'config.yaml', 
                   start_model = base_model,
                   start_scen = base_scen,
                   target_model = 'gas_security',
                   target_scen = model_scen,
                   extra_parameter_updates = liquefaction_parameters)

    print("Updating extraction constraints")
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model='gas_security', scenario=model_scen)
    out_scenario = base_scenario.clone('gas_security', model_scen)
    out_scenario.set_as_default()

    for g in ['growth_activity_up']:
        updf = out_scenario.par(g)
        updf = updf[(updf['technology'].str.contains('gas_extr_mpen'))]
        updf = updf[updf['node_loc'].isin(['R12_WEU'])]
    
        remdf = updf.copy()
        if g == 'growth_activity_up':
            updf['value'] = 0.01
        elif g == 'growth_activity_lo':
            updf['value'] = -0.01
            
        with out_scenario.transact("update growth activity to gas_extr_mpen"):
            out_scenario.remove_par(g, remdf)
            out_scenario.add_par(g, updf)

#    print("Update crude resource remaining")
#    resdf =  out_scenario.par("resource_remaining", filters = {"commodity": ["crude_1", "crude_2", "crude_3", "crude_4",
#                                                                             "crude_5", "crude_6", "crude_7"]})
#    remdf = resdf.copy()
#    resdf['value'] = 0.05
#    with out_scenario.transact("update resource remaining for crude in EEU, LAM, PAO"):
#        out_scenario.remove_par("resource_remaining", remdf)
#        out_scenario.add_par("resource_remaining", resdf)

    print("Remove oil_imp_c relation activity")
    for p in ["relation_activity", "relation_upper", "relation_lower"]:
        remdf = out_scenario.par(p, filters = {"relation": "oil_imp_c"})
        with out_scenario.transact(f"remove relation {p}"):
            out_scenario.remove_par(p, remdf)
            
    print("Solve scenario")
    out_scenario.solve()
    mp.close_db()