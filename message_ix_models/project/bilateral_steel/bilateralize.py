# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'bilateral_steel', config_name = 'config.yaml')
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
prepare_edit_files(project_name = 'bilateral_steel', 
                   config_name = 'config.yaml',
                   P_access = True,)
                   #reimport_BACI = True)

# Generate scenario
trade_dict = bare_to_scenario(project_name = 'bilateral_steel', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

print(trade_dict)

print("Setting up scenario")
mp = ixmp.Platform()
mp.add_unit("USD/Mt")

load_and_solve(trade_dict = trade_dict,
               solve = False,
               project_name = 'bilateral_steel', 
               config_name = 'config.yaml', 
               start_model = models_scenarios['SSP2']['model'],
               start_scen = models_scenarios['SSP2']['scenario'],
               target_model = 'steel_trade',
               target_scen = 'only_steel')