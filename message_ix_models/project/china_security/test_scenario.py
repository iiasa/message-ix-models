# -*- coding: utf-8 -*-
"""
Test minor updates to scenarios
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

base_model = 'china_security'
base_scen = 'SSP2_Baseline'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone('china_security', "test", keep_solution = False)

print("Adjust re-exports for lightoil and fueloil")
adjust_reexports(base_scenario = out_scenario,
                 trade_commodity_list = ['lightoil', 'fueloil'],
                 base_level = 'secondary')
    
print("Solve scenario")
out_scenario.solve()
mp.close_db()