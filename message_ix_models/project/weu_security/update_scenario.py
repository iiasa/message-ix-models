# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.weu_security.liquefaction_calibration import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

base_model = 'weu_security'
base_scen = 'SSP2_INDC2030'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone('weu_security', "SSP_INDC2030_loose", keep_solution = False)

print("Remove emissions constraints on non-WEU or EEU regions")
remdf = out_scenario.par("bound_emission")
remdf = remdf[remdf['node'].isin(['R12_WEU', 'R12_EEU']) == False]
with out_scenario.transact(f"remove emission bound for indc scenario"):
    out_scenario.remove_par("bound_emission", remdf)
    
print("Solve scenario")
out_scenario.solve()
mp.close_db()
