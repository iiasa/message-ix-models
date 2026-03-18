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

base_model = 'SSP_SSP2_v6.4'
base_scen = 'baseline'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone('weu_security', "SSP_SSP2_v6.4", keep_solution = False)
#df = base_scenario.var("ACT", filters = {"technology": "LNG_shipped_exp_eeu",
#                                         "year_act": 2030})
#print(df)

#base_vcost
#print("Solve scenario")
out_scenario.solve()
mp.close_db()
