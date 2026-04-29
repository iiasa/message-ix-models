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
base_scen = 'SSP2'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone('weu_security', "tec_lt", keep_solution = False)

print("Update pipeline lifetime")
cdf = out_scenario.par("technical_lifetime")
cdf = cdf[cdf['technology'].str.contains('_pipe_')]
cdf_new = cdf.copy()
cdf_new['value'] = 50

with out_scenario.transact("Update pipeline lifetime"):
    out_scenario.remove_par("technical_lifetime", cdf)
    out_scenario.add_par("technical_lifetime", cdf_new)
    
print("Solve scenario")
out_scenario.solve()
mp.close_db()
