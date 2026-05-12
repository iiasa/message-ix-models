# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.tools.bilateralize.liquefaction_calibration import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'sparccle_trade', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

for in_scen in ['SSP3_NPiREF', 'SSP3_STS3']:
    base_model = 'sparccle_trade'
    base_scen = in_scen
    
    mp = ixmp.Platform()
    
    base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
    out_scenario = base_scenario.clone('sparccle_trade', base_scen + "_ins10", keep_solution = False)
    
    print("Increase shipping costs to proxy for insurance")
    cdf = out_scenario.par("var_cost")
    cdf = cdf[cdf['technology'].str.contains('_tanker_')]
    cdf_new = cdf.copy()
    cdf_new['value'] *= 10
    
    with out_scenario.transact("Update shipped technology variable costs"):
        out_scenario.remove_par("var_cost", cdf)
        out_scenario.add_par("var_cost", cdf_new)
        
    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})
mp.close_db()