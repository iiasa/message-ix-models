# -*- coding: utf-8 -*-
"""
Update scenario for testing
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

mp = ixmp.Platform()

for in_scen in ['SSP3_NPiREF']:
    base_model = 'sparccle_trade'
    base_scen = in_scen
    
    base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
    out_scenario = base_scenario.clone('sparccle_trade', base_scen + "_holdha", keep_solution = False)
    
    print("Hold level of trade in 2030 for historical year as 2025 (FMY is 2035)")
    hadf = out_scenario.par('historical_activity')
    hadf = hadf[(hadf['technology'].str.contains("_imp"))|(hadf['technology'].str.contains("_exp_"))]
    hadf = hadf[hadf['year_act'] == 2025]
    hadf['year_act'] = 2030
    print(hadf[0:10])
    
    with out_scenario.transact("hold level of trade in 2030"):
        out_scenario.add_par('historical_activity', hadf)
        
    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})
    
mp.close_db()
