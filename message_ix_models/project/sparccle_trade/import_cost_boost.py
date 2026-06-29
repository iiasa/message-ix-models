# -*- coding: utf-8 -*-
"""
Make the cost of importing fossil fuels higher for Europe
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
    out_scenario = base_scenario.clone('sparccle_trade', base_scen + "_impcosts_hi", keep_solution = False)
    
    print("Increase imports costs on fuel exports destined for Europe")
    cdf = out_scenario.par("import_cost")
    cdf = cdf[cdf['commodity'].isin(['coal', 'crudeoil', 'gas', 'fueloil', 'lightoil', 'LNG'])
    cdf_new = cdf.copy()
    cdf_new['value'] *= 2
    
    with out_scenario.transact("Update import costs costs"):
        out_scenario.remove_par("import_cost", cdf)
        out_scenario.add_par("import_cost", cdf_new)
        
    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})

    mp.close_db()