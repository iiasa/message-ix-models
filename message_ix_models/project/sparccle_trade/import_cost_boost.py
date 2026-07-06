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

mp = ixmp.Platform()
for in_scen in ['SSP3_NPiREF', 'SSP3_STS3']:
    base_model = 'sparccle_trade'
    base_scen = in_scen
    
    base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
    out_scenario = base_scenario.clone('sparccle_trade', base_scen + "_impcosts_hi", keep_solution = False)
    out_scenario.set_as_default()
    
    print("Increase fixed costs on fuel exports destined for Europe")
    cdf = out_scenario.par("fix_cost")
    cdf = cdf[(cdf['technology'].str.contains('_exp_weu'))|(cdf['technology'].str.contains('_exp_eeu'))]
    cdf['commodity'] = cdf['technology'].str.split('_exp_').str[0]
    cdf = cdf[cdf['commodity'].isin(['coal_shipped', 'crudeoil_shipped', 'crudeoil_piped', 'gas_piped', 
                                     'foil_piped', 'foil_shipped', 'loil_piped', 'loil_shipped',
                                     'LNG_shipped'])]
    cdf = cdf.drop(columns = ['commodity'])
    cdf_new = cdf.copy()
    cdf_new['value'] *= 2

    printdf_old = cdf[cdf['year_act'] == 2035]
    printdf_new = cdf_new[cdf_new['year_act'] == 2035]
    print("### Default Fixed Costs ###")
    print(printdf_old)
    print("### Updated Fixed Costs ###")
    print(printdf_new)
    
    with out_scenario.transact("Update import costs costs"):
        out_scenario.remove_par("fix_cost", cdf)
        out_scenario.add_par("fix_cost", cdf_new)
        
    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})

mp.close_db()