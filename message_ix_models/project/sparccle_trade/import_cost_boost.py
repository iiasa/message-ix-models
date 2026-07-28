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
from itertools import product

# Import scenario and models
config, config_path = load_config(project_name = 'sparccle_trade', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

mp = ixmp.Platform()

# Trade friction functions
def friction_dictionary(friction_endyear:int = 2100):

    # Import scenario and models
    config, config_path = load_config(project_name = 'sparccle_trade', config_name = 'config.yaml')
    data_path = package_data_path("bilateralize")

    sens_i = config['restriction']['exporters']
    sens_j = config['restriction']['importers']
    sens_techs = config['restriction']['technologies']

    base_years = [2030, 2035, 2040, 2045, 2050, 2055,
                  2060, 2070, 2080, 2090, 2100, 2110]
    fric_years = [y for y in base_years if y <= friction_endyear]
    
    bound_out = pd.DataFrame()
    for tec in sens_techs:
        tec_list = sens_techs
        
        basedf = pd.DataFrame(product(sens_i, tec_list,
                                  fric_years,
                                  ["M1"],
                                  ["year"]))
        basedf.columns = ['node_loc', 'technology', 'year_act', 'mode', 'time']

        bounddf = message_ix.make_df(
            "bound_activity_up",
            node_loc = basedf['node_loc'],
            technology = basedf['technology'],
            value = 0,
            year_act = basedf['year_act'],
            mode = basedf['mode'],
            time = basedf['time'],
            unit = '-')

        bound_out = pd.concat([bound_out, bounddf])
        
    return bound_out

# Increase import costs
def import_cost_update(scenario,
                       importers_list:list,
                       cost_multiplier:int,
                       cost_parameter:str = "fix_cost"):

    df = scenario.par(cost_parameter)
    
    for i in importers_list:
        print(f"Increase fixed costs on fuel exports destined for {i}")
        
        cdf = df[df['technology'].str.contains(f'_exp_{i}')].copy()
        cdf['commodity'] = cdf['technology'].str.split('_exp_').str[0]
        cdf = cdf[cdf['commodity'].isin(['coal_shipped', 'crudeoil_shipped', 'crudeoil_piped', 'gas_piped', 
                                         'foil_piped', 'foil_shipped', 'loil_piped', 'loil_shipped',
                                         'LNG_shipped'])]
        cdf = cdf.drop(columns = ['commodity'])
        cdf_new = cdf.copy()
        cdf_new['value'] *= cost_multiplier
    
        printdf_old = cdf[cdf['year_act'] == 2035]
        printdf_new = cdf_new[cdf_new['year_act'] == 2035]
        print("### Default Fixed Costs ###")
        print(printdf_old)
        print("### Updated Fixed Costs ###")
        print(printdf_new)
    
        with scenario.transact(f"Update import costs for {i}"):
            scenario.remove_par("fix_cost", cdf)
            scenario.add_par("fix_cost", cdf_new)

# Run scenarios
for in_scen in ['SSP3_NPiREF', 'SSP3_STS3']:
    base_model = 'sparccle_trade'
    base_scen = in_scen
    
    base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
    out_scenario = base_scenario.clone('sparccle_trade', base_scen + "_frictions", keep_solution = False)
    out_scenario.set_as_default()

    # Increase import costs
    import_cost_update(scenario = out_scenario,
                       importers_list = ["eeu", "weu"],
                       cost_multiplier = 1.2)
    import_cost_update(scenario = out_scenario,
                       importers_list = ["afr", "chn", "fsu", 
                                         "lam", "mea", "nam", 
                                         "pao", "pas", "rcpa", "sas"],
                       cost_multiplier = 1.2)

    # Add FSU trade friction
    fsu_bound = friction_dictionary()
    
    with out_scenario.transact(f"Add friction sensitivity"):
        out_scenario.add_par('bound_activity_up', fsu_bound)

    with out_scenario.transact("Remove constraints on shocked technologies"):
        for par in ["growth_activity_lo", "growth_activity_up", "initial_activity_lo", "initial_activity_up"]:
            basepar = out_scenario.par(par, filters = {"technology": fsu_bound['technology'],
                                                       "node_loc": fsu_bound['node_loc']})
            if len(basepar) != 0:
                print(f"...{par}")
                out_scenario.remove_par(par, basepar)
                
    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})

mp.close_db()