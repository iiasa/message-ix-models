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
base_scen = 'FSU2040_NAM1000'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone(base_model, base_scen + '_update',
                                   keep_solution = False)
out_scenario.set_as_default()

print("Remove dynamic constraint in shock year")
for par in ['growth_activity_lo', 'initial_activity_lo', 'growth_activity_up', 'initial_activity_up']:
    par_base = out_scenario.par(par, filters = {'year_act':[2030]})
    for tec in ['crudeoil_shipped', 'LNG_shipped', 'lightoil_shipped']:
        par_rem = par_base[par_base['technology'].str.contains(tec)]
        with out_scenario.transact(f'Remove {par} from {tec}'):
            out_scenario.remove_par(par, par_rem)
    
print("Remove balance equality sets")
be_rem = out_scenario.set("balance_equality")
be_rem = be_rem[(be_rem['level'].str.contains('import'))|(be_rem['level'].str.contains('export'))]
print(be_rem)
with out_scenario.transact('remove balance equality for import/export'):
    out_scenario.remove_set('balance_equality', be_rem)
    
print("Add balance equality sets")
be_df = out_scenario.par("output")
for tec in config['covered_trade_technologies']:
    print(f"---Add {tec}")
    tecdf = be_df[be_df['technology'].str.contains(tec)].copy()
    comdf = tecdf[['commodity', 'level']].drop_duplicates()
    print(comdf)
    comdf = comdf[comdf['level'].isin(['piped', 'shipped'])].drop_duplicates()

    with out_scenario.transact(f"add balance equality sets for {tec}"):
        out_scenario.add_set("balance_equality", comdf)
        
print("Solve scenario")
out_scenario.solve()
mp.close_db()
