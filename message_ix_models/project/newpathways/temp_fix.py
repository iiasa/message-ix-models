# -*- coding: utf-8 -*-
"""
Run scenario(s)
"""
# Import packages
import os
import sys
import pandas as pd
import logging
import yaml
import message_ix
import ixmp
import itertools
import pickle

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *
from message_ix_models.tools.bilateralize.pull_gem import *
from message_ix_models.tools.bilateralize.mariteam_calibration import *

# Bring in configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')

covered_tec = config['covered_trade_technologies']
message_regions = config['scenario']['regions']

# Get logger
log = get_logger(__name__)

# Import pickle of parameter definitions
tdf = os.path.join(os.path.dirname(config_path), 'scenario_parameters.pkl')
trade_parameters = pd.read_pickle(tdf)

# Load the scenario
mp = ixmp.Platform()
start_model = config.get("scenario", {}).get("target_model")
start_scen = 'coal'

scen = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
#scen = base.clone(start_model, 'gas-LNG-coal', keep_solution=False)
#scen = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
scen.set_as_default()
#scen.remove_solution()

# rem_df = trade_parameters['coal_shipped']['trade']['var_cost']

rem_df = scen.par('relation_activity', filters = {'relation': ['domestic_coal']})
rem_df2 = scen.par('relation_upper', filters = {'relation': ['domestic_coal']})
# add_df = rem_df.copy().drop(['value'], axis = 1)

# add_df_v = update_liquefaction_input(message_regions = message_regions,
#                                      project_name = 'newpathways',
#                                      config_name = 'config.yaml')['input']
# add_df = add_df.merge(add_df_v, 
#                       left_on = ['node_loc', 'technology', 'commodity', 'level'],
#                       right_on = ['node_loc', 'technology', 'commodity', 'level'],
#                       how = 'left')

with scen.transact("remove domestic coal relation upper"): 
    scen.remove_par('relation_upper', rem_df2)

    #.add_par('input', add_df)

#with scen.transact("Add var cost to LNG trade"): 
#    scen.remove_par('var_cost', add_df)
    
solver = "MESSAGE"
scen.solve(solver, solve_options=dict(lpmethod=4))


save_to_gdx(mp = mp,
            scenario = scen,
            output_path = Path(os.path.join(gdx_location, 'MsgData_'+ start_model + '_' + start_scen + '.gdx')))     



input_coal_trd = base.par('input', filters = {'technology': ['coal_trd']})
output_coal_trd = base.par('output', filters = {'technology': ['coal_trd']})

with base.transact("remove coal_trd"): 
    base.remove_set('technology', 'coal_trd')   
    
check = scen_nocoal.par('output', filters = {'technology': ['coal_imp'],
                                             'commodity': ['coal'],
                                             'level': ['secondary']
                                             })