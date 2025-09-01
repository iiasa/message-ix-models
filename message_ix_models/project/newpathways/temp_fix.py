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
start_scen = 'pipelines_only'

#base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
#scen = base.clone(start_model, 'baseline_NAMrsc', keep_solution=False)
scen = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
scen.set_as_default()
scen.remove_solution()

#add_df = trade_parameters['LNG_shipped']['trade']['var_cost']

rem_df = scen.par('input', filters = {'technology': ['gas_pipe_afr',
                                                     'gas_pipe_chn',
                                                     'gas_pipe_eeu',
                                                     'gas_pipe_fsu',
                                                     'gas_pipe_lam',
                                                     'gas_pipe_mea',
                                                     'gas_pipe_nam',
                                                     'gas_pipe_pao',
                                                     'gas_pipe_pas',
                                                     'gas_pipe_rcpa',
                                                     'gas_pipe_sas',
                                                     'gas_pipe_weu'],
                                         'commodity': 'steel'})
#add_df = rem_df.copy()
#add_df['value'] = 0.00002

with scen.transact("Remove steel input for pipelines"): 
    scen.remove_par('input', rem_df)
    #scen.add_par('input', add_df)

#with scen.transact("Add var cost to LNG trade"): 
#    scen.remove_par('var_cost', add_df)
    
solver = "MESSAGE"
scen.solve(solver, solve_options=dict(lpmethod=4))







    