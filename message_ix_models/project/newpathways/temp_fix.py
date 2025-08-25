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
start_scen = 'pipelines_LNG'

scen = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
scen.set_as_default()

add_df = trade_parameters['LNG_shipped']['trade']['var_cost']

scen.remove_solution()

rem_df = scen.par('inv_cost', filters = {'technology': list(add_df['technology'].unique())})

with scen.transact("Remove inv cost to LNG trade"): 
    scen.remove_par('inv_cost', rem_df)
    #scen.add_par('inv_cost', add_df)

with scen.transact("Add var cost to LNG trade"): 
    scen.remove_par('var_cost', add_df)
    
solver = "MESSAGE"
scen.solve(solver, solve_options=dict(lpmethod=4))







    