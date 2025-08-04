# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 15:51:14 2025

@author: shepard
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

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *
from message_ix_models.tools.bilateralize.pull_gem import *
from message_ix_models.tools.bilateralize.mariteam_calibration import *

# Load config
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')
   
# Load the scenario
mp = ixmp.Platform()
 
# Clone scenario
start_model = config.get("scenario", {}).get("start_model", [])
start_scen = config.get("scenario", {}).get("start_scen", [])

target_model = config.get("scenario", {}).get("target_model", [])
target_scen = config.get("scenario", {}).get("target_scen", [])

base = message_ix.Scenario(mp, start_model, start_scen)
scen = base.clone("NP_SSP2_baseline", "v5.3", keep_solution=True)
scen.set_as_default()
solver = "MESSAGE"
scen.solve(solver, solve_options=dict(lpmethod=4))
  
# Sandbox
df = trade_dict['LNG_shipped']['flow']['historical_activity']
teclist = list(df['technology'].unique())
basedf = scen.par('historical_activity', filters = {'technology': teclist})

with scen.transact('Add historical activity for LNG tanker'):
    scen.remove_par('historical_activity', basedf)
    scen.add_par('historical_activity', df)
      
gdx_location: str = os.path.join("H:", "script", "message_ix", "message_ix", "model", "data")
save_to_gdx(mp = mp,
            scenario = scen,
            output_path = Path(os.path.join(gdx_location, 'MsgData_'+ target_model + '_' + target_scen + '.gdx')))     
      
