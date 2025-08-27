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

# Update scenario: default values
clone_and_update(trade_dict=trade_parameters,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True,
                 update_scenario_name = 'pipelines_LNG')

# Update scenario: no cost on flow technology
trade_parameters_novar = pd.read_pickle(tdf)
del trade_parameters_novar['LNG_shipped']['flow']['var_cost']
del trade_parameters_novar['LNG_shipped']['flow']['inv_cost']

clone_and_update(trade_dict=trade_parameters_novar,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True,
                 update_scenario_name = 'LNG_noFLcost')

# Update scenario: no fixed cost on trade for LNG 
trade_parameters_nofix = pd.read_pickle(tdf)
del trade_parameters_nofix['LNG_shipped']['trade']['fix_cost']

clone_and_update(trade_dict=trade_parameters_nofix,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True,
                 update_scenario_name = 'LNG_noTRfixcost')

# Update scenario: Reduce NAM shale to MEA levels
update_var_cost = pd.DataFrame.from_dict(dict(node_loc = ['R12_NAM',
                                                          'R12_NAM',
                                                          'R12_NAM',
                                                          'R12_NAM'],
                                              technology = ['gas_extr_4',
                                                            'gas_extr_5',
                                                            'gas_extr_6',
                                                            'gas_extr_7'],
                                              multiplier = [0.8,
                                                            0.8,
                                                            0.8,
                                                            0.8])) # MEA values
# update_inv_cost = pd.DataFrame.from_dict(dict(node_loc = ['R12_NAM'],
#                                               technology = ['gas_extr_7'],
#                                               multiplier = [0.9]))

additional_parameters = {'var_cost': update_var_cost}

clone_and_update(trade_dict=trade_parameters,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True,
                 additional_parameter_updates = additional_parameters,
                 update_scenario_name = 'lowNAMshalecost')