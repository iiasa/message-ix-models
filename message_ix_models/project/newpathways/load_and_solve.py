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
from message_ix_models.project.newpathways.liquefaction_calibration import *

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

# # Update scenario: default values
# clone_and_update(trade_dict=trade_parameters,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  update_scenario_name = 'pipelines_LNG')

# Update scenario: Increase liquefaction gas penalty
additional_parameters_input = update_liquefaction_input(message_regions = message_regions,
                                                        project_name = 'newpathways',
                                                        config_name = 'config.yaml')
clone_and_update(trade_dict=trade_parameters,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True,
                 additional_parameter_updates = additional_parameters_input,
                 update_scenario_name = 'LNG_prod_penalty')

# # Update scenario: no cost on flow technology
# trade_parameters_novar = pd.read_pickle(tdf)
# del trade_parameters_novar['LNG_shipped']['flow']['var_cost']
# del trade_parameters_novar['LNG_shipped']['flow']['inv_cost']

# clone_and_update(trade_dict=trade_parameters_novar,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  update_scenario_name = 'LNG_noFLcost')

# # Update scenario: no variable cost on trade for LNG 
# trade_parameters_nofix = pd.read_pickle(tdf)
# del trade_parameters_nofix['LNG_shipped']['trade']['var_cost']

# clone_and_update(trade_dict=trade_parameters_nofix,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  update_scenario_name = 'LNG_noTRvarcost')

# # Update scenario: Increase var costs to full export values (5x)
# trade_parameters_LNGin = pd.read_pickle(tdf)
# update_varcost = pd.DataFrame.from_dict(dict(technology = ['LNG_shipped_exp_afr',
#                                                            'LNG_shipped_exp_chn',
#                                                            'LNG_shipped_exp_eeu',
#                                                            'LNG_shipped_exp_fsu',
#                                                            'LNG_shipped_exp_lam',
#                                                            'LNG_shipped_exp_mea',
#                                                            'LNG_shipped_exp_nam',
#                                                            'LNG_shipped_exp_pao',
#                                                            'LNG_shipped_exp_pas',
#                                                            'LNG_shipped_exp_rcpa',
#                                                            'LNG_shipped_exp_sas',
#                                                            'LNG_shipped_exp_weu'],
#                                              multiplier = [5, 5, 5, 5, 5, 5,
#                                                            5, 5, 5, 5, 5, 5]))
# additional_parameters_varcost = {'var_cost': update_varcost}

# clone_and_update(trade_dict=trade_parameters,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  additional_parameter_updates = additional_parameters_varcost,
#                  update_scenario_name = 'LNG_lowTRvarcost')

# # Update scenario: Increase fuel requirements for LNG shipping by 50%
# trade_parameters_LNGin = pd.read_pickle(tdf)
# update_input = pd.DataFrame.from_dict(dict(technology = ['LNG_tanker_LNG', 'LNG_tanker_foil'],
#                                            multiplier = [1.5, 1.5]))
# additional_parameters_input = {'input': update_input}

# clone_and_update(trade_dict=trade_parameters,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  additional_parameter_updates = additional_parameters_input,
#                  update_scenario_name = 'high_LNGtankerfuel')

# # Update scenario: Reduce NAM extraction costs by 20%
# update_var_cost = pd.DataFrame.from_dict(dict(node_loc = ['R12_NAM',
#                                                           'R12_NAM',
#                                                           'R12_NAM',
#                                                           'R12_NAM',
#                                                           'R12_NAM',
#                                                           'R12_NAM',
#                                                           'R12_NAM'],
#                                               technology = ['gas_extr_1',
#                                                             'gas_extr_2',
#                                                             'gas_extr_3',
#                                                             'gas_extr_4',
#                                                             'gas_extr_5',
#                                                             'gas_extr_6',
#                                                             'gas_extr_7'],
#                                               multiplier = [0.8,
#                                                             0.8,
#                                                             0.8,
#                                                             0.8,
#                                                             0.8,
#                                                             0.8,
#                                                             0.8]))

# additional_parameters_varcost = {'var_cost': update_var_cost}

# clone_and_update(trade_dict=trade_parameters,
#                  project_name = 'newpathways',
#                  config_name = 'config.yaml',
#                  log=log,
#                  to_gdx = False,
#                  solve = True,
#                  additional_parameter_updates = additional_parameters_varcost,
#                  update_scenario_name = 'lowNAMshalecost')