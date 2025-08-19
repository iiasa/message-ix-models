# -*- coding: utf-8 -*-
"""
Move data from bare files to the MESSAGEix scenario
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

# Read and inflate sheets based on model horizon
trade_dict = build_parameter_sheets(project_name = 'newpathways', config_name = 'config.yaml',
                                    log=log)

# Historical calibration for trade technology
histdf = build_historical_activity(message_regions,
                                   project_name = 'newpathways', config_name = 'config.yaml')
histdf = histdf[histdf['year_act'].isin([2000, 2005, 2010, 2015, 2020, 2023])]
histdf['year_act'] = np.where(histdf['year_act'] == 2023, 2025, histdf['year_act']) # TODO: Assume 2023 values FOR NOW 
histdf = histdf[histdf['value'] > 0]

histnc = build_historical_new_capacity_trade(message_regions,
                                             project_name = 'newpathways', config_name = 'config.yaml')

hist_tec = {}
for tec in covered_tec:
    add_tec = config[tec + '_trade']['trade_technology'] + '_exp'
    hist_tec[tec] = add_tec

for tec in hist_tec.keys():
    log.info('Add historical activity for ' + tec)
    add_df = histdf[histdf['technology'].str.contains(hist_tec[tec])]
    trade_dict[tec]['trade']['historical_activity'] = add_df
    
    log.info('Add historical new capacity for ' + tec)
    add_df = histnc[histnc['technology'].str.contains(hist_tec[tec])]
    trade_dict[tec]['trade']['historical_new_capacity'] = add_df

# Historical new capacity for maritime shipping
hist_lng_foil = build_historical_new_capacity_flow('LNG Tankers.csv', 'LNG_tanker_foil',
                                                   project_name = 'newpathways', config_name = 'config.yaml')
hist_lng_lng = build_historical_new_capacity_flow('LNG Tankers.csv', 'LNG_tanker_LNG',
                                                  project_name = 'newpathways', config_name = 'config.yaml')
hist_lng_foil['value'] *= 0.2 # Assume 20% of historical LNG tankers are propelled using diesel
hist_lng_lng['value'] *= 0.8 # Assume 80% of historical LNG tankers are propelled using LNG
hist_lng = pd.concat([hist_lng_foil, hist_lng_lng])
trade_dict['LNG_shipped']['flow']['historical_new_capacity'] = hist_lng

## MANUAL ADDITIONS
# Set emission factors for piped gas # TODO
# tdf = pd.read_csv(os.path.join(package_data_path("bilateralize"), "gas_piped", "bare_files", "emission_factor.csv"))
# trade_dict['gas_piped']['trade']['emission_factor'] = tdf

# Set WEU imports of piped gas from FSU to 0 in 2025 #TODO: Check, as there are some exports
# tdf = trade_dict['gas_piped']['trade']['historical_activity']
# add_df = {'node_loc': ['R12_FSU'],
#           'technology': ['gas_piped_exp_weu'],
#           'year_act': [2025],
#           'mode': ['M1'],
#           'time': ['year'],
#           'value': [0],
#           'unit': ['GWa']}
# tdf = pd.concat([tdf, pd.DataFrame.from_dict(add_df)])
# trade_dict['gas_piped']['trade']['historical_activity'] = tdf.reset_index(drop = True)

# Save trade_dictionary
tdf = os.path.join(os.path.dirname(config_path), 'scenario_parameters.pkl')
with open(tdf, 'wb') as f: pickle.dump(trade_dict, f)

