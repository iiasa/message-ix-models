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
trade_dict = build_parameter_sheets(log=log)

# Historical calibration for trade technology
histdf = build_historical_activity(message_regions)
histdf = histdf[histdf['year_act'].isin([2000, 2005, 2010, 2015, 2020, 2023])]
histdf['year_act'] = np.where(histdf['year_act'] == 2023, 2025, histdf['year_act']) # TODO: Assume 2023 values FOR NOW 
histdf = histdf[histdf['value'] > 0]

hist_tec = {}
for tec in covered_tec:
    add_tec = config[tec + '_trade']['trade_technology'] + '_exp'
    hist_tec[tec] = add_tec

for tec in hist_tec.keys():
    log.info('Add historical activity for ' + tec)
    add_df = histdf[histdf['technology'].str.contains(hist_tec[tec])]
    trade_dict[tec]['trade']['historical_activity'] = add_df

# Historical calibration for pipelines
for tec in [i for i in covered_tec if 'piped' in i]:
    histdf = pd.read_csv(os.path.join(os.path.dirname(package_data_path("bilateralize", tec)), tec, "GEM", "GEM.csv"))
    histdf['node_loc'] = histdf['EXPORTER']
    histdf['importer'] = histdf['IMPORTER'].str.replace(config['scenario']['regions'] + '_', '').str.lower()
    histdf['technology'] = config[tec + '_trade']['flow_technology'] + '_' + histdf['importer']
    histdf['year_act'] = 2025 # last historical year
    histdf['mode'] = 'M1'
    histdf['time'] = 'year'
    histdf['value'] = histdf['LengthMergedKm']
    histdf['unit'] = 'km'
    histdf = histdf[['node_loc', 'technology', 'year_act', 'mode', 'time', 'value', 'unit']]
    
    trade_dict[tec]['flow']['historical_activity'] = histdf

    
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
    
# Update scenario
clone_and_update(trade_dict=trade_dict,
                 log=log,
                 to_gdx = False,
                 solve = True)
