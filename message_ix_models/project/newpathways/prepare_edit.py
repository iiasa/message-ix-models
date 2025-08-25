# -*- coding: utf-8 -*-
"""
Bilateralize trade flows
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

data_path = package_data_path("bilateralize")
data_path = os.path.join(os.path.dirname(data_path), "bilateralize")

# Connect to ixmp
mp = ixmp.Platform()

# Get logger
log = get_logger(__name__)

# Generate bare sheets
generate_bare_sheets(log=log, 
                     project_name = 'newpathways', 
                     config_name = 'config.yaml',
                     message_regions = message_regions)

# Import calibration files from Global Energy Monitor
import_gem(input_file = gas_pipeline_file, 
           input_sheet = gas_pipeline_sheet, 
           trade_technology = "gas_piped",
           flow_technology = "gas_pipe",
           flow_commodity = "gas_pipeline_capacity",
           project_name = 'newpathways', config_name = 'config.yaml')

# Add MariTEAM calibration for maritime shipping
calibrate_mariteam(covered_tec, message_regions,
                   project_name = 'newpathways', config_name = 'config.yaml')

# Add variable costs for shipped commodities
costdf = build_historical_price(message_regions,
                                project_name = 'newpathways', config_name = 'config.yaml')

for tec in [i for i in covered_tec if 'shipped' in i]:
    log.info('Add variable cost for ' + tec)
    add_df = costdf[costdf['technology'].str.contains(tec)]
    col_list = add_df.columns
    min_cost = add_df['value'].min()
    
    input_df = pd.read_csv(os.path.join(data_path, tec, "edit_files", "input.csv"))[['node_loc', 'technology', 'year_act', 'year_vtg', 'mode', 'time']].drop_duplicates()
    add_df = input_df.merge(add_df, left_on = ['node_loc', 'technology', 'year_act', 'year_vtg',  'mode', 'time'], 
                            right_on = ['node_loc', 'technology', 'year_act', 'year_vtg',  'mode', 'time'], how = 'left')
    
    add_df['value'] = np.where(add_df['value'].isnull(), min_cost, add_df['value'])
    add_df['value'] = add_df['value']/5
    
    add_df['unit'] = 'USD/GWa'
    
    add_df = add_df[col_list]    
    
    add_df.to_csv(os.path.join(data_path, tec, "edit_files", "var_cost.csv"),
                  index = False)

