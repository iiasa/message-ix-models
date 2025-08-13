# -*- coding: utf-8 -*-
"""
Background: Historical trade data
@junukitashepard
"""
# Import packages
import os
import sys
import pandas as pd
import logging

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *
from message_ix_models.tools.bilateralize.pull_gem import *

# Globals
message_regions = 'R12'
project_name = 'newpathways'
config_name = 'config.yaml'

# Configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')
data_path = os.path.dirname(config_path)

# Import dataframes
iea_gas = import_iea_gas(project_name = project_name,
                         config_name = config_name)

# Add MESSAGE regions
dict_dir = package_data_path("bilateralize", "node_lists", message_regions + '_node_list.yaml')
with open(dict_dir, "r") as f:
    dict_message_regions = yaml.safe_load(f) 
region_list = [i for i in list(dict_message_regions.keys()) if i != 'World']

iea_gas['EXPORTER REGION'] = ''; iea_gas['IMPORTER REGION'] = ''
for t in ['EXPORTER', 'IMPORTER']:
    for r in region_list:
        iea_gas[t + ' REGION'] = np.where(iea_gas[t].isin(dict_message_regions[r]['child']),
                                          r, iea_gas[t + ' REGION'])

# Set up for PowerBI
country_lines = iea_gas[iea_gas['ENERGY (TJ)'] > 0]
country_lines['PAIR'] = country_lines['EXPORTER'] + '-' + country_lines['IMPORTER']
country_lines['REGION PAIR'] = country_lines['EXPORTER REGION'] + '-' + country_lines['IMPORTER REGION']

country_lines.to_csv(os.path.join(data_path, 'diagnostics', 'historical_gas_bycountry.csv'), index = False)