# -*- coding: utf-8 -*-
"""
Resource Supply Curves (RSC) from a scenario
"""

# -*- coding: utf-8 -*-
"""
Diagnostics on bilateralization
"""
# Import packages
import os
import sys
import pandas as pd
import numpy as np
import logging
import yaml
import message_ix
import ixmp
import itertools
import plotly.graph_objects as go

from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.util import package_data_path
from ixmp import Platform

# Connect to ixmp
mp = ixmp.Platform()

scenario_name = 'baseline'
model_name = 'NP_SSP2_6.2'
gas_commodities = {1: 'Identified Reserves',
                   2: 'Undiscovered Natural Gas (Mode)',
                   3: 'UND (Diff between Mode and 5%)',
                   4: 'Estimated enhanced recovery',
                   5: 'Non-conventional reserves (20% CBM, 15% FS, 15% TF)',
                   6: 'Non-conventional reserves (80% CBM, 60%)',
                   7: 'Non-conventional reserves (80% CBM, 40%)'}

# Connect to scenario
scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)

# Bring in configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')
data_path = os.path.dirname(config_path)

# Pull resource
res_vol = scen.par('resource_volume', 
                   filters = {'commodity': ['gas_' + str(i) for i in list(gas_commodities.keys())]})
res_vol['value_TJ'] = res_vol['value'] / (3.1712 * 1e-5) # GWa to TJ
res_vol['value_EJ'] = res_vol['value_TJ'] /1e6

res_vol['resource_reserve'] = 'Additional Resources'
res_vol['resource_reserve'] = np.where(res_vol['commodity'].isin(['gas_1', 'gas_5', 'gas_6', 'gas_7']), 
                                       'Identified Reserve', res_vol['resource_reserve'])

res_vol['gas_type'] = ''
for i in gas_commodities.keys():
    res_vol['gas_type'] = np.where(res_vol['commodity'] == 'gas_' + str(i),
                                   gas_commodities[i],
                                   res_vol['gas_type'])

res_vol = res_vol[res_vol['node'] == 'R12_NAM']
res_vol = res_vol[['node', 'commodity', 'gas_type', 'resource_reserve', 'value_EJ']]
res_vol['source'] = 'SSP2_v6.2'

# Bring in GEA (2017) and Rogner (1996)
other_res_vol = pd.read_csv(os.path.join(data_path, 'gas_resources.csv'))

res_vol = res_vol.merge(other_res_vol, 
                        left_on = ['commodity', 'resource_reserve', 'source', 'node', 'value_EJ'],
                        right_on = ['commodity', 'resource_reserve', 'source', 'node', 'value_EJ'],
                        how = 'outer')

res_vol.to_csv(os.path.join(data_path, 'gas_resources_comparison.csv'))

# Pull costs
inv_cost = scen.par('inv_cost',
                    filters = {'technology': ['gas_extr_' + str(i) for i in list(gas_commodities.keys())]})
var_cost = scen.par('var_cost',
                    filters = {'technology': ['gas_extr_' + str(i) for i in list(gas_commodities.keys())]})

