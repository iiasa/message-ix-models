# -*- coding: utf-8 -*-
"""
Liquefaction calibration
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
import numpy as np
import pickle

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.iea import web
from message_ix_models.tools.bilateralize import bilateralize

def update_liquefaction_input(message_regions: str = "R12", 
                              project_name: str = None,
                              config_name: str = None):
    
    # Pull in configuration
    config, config_path = bilateralize.load_config(project_name = project_name, 
                                                   config_name = config_name)
    
    # Pull in regionalization
    dict_dir = package_data_path("bilateralize", "node_lists", message_regions + "_node_list.yaml")
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f) 
    region_list = [i for i in list(dict_message_regions.keys()) if i != 'World']
    
    # Pull in dataset
    datadir = os.path.dirname(package_data_path("bilateralize", "LNG Liquefaction.xlsx"))
    liqdf = pd.read_excel(os.path.join(datadir, "LNG Liquefaction.xlsx"),
                          sheet_name = 'Fuel Losses')
    
    # Collapse to regionalization
    liqdf['node_loc'] = ''
    for r in region_list:
        liqdf['node_loc'] = np.where(liqdf['ISO'].isin(dict_message_regions[r]['child']),
                                     r, liqdf['node_loc'])
    liqdf = liqdf.groupby(['node_loc'])['Use Value'].mean().reset_index()
    liqvalue = round(liqdf['Use Value'].mean(), 2)
    
    # Set up dataframe for all regions
    outdf = pd.DataFrame.from_dict(dict(node_loc = region_list))
    outdf = outdf.merge(liqdf, left_on = ['node_loc'], right_on = ['node_loc'], how = 'left')
    outdf['Use Value'] = np.where(outdf['Use Value'].isnull(), liqvalue, outdf['Use Value'])
    outdf = outdf.rename(columns = {'Use Value': 'value'})
    outdf['value'] = round(outdf['value'], 2)
    outdf['technolgoy'] = 'LNG_prod'
    outdf['commodity'] = 'gas'
    outdf['level'] = 'primary'
    
    return dict(input = outdf)