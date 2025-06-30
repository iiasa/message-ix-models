# -*- coding: utf-8 -*-
"""
Historical Calibration
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

# Dictionaries of ISO - IEA - MESSAGE Regions
def generate_dicts(message_regions):
    
    message_regions = 'R12'
    
    dict_dir = package_data_path("bilateralize", 'iea_message_node_mapping.yaml')
    with open(dict_dir, "r") as f:
        dict_iea_to_message = yaml.safe_load(f) 
    dict_iea_to_message = dict_iea_to_message['IEA_to_message']
    
    dict_dir = package_data_path("bilateralize", message_regions + '_node_list.yaml')
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f) 
    
    full_dict = dict()
    for region in dict_message_regions.keys():
        if "child" in dict_message_regions[region].keys():
            for iso in dict_message_regions[region]["child"]:
                full_dict[iso] = {message_regions: region,
                                  "IEA CF Region": None}
    for ieareg in dict_iea_to_message.keys():
        if dict_iea_to_message[ieareg]["ISO"] != None:
            full_dict[dict_iea_to_message[ieareg]["ISO"]]['IEA CF Region'] = ieareg
    
    # Import conversion factors
    cfdf =  pd.read_excel(os.path.join(os.path.dirname(dict_dir), 'historical_calibration', 'conversion_factors.xlsx'),
                          skiprows=2)
    
    # Regional aggregation of conversion factors
    
# Generate conversion factors for all ISO codes
# Import UN Comtrade data and link to conversion factors
# Aggregate UN Comtrade data to MESSAGE Regions
