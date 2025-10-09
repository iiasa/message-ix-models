# -*- coding: utf-8 -*-
"""
Calibration with MariTEAM output
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

def calibrate_mariteam(covered_tec,
                       message_regions,
                       mtdict = {'LNG_shipped': {'astd_ship_type': 'Gas tankers',
                                                 'flow_technology': ['LNG_tanker_LNG', 'LNG_tanker_loil']},
                                 'crudeoil_shipped': {'astd_ship_type': 'Crude oil tankers',
                                                      'flow_technology': ['crudeoil_tanker_loil']},
                                 'coal_shipped': {'astd_ship_type': 'Bulk carriers',
                                                  'flow_technology': ['energy_bulk_carrier_loil']},
                                 'eth_shipped': {'astd_ship_type': 'Oil product tankers',
                                                 'flow_technology': ['oil_tanker_eth', 'oil_tanker_loil']},
                                 'foil_shipped': {'astd_ship_type': 'Oil product tankers',
                                                  'flow_technology': ['oil_tanker_loil']},
                                 'loil_shipped': {'astd_ship_type': 'Oil product tankers',
                                                  'flow_technology': ['oil_tanker_loil']}},
                       mt_output = "MariTEAM_output_2025-07-21.csv",
                       project_name: str = None,
                       config_name: str = None):
    # Data paths
    config, config_path = bilateralize.load_config(project_name = project_name, 
                                      config_name = config_name)
    p_drive = config['p_drive_location']
    data_path = os.path.join(p_drive, "MESSAGE_trade")
    mt_path = os.path.join(data_path, "MariTEAM")
    out_path = os.path.join(os.path.dirname(package_data_path("bilateralize")), "bilateralize")

    # Import MariTEAM outputs
    mtdf = pd.read_csv(os.path.join(mt_path, mt_output))
    mtdf = mtdf[mtdf[message_regions + '_origin'] != mtdf[message_regions + '_destination']] # no intraregional trade
    
    for tec in [i for i in covered_tec if 'shipped' in i]:
        
        for flow_fuel in mtdict[tec]['flow_technology']:
            basedf = mtdf[mtdf['astd_ship_type'] == mtdict[tec]['astd_ship_type']].copy()
            basedf['node_loc'] = basedf[message_regions + '_origin']
            basedf['technology'] = flow_fuel
            
            # Fuel consumption (input)
            mt_input = basedf.copy()
            mt_input['mt_value'] = mt_input['intensity_MJ_tonne']/mt_input['distance_km_sum'] # MJ/t-km
            mt_input['mt_value'] = mt_input['mt_value']*3.17e-11 # GWa/t-km
            mt_input['mt_value'] = mt_input['mt_value']*1e6 # GWa/Mt-km
            mt_input['unit'] = 'GWa' # denominator assumed in output
            mt_input = mt_input.groupby(['node_loc', 'technology', 'unit'])['mt_value'].sum().reset_index()
            
            regavg = basedf.groupby(['node_loc'])[['energy_mj_sum', 'dwt', 'distance_km_sum']].sum().reset_index()
            regavg['mt_value_reg'] = regavg['energy_mj_sum']/(regavg['dwt']*regavg['distance_km_sum'])
            regavg = regavg[['node_loc', 'mt_value_reg']]
            
            inputdf = pd.read_csv(os.path.join(out_path, tec, "edit_files", "flow_technology", "input.csv"))
            inputdf = inputdf.merge(mt_input, 
                                    left_on = ['node_loc', 'technology', 'unit'],
                                    right_on = ['node_loc', 'technology', 'unit'], how = 'left')
            inputdf = inputdf.merge(regavg,
                                    left_on = ['node_loc'], right_on = ['node_loc'], how = 'left')
            
            inputdf['value'] = np.where(inputdf['mt_value'] > 0, inputdf['mt_value'], inputdf['value'])
            inputdf['value'] = np.where((inputdf['value'].isnull()) &\
                                        (inputdf['technology'].str.contains(flow_fuel)), 
                                        inputdf['mt_value_reg'], inputdf['value'])
            
            inputdf = inputdf[['node_origin', 'node_loc', 'technology', 'year_vtg', 'year_act', 'mode',
                               'commodity', 'level', 'value', 'time', 'time_origin', 'unit']]
            inputdf.to_csv(os.path.join(out_path, tec, "edit_files", "flow_technology", "input.csv"), index = False)
            
            if not os.path.isfile(os.path.join(out_path, tec, "bare_files", "flow_technology", "input.csv")):
                inputdf.to_csv(os.path.join(out_path, tec, "bare_files", "flow_technology", "input.csv"), index = False)

        # Historical activity
        histdf = pd.DataFrame()
        for flow_fuel in mtdict[tec]['flow_technology']:
            basedf = mtdf[mtdf['astd_ship_type'] == mtdict[tec]['astd_ship_type']].copy()
            basedf['node_loc'] = basedf[message_regions + '_origin']
            basedf['technology'] = flow_fuel
            basedf['value'] = basedf['distance_km_sum'] * basedf['dwt'] # t-km
            basedf['value'] = basedf['value'] / 1e6 # Mt-km
            basedf['unit'] = 'Mt-km'
            basedf['year_act'] = 2025 # last historical year
            basedf['mode'] = basedf['R12_origin'].str.replace(message_regions + '_', '') + '-' +\
                             basedf['R12_destination'].str.replace(message_regions + '_', '')
            basedf['time'] = 'year'
            basedf = basedf[['node_loc', 'technology', 'year_act', 'value', 'unit', 'mode', 'time']]
            
            if flow_fuel == 'LNG_tanker_LNG': basedf['value'] *= 0.8 # Assume 80% of historical LNG tankers are propelled using LNG
            if flow_fuel == 'LNG_tanker_foil': basedf['value'] *= 0.2 # Assume 20% of historical LNG tankers are propelled using diesel
            histdf = pd.concat([histdf, basedf])
            
        histdf.to_csv(os.path.join(out_path, tec, "edit_files", "flow_technology", "historical_activity.csv"), index = False)
        histdf.to_csv(os.path.join(out_path, tec, "bare_files", "flow_technology", "historical_activity.csv"), index = False)