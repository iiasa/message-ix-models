# -*- coding: utf-8 -*-
"""
Move data from bare files to a dictionary compatible with updating a MESSAGEix scenario

This script is the second step in implementing the bilateralize tool.
It moves data from /data/bilateralize/[your_trade_commodity]/bare_files/ to a dictionary compatible with updating a MESSAGEix scenario.
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

def bare_to_scenario(project_name: str | None = None, 
                     config_name: str | None = None,
                     scenario_parameter_name: str = "scenario_parameters.pkl"):
    """
    Move data from bare files to a dictionary compatible with updating a MESSAGEix scenario
    
    Args:
        project_name: Name of the project (e.g., 'newpathways')
        config_name: Name of the config file (e.g., 'config.yaml')
        scenario_parameter_name: Name of the scenario parameter file (default is'scenario_parameters.pkl')
    
    Output:
        trade_dict: Dictionary compatible with updating a MESSAGEix scenario
    """
    # Bring in configuration
    config, config_path, tec_config = load_config(project_name = project_name, 
                                                  config_name = config_name,
                                                  load_tec_config = True)

    covered_tec = config['covered_trade_technologies']
    message_regions = config['scenario']['regions']

    # Get logger
    log = get_logger(__name__)

    # Read and inflate sheets based on model horizon
    trade_dict = build_parameter_sheets(log=log, project_name = project_name, config_name = config_name)

    # Historical calibration for trade technology
    histdf = build_historical_activity(message_regions = message_regions, 
                                        project_name = project_name, config_name = config_name,
                                        reimport_BACI = False)
    histdf = histdf[histdf['year_act'].isin([2000, 2005, 2010, 2015, 2020, 2023])]
    histdf['year_act'] = np.where((histdf['year_act'] == 2023), 2025, histdf['year_act']) # TODO: Assume 2023 values FOR NOW 
    histdf = histdf[histdf['value'] > 0]
    histdf['technology'] = histdf['technology'].str.replace('ethanol_', 'eth_')
    histdf['technology'] = histdf['technology'].str.replace('fueloil_', 'foil_')

    histnc = build_historical_new_capacity_trade(message_regions,
                                                 project_name = project_name, config_name = config_name)

    hist_tec = {}
    for tec in [c for c in covered_tec if c not in ['crudeoil_piped', 'foil_piped', 'loil_piped']]:
        add_tec = tec_config[tec][tec + '_trade']['trade_technology'] + '_exp'
        hist_tec[tec] = add_tec

    for tec in hist_tec.keys():
        log.info('Add historical activity for ' + tec)
        add_df = histdf[histdf['technology'].str.contains(hist_tec[tec])]
        trade_dict[tec]['trade']['historical_activity'] = add_df
        
        log.info('Add historical new capacity for ' + tec)
        add_df = histnc[histnc['technology'].str.contains(hist_tec[tec])]
        trade_dict[tec]['trade']['historical_new_capacity'] = add_df

    # Historical new capacity for maritime shipping
    shipping_fuel_dict = config['shipping_fuels']
    # TODO: Add coal
    hist_crude_loil = build_historical_new_capacity_flow('Crude Tankers.csv', 'crudeoil_tanker_loil',
                                                         project_name = project_name, config_name = config_name)
    hist_lh2_loil = build_historical_new_capacity_flow('LH2 Tankers.csv', 'lh2_tanker_loil',
                                                        project_name = project_name, config_name = config_name)    
    hist_lng = pd.DataFrame()
    for f in ['loil', 'LNG']:
        hist_lng_f = build_historical_new_capacity_flow('LNG Tankers.csv', 'LNG_tanker_'+f,
                                                        project_name = project_name, config_name = config_name)
        hist_lng_f['value'] *= shipping_fuel_dict['LNG_tanker']['LNG_tanker_' + f]
        hist_lng = pd.concat([hist_lng, hist_lng_f])

    hist_oil = pd.DataFrame()
    for f in ['loil', 'foil', 'eth']:
        hist_oil_f = build_historical_new_capacity_flow('Oil Tankers.csv', 'oil_tanker_'+f,
                                                        project_name = project_name, config_name = config_name)
        hist_oil_f['value'] *= shipping_fuel_dict['oil_tanker']['oil_tanker_' + f]
        hist_oil = pd.concat([hist_oil, hist_oil_f])
                
    trade_dict['crudeoil_shipped']['flow']['historical_new_capacity'] = hist_crude_loil
    trade_dict['lh2_shipped']['flow']['historical_new_capacity'] = hist_lh2_loil
    trade_dict['LNG_shipped']['flow']['historical_new_capacity'] = hist_lng
    trade_dict['eth_shipped']['flow']['historical_new_capacity'] = hist_oil[hist_oil['technology'] != 'oil_tanker_foil']
    trade_dict['foil_shipped']['flow']['historical_new_capacity'] = hist_oil
    trade_dict['loil_shipped']['flow']['historical_new_capacity'] = hist_oil

    # Historical activity should only be added for technologies in input
    for tec in covered_tec:
        if 'historical_activity' in trade_dict[tec]['trade'].keys():
            trade_dict[tec]['trade']['historical_activity'] = trade_dict[tec]['trade']['historical_activity'][trade_dict[tec]['trade']['historical_activity']['technology'].isin(trade_dict[tec]['trade']['input']['technology'])]
        if 'historical_new_capacity' in trade_dict[tec]['trade'].keys():
            trade_dict[tec]['trade']['historical_new_capacity'] = trade_dict[tec]['trade']['historical_new_capacity'][trade_dict[tec]['trade']['historical_new_capacity']['technology'].isin(trade_dict[tec]['trade']['input']['technology'])]

    # Ensure flow technologies are only added once
    covered_flow_tec = []
    for tec in covered_tec:
        flow_tecs = list(trade_dict[tec]['flow']['input']['technology'].unique())
        for par in trade_dict[tec]['flow'].keys():
            trade_dict[tec]['flow'][par] = trade_dict[tec]['flow'][par]\
                [trade_dict[tec]['flow'][par]['technology'].isin(covered_flow_tec) == False]
        covered_flow_tec = covered_flow_tec + flow_tecs

    # Save trade_dictionary
    tdf = os.path.join(os.path.dirname(config_path), scenario_parameter_name)
    with open(tdf, 'wb') as f: pickle.dump(trade_dict, f)