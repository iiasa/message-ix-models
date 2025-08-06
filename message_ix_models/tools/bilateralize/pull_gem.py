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
import numpy as np
import pickle

from pathlib import Path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.util import package_data_path
from message_ix_models.tools.iea import web

# Data paths
data_path = os.path.join("P:", "ene.model", "MESSAGE_Trade")
gem_path = os.path.join(data_path, "Global Energy Monitor")

oil_pipeline_file = 'GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx'
oil_pipeline_sheet = 'Pipelines'

gas_pipeline_file = 'GEM-GGIT-Gas-Pipelines-2024-12.xlsx'
gas_pipeline_sheet = 'Gas Pipelines 2024-12-17'

# Set up MESSAGE regions
def gem_region(project_name = None, 
               config_name = None):
    
    config, config_path = load_config(project_name = project_name, 
                                      config_name = config_name)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) 
    message_regions = config['scenario']['regions']
    
    full_path = package_data_path("bilateralize", "node_lists", message_regions + "_node_list.yaml")
    with open(full_path, "r") as f:
        message_regions = yaml.safe_load(f) 
    message_regions_list = [r for r in message_regions.keys() if r not in ['World', 'GLB']]
    
    return message_regions_list, message_regions

# Import files
def import_gem(input_file: str, 
               input_sheet: str, 
               trade_technology: str, 
               flow_technology: str,
               flow_commodity: str,
               project_name:str = None,
               config_name: str = None):
    
    df = pd.read_excel(os.path.join(gem_path, input_file),
                       sheet_name = input_sheet)
    
    df = df[df['StopYear'].isnull()] # Only continuing projects
    
    df = df[['StartYear1', 'StartCountry', 'EndCountry',
             'CapacityBOEd', 'CostUSD', 'LengthMergedKm']].drop_duplicates()

    # Clean up country codes   
    cw = pd.read_csv(os.path.join(gem_path, 'country_crosswalk.csv'))
    for i in ['Start', 'End']:
        df = df.merge(cw, left_on = i + 'Country', right_on = 'GEM Country', how = 'left')
        df = df.rename(columns = {'ISO': i + 'ISO'})
    
    # Add MESSAGE regions
    message_regions_list, message_regions = gem_region(project_name, config_name)
    df['EXPORTER'] = ''; df['IMPORTER'] = ''
    for r in message_regions_list:
        df['EXPORTER'] = np.where(df['StartISO'].isin(message_regions[r]['child']), r, df['EXPORTER'])
        df['IMPORTER'] = np.where(df['EndISO'].isin(message_regions[r]['child']), r, df['IMPORTER'])
    
    # Collapse
    df['CapacityBOEd'] = df['CapacityBOEd'].replace('--', '0', regex = True).astype(float)
    df['CostUSD'] = df['CostUSD'].replace('--', '0', regex = True).astype(float)
    df['LengthMergedKm'] = df['LengthMergedKm'].replace('--', '0', regex = True).astype(float)
    
    df = df[(df['CapacityBOEd'].isnull() == False) & (df['CostUSD'].isnull() == False)]
    df_long = df.copy() # Keep for historical new capacity
    df = df.groupby(['EXPORTER', 'IMPORTER'])[['CapacityBOEd', 'CostUSD', 'LengthMergedKm']].sum().reset_index()
    
    # Convert units
    df['Capacity (BOEa)'] = df['CapacityBOEd']*365
    df['Capacity (TJ)'] = df['Capacity (BOEa)'] * 0.006 # BOEa to TJ
    df['Capacity (GWa)'] = df['Capacity (TJ)'] * (3.1712 * 1e-5) # TJ to GWa
    
    # Generate investment costs
    df['InvCost (USD/km)'] = (df['CostUSD'])/df['Capacity (GWa)']
    #TODO: Add industry-specific deflators
    
    # Generate capacity
    df['Capacity (GWa/km)'] = (df['Capacity (GWa)'])/df['LengthMergedKm']
    
    # Cut down
    df = df[df['EXPORTER'] != df['IMPORTER']]
    df = df[(df['EXPORTER']!= "") & (df['IMPORTER'] != "")]
    
    df['trade_technology'] = trade_technology
    df['flow_technology'] = flow_technology
    
    # Output to trade_technology
    export_dir = package_data_path("bilateralize", trade_technology)
    trade_dir = os.path.join(os.path.dirname(export_dir), trade_technology, "edit_files")
    flow_dir = os.path.join(os.path.dirname(export_dir), trade_technology, "edit_files", "flow_technology")
    export_dir = os.path.join(os.path.dirname(export_dir), trade_technology, "GEM")
    if not os.path.isdir(export_dir):
        os.makedirs(export_dir)
    
    df.to_csv(os.path.join(export_dir, "GEM.csv"))
    
    # Investment Costs
    inv_cost = df[['EXPORTER', 'IMPORTER', 'InvCost (USD/km)']].drop_duplicates()
    inv_cost['node_loc'] = inv_cost['EXPORTER']
    inv_cost['technology'] = flow_technology + '_' + inv_cost['IMPORTER'].str.lower().str.split('_').str[-1]
    inv_cost['value_update'] = inv_cost['InvCost (USD/km)']/1e6 # in MUSD/km
    inv_cost = inv_cost[['node_loc', 'technology', 'value_update']]
    inv_cost.to_csv(os.path.join(export_dir, "inv_cost_GEM.csv"), index = False)
    
    basedf = pd.read_csv(os.path.join(flow_dir, "inv_cost.csv"))
    basedf['value'] = 100
    inv_cost = basedf.merge(inv_cost,
                            left_on = ['node_loc', 'technology'],
                            right_on = ['node_loc', 'technology'],
                            how = 'left')
    inv_cost['value'] = np.where(inv_cost['value_update'] > 0, inv_cost['value_update'], inv_cost['value'])
    inv_cost['year_vtg'] = 'broadcast'
    inv_cost['unit'] = 'USD/km'
    inv_cost = inv_cost[['node_loc', 'technology', 'year_vtg', 'value', 'unit']]
    inv_cost.to_csv(os.path.join(flow_dir, "inv_cost.csv"), index = False)
    
    # Historical activity
    hist_act = df[['EXPORTER', 'IMPORTER', 'LengthMergedKm']].drop_duplicates()
    hist_act['node_loc'] = hist_act['EXPORTER']
    hist_act['technology'] = flow_technology + '_' + hist_act['IMPORTER'].str.lower().str.split('_').str[-1]
    hist_act['value'] = round(hist_act['LengthMergedKm'],0)
    hist_act = hist_act[['node_loc', 'technology', 'value']]
    hist_act['year_act'] = 2025
    hist_act['unit'] = 'km'
    hist_act['mode'] = 'M1'
    hist_act['time'] = 'year'
    hist_act = hist_act[['node_loc', 'technology', 'year_act', 'value', 'unit', 'mode', 'time']]
    hist_act.to_csv(os.path.join(export_dir, "historical_activity_GEM.csv"), index = False)
    hist_act.to_csv(os.path.join(flow_dir, "historical_activity.csv"), index = False)

    # Historical new capacity
    hist_cap = df_long[['EXPORTER', 'IMPORTER', 'Len']]
    # Input
    inputdf = df[['EXPORTER', 'IMPORTER', 'Capacity (GWa/km)']].drop_duplicates()
    inputdf['node_loc'] = inputdf['EXPORTER']
    inputdf['technology'] = trade_technology + '_exp_' + inputdf['IMPORTER'].str.lower().str.split('_').str[-1]
    inputdf['value_update'] = round((1/inputdf['Capacity (GWa/km)']),0)
    inputdf['commodity'] = flow_commodity
    inputdf = inputdf[['node_loc', 'technology', 'value_update', 'commodity']] # km/GWa
    
    basedf = pd.read_csv(os.path.join(trade_dir, "input.csv"))
    basedf['value'] = np.where(basedf['commodity'] == flow_commodity,
                               30, # The largest capacity pipelines have maximum 300,000GWh (~30bcm) annually
                               basedf['value'])
    
    inputdf = basedf.merge(inputdf,
                           left_on = ['node_loc', 'technology', 'commodity'],
                           right_on = ['node_loc', 'technology', 'commodity'],
                           how = 'left')
    inputdf['value'] = np.where((inputdf['value_update'].isnull() == False)&\
                                (inputdf['value_update']<10000) &\
                                (inputdf['commodity'] == flow_commodity),
                                inputdf['value_update'],
                                inputdf['value'])
    inputdf = inputdf.drop(['value_update'], axis = 1)
    
    inputdf.to_csv(os.path.join(export_dir, "inputs_GEM.csv"), index = False)
    inputdf.to_csv(os.path.join(trade_dir, "input.csv"), index = False)

