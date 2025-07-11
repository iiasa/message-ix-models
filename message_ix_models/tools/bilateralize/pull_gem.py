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
full_path = package_data_path("bilateralize", "config.yaml")
with open(full_path, "r") as f:
    config = yaml.safe_load(f) 
message_regions = config['scenario']['regions']
full_path = package_data_path("bilateralize", message_regions + "_node_list.yaml")
with open(full_path, "r") as f:
    message_regions = yaml.safe_load(f) 
message_regions_list = [r for r in message_regions.keys() if r not in ['World', 'GLB']]

# Import files
def import_gem(input_file, input_sheet, 
               trade_technology, flow_technology):
    
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
    df['EXPORTER'] = ''; df['IMPORTER'] = ''
    for r in message_regions_list:
        df['EXPORTER'] = np.where(df['StartISO'].isin(message_regions[r]['child']), r, df['EXPORTER'])
        df['IMPORTER'] = np.where(df['EndISO'].isin(message_regions[r]['child']), r, df['IMPORTER'])
    
    # Collapse
    df['CapacityBOEd'] = df['CapacityBOEd'].replace('--', '0', regex = True).astype(float)
    df['CostUSD'] = df['CostUSD'].replace('--', '0', regex = True).astype(float)
    df['LengthMergedKm'] = df['LengthMergedKm'].replace('--', '0', regex = True).astype(float)
    
    df = df[(df['CapacityBOEd'].isnull() == False) & (df['CostUSD'].isnull() == False)]
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
    export_dir = os.path.join(os.path.dirname(export_dir), trade_technology, "GEM")
    if not os.path.isdir(export_dir):
        os.makedirs(export_dir)
    
    df.to_csv(os.path.join(export_dir, "GEM.csv"))
    
    # Investment Costs
    inv_cost = df[['EXPORTER', 'IMPORTER', 'InvCost (USD/km)']].drop_duplicates()
    inv_cost['node_loc'] = inv_cost['EXPORTER']
    inv_cost['technology'] = trade_technology + '_' + flow_technology + '_exp_' + inv_cost['IMPORTER'].str.lower().str.split('_').str[-1]
    inv_cost['value'] = inv_cost['InvCost (USD/km)']
    inv_cost = inv_cost[['node_loc', 'technology', 'value']]
    inv_cost.to_csv(os.path.join(export_dir, "inv_cost_GEM.csv"), index = False)
    
    # Historical activity
    hist_act = df[['EXPORTER', 'IMPORTER', 'LengthMergedKm']].drop_duplicates()
    hist_act['node_loc'] = hist_act['EXPORTER']
    hist_act['technology'] = trade_technology + '_' + flow_technology + '_exp_' + hist_act['IMPORTER'].str.lower().str.split('_').str[-1]
    hist_act['value'] = hist_act['LengthMergedKm']
    hist_act = hist_act[['node_loc', 'technology', 'value']]
    hist_act.to_csv(os.path.join(export_dir, "historical_activity_GEM.csv"), index = False)
    
    # Relation activity
    relation = df[['EXPORTER', 'IMPORTER', 'Capacity (GWa/km)']].drop_duplicates()
    relation['node_loc'] = relation['EXPORTER']
    relation['technology'] = trade_technology + '_exp_' + relation['IMPORTER'].str.lower().str.split('_').str[-1]
    relation['value'] = relation['Capacity (GWa/km)']
    relation = relation[['node_loc', 'technology', 'value']]
    relation['technology'] = relation['technology'].str.replace(trade_technology + '_exp_', trade_technology + '_pipe_')
    relation.to_csv(os.path.join(export_dir, "relation_GEM.csv"), index = False)
    
