# -*- coding: utf-8 -*-
"""
Prepare edit files for bilateralize tool

This script is the first step in implementing the bilateralize tool.
It generates empty (or default valued) parameters that are required for bilateralization, specified by commodity.
This step is optional in a workflow; users can move directly to the next step (2_bare_to_scenario).
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
from message_ix_models.tools.bilateralize.calculate_distance import *

def prepare_edit_files(project_name: str | None = None, 
                        config_name: str | None = None):
    """
    Prepare edit files for bilateralize tool
    
    Args:
        project_name: Name of the project (e.g., 'newpathways')
        config_name: Name of the config file (e.g., 'config.yaml')
    """
    
    # Bring in configuration
    config, config_path = load_config(project_name = project_name, config_name = config_name)

    covered_tec = config['covered_trade_technologies']
    message_regions = config['scenario']['regions']

    data_path = package_data_path("bilateralize")
    data_path = os.path.join(os.path.dirname(data_path), "bilateralize")

    # Connect to ixmp
    mp = ixmp.Platform()

    # Get logger
    log = get_logger(__name__)

    # Calculate distances
    calculate_distance(message_regions)

    # Generate bare sheets
    generate_bare_sheets(log=log)

    # Import calibration files from Global Energy Monitor
    import_gem(input_file = 'GEM-GGIT-Gas-Pipelines-2024-12.xlsx', 
               input_sheet = 'Gas Pipelines 2024-12-17', 
               trade_technology = "gas_piped",
               flow_technology = "gas_pipe",
               flow_commodity = "gas_pipeline_capacity")

    for tradetec in ['crudeoil_piped', 'foil_piped', 'loil_piped']:
        import_gem(input_file = 'GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx', 
                   input_sheet = 'Pipelines', 
                   trade_technology = tradetec,
                   flow_technology = "oil_pipe",
                   flow_commodity = "oil_pipeline_capacity")

    # Add MariTEAM calibration for maritime shipping
    calibrate_mariteam(covered_tec, message_regions)

    # Add variable costs for shipped commodities
    costdf = build_historical_price(message_regions)
    costdf['technology'] = costdf['technology'].str.replace("ethanol_", "eth_")
    costdf['technology'] = costdf['technology'].str.replace("fueloil_", "foil_")

    for tec in [i for i in covered_tec if i != "gas_piped"]:
        log.info('Add variable cost for ' + tec)
        
        if 'piped' in tec:
            tec_shipped = tec.replace('piped', 'shipped')
            add_df = costdf[costdf['technology'].str.contains(tec_shipped)].copy()
            add_df['technology'] = add_df['technology'].str.replace('shipped', 'piped')
        else:
            add_df = costdf[costdf['technology'].str.contains(tec)]
            
        col_list = add_df.columns
        mean_cost = add_df['value'].mean()
        
        input_df = pd.read_csv(os.path.join(data_path, tec, "edit_files", "input.csv"))[['node_loc', 'technology', 'year_act', 'year_vtg', 'mode', 'time']].drop_duplicates()
        add_df = input_df.merge(add_df, left_on = ['node_loc', 'technology', 'year_act', 'year_vtg',  'mode', 'time'], 
                                right_on = ['node_loc', 'technology', 'year_act', 'year_vtg',  'mode', 'time'], how = 'left')
        
        add_df['value'] = np.where(add_df['value'].isnull(), mean_cost, add_df['value'])
        add_df['value'] = round(add_df['value']/5,0)
        
        add_df['unit'] = 'USD/GWa'
        
        add_df = add_df[col_list]    
        
        add_df = add_df[add_df['technology'].str.contains('_imp') == False] # No costs applied to import technologies
        
        add_df.to_csv(os.path.join(data_path, tec, "edit_files", "var_cost.csv"),
                    index = False)
        add_df.to_csv(os.path.join(data_path, tec, "bare_files", "var_cost.csv"),
                    index = False)



