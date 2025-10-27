# -*- coding: utf-8 -*-
"""
Bilateralize trade flows
"""
# Import packages
import ixmp
import itertools
import logging
import message_ix
import numpy as np
import os
import pandas as pd
import shutil
import sys
import yaml

from pathlib import Path
from message_ix_models.util import package_data_path

#%% Get logger
def get_logger(name: str):

    # Set the logging level to INFO (will show INFO and above messages)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Define the format of log messages:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

    # Apply the format to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    log.addHandler(handler)

    return log

#%% Load config yaml
def load_config(project_name:str = None,
                config_name:str = None,
                load_tec_config:bool = False):
    """
    Load config file and optional trade-specific config files.
    
    Args:
        project_name: Name of the project (message_ix_models/project/[THIS])
        config_name: Name of the base config file (e.g., config.yaml)
        load_tec_config: If True, load the trade-specific config files (default is False)
        
    Returns:
        config: Config dictionary (base config)
        config_path: Path to the config file
        tec_config_dict: Dictionary of trade-specific config files (if load_tec_config is True)
    """
    # Load config
    if (project_name is None) & (config_name is None):
        config_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path("bilateralize")), 
                                                   "bilateralize", "configs", "base_config.yaml"))
    if project_name is not None:
        if config_name is None: config_name = "config.yaml"
        config_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path(project_name)), 
                                                   os.path.pardir, "project", project_name, config_name))
        
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) # safe_load is recommended over load for security
    
    if load_tec_config == False:
        return config, config_path
    elif load_tec_config == True:
        tec_config_dict = dict()
        for tec in config['covered_trade_technologies']:
            tec_config_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path("bilateralize")), 
                                         "bilateralize", "configs", tec + ".yaml"))
            with open(tec_config_path, "r") as f:
                tec_config = yaml.safe_load(f)
            tec_config_dict[tec] = tec_config
        return config, config_path, tec_config_dict

#%% Copy columns from template, if exists
def copy_template_columns(df, 
                          template, 
                          exclude_cols=["node_loc", "technology"]):
    """
    Copy columns from template to dataframe.
    
    Args:
        df: Dataframe to copy columns to
        template: Template dataframe
        exclude_cols: Columns to exclude from copying
    """
    for col in template.columns:
        if col not in exclude_cols:
            df[col] = template[col].iloc[0]
            
#%% Broadcast years to create vintage-activity year pairs.
def broadcast_yv_ya(df: pd.DataFrame, 
                    ya_list: list[int],
                    yv_list: list[int],
                    tec_lifetime: pd.DataFrame):
    """
    Broadcast years to create vintage-activity year pairs.

    Args:
        df: Input parameter DataFrame
        ya_list: List of activity years to consider
        yv_list: List of vintage years to consider
        tec_lifetime: Technical lifetime of the technology, provided via dataframe
    Returns:
        pd.DataFrame: DataFrame with expanded rows for each vintage-activity year pair
    """
    all_new_rows = []
    
    tecltdf = tec_lifetime.copy()
    tecltdf['teclt'] = tecltdf['value']
    
    lts = df.merge(tecltdf[['node_loc', 'technology', 'teclt']].drop_duplicates(),
                   left_on = ['node_loc', 'technology'],
                   right_on = ['node_loc', 'technology'],
                   how = 'left')
    
    # Process each row in the original DataFrame
    for _, row in lts.iterrows():
        
        teclt_row = row['teclt']

        # For each activity year
        for ya in ya_list:
            # Get all vintage years that are <= activity year for a period < technical lifetime
            yv_list = [yv for yv in ya_list if yv <= ya]
            yv_list = [yv for yv in yv_list if yv >= ya-teclt_row]
            
            # Create new rows for each vintage year
            for yv in yv_list:
                new_row = row.copy()
                new_row["year_act"] = int(ya)
                new_row["year_vtg"] = int(yv)
                all_new_rows.append(new_row)
                
    # Combine original DataFrame with new rows
    result_df = pd.DataFrame(all_new_rows).drop(['teclt'], axis = 1)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df

#%% Broadcast vintage years
def broadcast_years(df: pd.DataFrame, 
                    year_type: str,
                    year_list: list[int]) -> pd.DataFrame:
    """
    Broadcast vintage, relation, or activity years.
    
    Args:
        df: Input parameter DataFrame
        year_type: Type of year to broadcast (e.g., 'year_vtg', 'year_rel', 'year_act')
        year_list: List of years to broadcast
    Returns:
        pd.DataFrame: DataFrame with expanded rows for each year
    """
    all_new_rows = []
    for _, row in df.iterrows():
        for y in year_list:
            new_row = row.copy()
            new_row[year_type] = int(y)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df[year_type] != "broadcast"]
    return result_df.drop_duplicates()

#%% Write just the GDX files
def save_to_gdx(mp: ixmp.Platform,
                scenario: str, 
                output_path: str):
    """
    Save the scenario to a GDX file.
    
    Args:
        mp: ixmp platform
        scenario: Scenario name
        output_path: Path to save the GDX file
    """
    from ixmp.backend import ItemType
    mp._backend.write_file(output_path,
                           ItemType.SET | ItemType.PAR,
                           filters={"scenario": scenario},
                           )
#%% Build base parameter dataframe based on network dataframe
def build_parameterdf(
        par_name: str,
        network_df: pd.DataFrame,
        col_values: dict,
        common_years: dict = dict(year_vtg = 'broadcast',
                                  year_rel = 'broadcast',
                                  year_act = 'broadcast'),
        common_cols: dict = dict(mode= 'M1',
                                 time= 'year',
                                 time_origin= 'year',
                                 time_dest= 'year'),
        export_only: bool = False):

    """
    Build parameter dataframes based on the specified network dataframe.
    
    Args:
        par_name: Parameter name (e.g., capacity_factor)
        network_df: Specified network dataframe
        col_values: Values for other columns to populate as default
        export_only: If True, only produces dataframe for export technology
    """
    
    df_export = message_ix.make_df(par_name,
                                   node_loc = network_df['exporter'],
                                   technology = network_df['export_technology'],
                                   **col_values, **common_years, **common_cols)
    df = df_export.copy()
    
    if export_only == False:
        df_import = message_ix.make_df(par_name,
                                       node_loc = network_df['importer'],
                                       technology = network_df['import_technology'],
                                       **col_values, **common_years, **common_cols)
        df = pd.concat([df, df_import])
    
    return df
        
#%% Main function to generate bare sheets
def generate_bare_sheets(
        log,
        project_name: str = None,
        config_name: str = None):
    """
    Generate bare sheets to collect required parameters

    Args:
        log (log, required): Log file to track progress
        project_name (str, optional): Project name (message_ix_models/project/[THIS]) 
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config_default.yaml
        message_regions (str, optional): Default is R12 regionality
    """
    data_path = package_data_path("bilateralize")
    data_path = os.path.join(os.path.dirname(data_path), "bilateralize")
    
    # Load config
    log.info(f"Loading config file")
    config_base, config_path, config_tec = load_config(project_name, config_name, load_tec_config=True)

    # Retrieve config sections    
    message_regions = config_base.get('scenario', {}).get('regions')
    covered_tec = config_base.get('covered_trade_technologies', {})

    config_dict = {}    
    for tec in covered_tec:
        tec_dict = config_tec.get(tec).get(tec + '_trade', {})
        for k in tec_dict.keys():
            if k not in config_dict.keys(): config_dict[k] = {}
            config_dict[k][tec] = tec_dict[k]
                 
    # Generate folder for each trade technology
    for tec in covered_tec:
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)
        if not os.path.isdir(tecpath):
            os.makedirs(tecpath)
        pathlist = [os.path.join(tecpath, 'edit_files'),
                    os.path.join(tecpath, 'edit_files', 'flow_technology'),
                    os.path.join(tecpath, 'bare_files'),
                    os.path.join(tecpath, 'bare_files', 'flow_technology')]
        for f in pathlist:
            if not os.path.isdir(f): 
                os.makedirs(f)
            
    # Generate full combination of nodes to build technology-specific network
    node_path = package_data_path("bilateralize", "node_lists", message_regions + "_node_list.yaml")
    with open(node_path, "r") as f:
        node_set = yaml.safe_load(f) 
    node_set = [r for r in node_set.keys() if r not in ['World', 'GLB']]
    
    node_df = pd.DataFrame(itertools.product(node_set, node_set))
    node_df.columns = ['exporter', 'importer']
    node_df = node_df[node_df['exporter'] != node_df['importer']]
    
    network_setup = {} # Dictionary for each covered technology
    for tec in covered_tec:

        node_df_tec = node_df.copy()
        
        node_df_tec['export_technology'] = config_dict['trade_technology'][tec] + '_exp'
        if config_dict['trade_tech_suffix'][tec] != None: 
            node_df_tec['export_technology'] = node_df_tec['export_technology'] + '_' + config_dict['trade_tech_suffix'][tec]
        
        node_df_tec['export_technology'] = node_df_tec['export_technology'] + '_' +\
                                           node_df_tec['importer'].str.lower().str.split('_').str[1]
        
        # If there are multiple trade "routes" (not flow, but decision) per technology
        if (config_dict['trade_tech_number'][tec] != None) & (config_dict['trade_tech_number'][tec] != 1): 
            ndt_out = pd.DataFrame()
            for i in list(range(1, config_dict['trade_tech_number'][tec] + 1)):
                ndt = node_df_tec.copy()
                ndt['export_technology'] = ndt['export_technology'] + '_' + str(i)
                ndt_out = pd.concat([ndt_out, ndt])
            node_df_tec = ndt_out.copy()
            
        node_df_tec['import_technology'] = config_dict['trade_technology'][tec] + '_imp'
        node_df_tec['INCLUDE? (No=0, Yes=1)'] = ''
        
        # Specify network (the function will stop to allow specification)
        if config_dict['specify_network'][tec] == True:
            try:
                specify_network_tec = pd.read_csv(
                    os.path.join(data_path, tec, "specify_network_" + tec + ".csv"))
            except FileNotFoundError:
                node_df_tec.to_csv(os.path.join(data_path, tec, "specify_network_" + tec + ".csv"), 
                                   index=False)
                raise Exception(
                    "The function stopped. Sheet specify_network_" + tec + ".csv has been generated. " +
                    "Fill in the specific pairs first and run again.")
            if any(specify_network_tec['INCLUDE? (No=0, Yes=1)'].notnull()) == False:
                raise Exception(
                    "The function stopped. Ensure that all values under 'INCLUDE? (No=0, Yes=1)' are filled")                
        elif config_dict['specify_network'][tec] == False:
            specify_network_tec = node_df_tec.copy()
            specify_network_tec['INCLUDE? (No=0, Yes=1)'] = 1
        else: 
            raise Exception("Please use True or False.")
        
        network_setup[tec] = specify_network_tec[specify_network_tec['INCLUDE? (No=0, Yes=1)'] == 1]

    # Common values across parameters
    common_years = dict(year_vtg= 'broadcast',
                        year_act= 'broadcast',
                        year_rel= 'broadcast')
    common_cols = dict(mode= 'M1',
                       time= 'year',
                       time_origin= 'year',
                       time_dest = 'year')
    
    parameter_outputs = dict() # This will get populated
    for tec in covered_tec:
        parameter_outputs[tec] = dict(trade = dict(),
                                      flow = dict())
    
    # Create bare file: input
    for tec in covered_tec:  
        
        # Trade Level (supply to piped/shipped)
        df_input_trade = message_ix.make_df('input',
                                            node_origin = network_setup[tec]['exporter'],
                                            node_loc = network_setup[tec]['exporter'],
                                            technology = network_setup[tec]['export_technology'],
                                            commodity = config_dict['trade_commodity'][tec],
                                            level = config_dict['export_level'][tec],
                                            value = 1,
                                            unit = config_dict['trade_units'][tec],
                                            **common_years, **common_cols)
                                 
        # Import Level (piped/shipped to import)
        df_input_import = message_ix.make_df('input',
                                             node_origin = network_setup[tec]['importer'],
                                             node_loc = network_setup[tec]['importer'],
                                             technology = network_setup[tec]['import_technology'],
                                             commodity = config_dict['trade_commodity'][tec],
                                             level = config_dict['trade_level'][tec],
                                             value = 1,
                                             unit = config_dict['trade_units'][tec],
                                             **common_years, **common_cols)
        
        df_input = pd.concat([df_input_trade, df_input_import]).drop_duplicates()
        
        parameter_outputs[tec]['trade']['input'] = df_input

    # Create base file: output
    for tec in covered_tec:  
        
        # Trade Level
        df_output_trade = message_ix.make_df('output',
                                             node_loc = network_setup[tec]['exporter'],
                                             node_dest = network_setup[tec]['importer'],
                                             technology = network_setup[tec]['export_technology'],
                                             commodity = config_dict['trade_commodity'][tec],
                                             level = config_dict['trade_level'][tec],
                                             value = 1,
                                             unit = config_dict['trade_units'][tec],
                                             **common_years, **common_cols)
        
        # Import Level
        df_output_import = message_ix.make_df('output',
                                              node_loc = network_setup[tec]['importer'],
                                              node_dest = network_setup[tec]['importer'],
                                              technology = network_setup[tec]['import_technology'],
                                              commodity = config_dict['trade_commodity'][tec],
                                              level = config_dict['import_level'][tec],
                                              value = 1,
                                              unit = config_dict['trade_units'][tec],
                                              **common_years, **common_cols)
    
        df_output = pd.concat([df_output_trade, df_output_import]).drop_duplicates()
        
        parameter_outputs[tec]['trade']['output'] = df_output
            
    # Create base file: technical_lifetime
    for tec in covered_tec:
        df_teclt = build_parameterdf('technical_lifetime',
                                     network_df = network_setup[tec],
                                     col_values = dict(value = 1, # Make 1 year by default
                                                       unit = 'y'))
        
        parameter_outputs[tec]['trade']['technical_lifetime'] = df_teclt.drop_duplicates()
            
    # Create base files: inv_cost, fix_cost, var_cost
    for cost_par in ['inv_cost', 'fix_cost', 'var_cost']:
        for tec in covered_tec:
            df_cost = build_parameterdf(cost_par,
                                        network_df = network_setup[tec],
                                        col_values = dict(unit = 'USD/' + config_dict['trade_units'][tec]))
        
        parameter_outputs[tec]['trade'][cost_par] = df_cost.drop_duplicates()

    # Create base file: historical activity
    for tec in covered_tec:
        df_hist = pd.DataFrame()
        for y in list(range(2000, 2025, 5)):
            ydf =  build_parameterdf('historical_activity',
                                     network_df = network_setup[tec], 
                                     col_values = dict(unit = config_dict['trade_units'][tec]))
            ydf['year_act'] = y
            df_hist = pd.concat([df_hist, ydf])
            
        parameter_outputs[tec]['trade']['historical_activity'] = df_hist.drop_duplicates()

    # Create base file: capacity_factor
    for tec in covered_tec: 
        df_cf = build_parameterdf('capacity_factor',
                                  network_df = network_setup[tec],
                                  col_values = dict(value = 1,
                                                    unit = '%'))
        
        parameter_outputs[tec]['trade']['capacity_factor'] = df_cf.drop_duplicates()
            
    # Create base files for constraints
    for par_name in ['initial_activity', 'abs_cost_activity_soft',
                     'growth_activity', 'level_cost_activity_soft', 'soft_activity']:
        for t in ['lo', 'up']:
            for tec in covered_tec:
                df_con = build_parameterdf(par_name + '_' + t,
                                           network_df = network_setup[tec],
                                           col_values = dict(unit = config_dict['trade_units'][tec]))
                
                if (par_name == 'growth_activity'):
                    if t == 'lo': df_con['value'] = -0.05
                    if t == 'up': df_con['value'] = 0.05
                parameter_outputs[tec]['trade'][par_name + '_' + t] = df_con.drop_duplicates()
  
    # Create base file: emission_factor
    for tec in covered_tec: 
        if config_dict['tracked_emissions'][tec] != None:
            df_ef = pd.DataFrame()
            for emission_type in config_dict['tracked_emissions'][tec]: 
                df_ef_t = build_parameterdf('emission_factor',
                                          network_df = network_setup[tec],
                                          col_values = dict(unit = None,
                                                            emission = emission_type))
                df_ef = pd.concat([df_ef, df_ef_t])
                
            parameter_outputs[tec]['trade']['emission_factor'] = df_ef.drop_duplicates()
            
    # Replicate base file: For gas- imports require relation to domestic_coal and domestic_gas
    for tec in covered_tec:  
        if tec in ['gas_piped']:
            for rel_act in ['domestic_coal', 'domestic_gas']:
                df_rel = message_ix.make_df('relation_activity',
                                            node_loc = network_setup[tec]['importer'],
                                            node_rel = network_setup[tec]['importer'],
                                            technology = network_setup[tec]['import_technology'],
                                            commodity = config_dict['trade_commodity'][tec],
                                            value = -1,
                                            unit = '???',
                                            relation = rel_act,
                                            **common_years, **common_cols)
                df_rel = df_rel.drop_duplicates()
              
                parameter_outputs[tec]['trade']['relation_activity_' + rel_act] = df_rel.drop_duplicates()
                   
    # Replicate base file: Relation for CO2 emissions accounting
    for tec in covered_tec:  
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['exporter'],
                                    node_rel = network_setup[tec]['exporter'],
                                    technology = network_setup[tec]['export_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    unit = '???',
                                    relation = 'CO2_Emission',
                                    **common_years, **common_cols)
        df_rel = df_rel.drop_duplicates()

        parameter_outputs[tec]['trade']['relation_activity_CO2_Emission'] = df_rel.drop_duplicates()
    
    # Replicate base file: Relation for primary energy total accounting
    for tec in covered_tec:  
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['exporter'],
                                    node_rel = network_setup[tec]['exporter'],
                                    technology = network_setup[tec]['export_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    value = -1,
                                    unit = '???',
                                    relation = 'PE_total_traditional',
                                    **common_years, **common_cols)
        df_rel = df_rel.drop_duplicates()
        
        parameter_outputs[tec]['trade']['relation_activity_PE_total_traditional'] = df_rel.drop_duplicates()
        
    # Create base file: Relation to aggregate exports so global level can be calculated/calibrated
    for tec in covered_tec:  
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['exporter'],
                                    node_rel = network_setup[tec]['exporter'],
                                    technology = network_setup[tec]['export_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    value = 1,
                                    unit = '???',
                                    relation = config_dict['trade_technology'][tec] + '_exp_global',
                                    **common_years, **common_cols)
        df_rel = df_rel.drop_duplicates()
        
        parameter_outputs[tec]['trade']['relation_activity_global_aggregate'] = df_rel.drop_duplicates()
        
    # Create base file: relation to link all pipelines/routes to an exporter
    for tec in covered_tec:    
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['exporter'],
                                    node_rel = network_setup[tec]['exporter'],
                                    technology = network_setup[tec]['export_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    value = 1,
                                    unit = '???',
                                    relation = config_dict['trade_technology'][tec] + '_exp_from_' +\
                                        network_setup[tec]['exporter'].str.lower().str.split('_').str[1],
                                    **common_years, **common_cols)
        df_rel = df_rel.drop_duplicates()
        
        parameter_outputs[tec]['trade']['relation_activity_regional_exp'] = df_rel.drop_duplicates()
        
    # Create base file: relation to link all pipelines/routes to an importer
    for tec in covered_tec:         
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['importer'],
                                    node_rel = network_setup[tec]['importer'],
                                    technology = network_setup[tec]['import_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    value = 1,
                                    unit = '???',
                                    relation = config_dict['trade_technology'][tec] + '_imp_to_' +\
                                        network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                    **common_years, **common_cols)
        df_rel = df_rel.drop_duplicates()
        
        parameter_outputs[tec]['trade']['relation_activity_regional_imp'] = df_rel.drop_duplicates()
    
    ## FLOW TECHNOLOGY ##
    #####################
    # Create file: Input
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['input'] = df_input = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]:
            
            # List of commodity/material inputs
            flow_inputs = config_dict['flow_fuel_input'][tec][flow_tec]
            #if config_dict['flow_material_input'][tec] != None:
            #    flow_inputs = flow_inputs + config_dict['flow_material_input'][tec][flow_tec]
            
            ## Create bare file: input        
            # Build by commodity input
            for c in flow_inputs:
                print(c)
                if c in config_dict['flow_fuel_input'][tec][flow_tec]: use_unit = 'GWa'
                elif c in config_dict['flow_material_input'][tec][flow_tec]: use_unit = 'Mt'
                
                # If bilaterally constrained (e.g., pipelines), technologies and outputs are bilateral
                if config_dict['flow_constraint'][tec] == "bilateral":
                    df_input_base = message_ix.make_df('input',
                                                       node_loc = network_setup[tec]['exporter'],
                                                       node_origin = network_setup[tec]['exporter'],
                                                       technology = flow_tec + '_' +\
                                                           network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                       commodity = c,
                                                       unit = use_unit,
                                                       **common_years, **common_cols)
                    df_input = pd.concat([df_input, df_input_base])
                
                # If globally constrained (e.g., shipping), technologies and outputs are global with diff modes
                if config_dict['flow_constraint'][tec] == 'global':
                    df_input_base = message_ix.make_df('input',
                                                       node_loc = network_setup[tec]['exporter'],
                                                       node_origin = network_setup[tec]['exporter'],
                                                       mode = network_setup[tec]['exporter'].str.replace(message_regions + '_', '') +\
                                                               '-' +\
                                                               network_setup[tec]['importer'].str.replace(message_regions + '_', ''),
                                                       technology = flow_tec,
                                                       commodity = c,
                                                       unit = use_unit,
                                                       time = 'year', time_origin = 'year',
                                                       **common_years)
                    if "shipped" in tec:
                        df_input_base['value'] = 1e-6 # Default is 1e-6 GWa
                    df_input = pd.concat([df_input, df_input_base])

                # For shipped trade, set up bunker fuels
                if config_dict['bunker_technology'][tec] is not None:

                    # Global bunker
                    df_input_gbunk = df_input.copy()
                    df_input_gbunk['technology'] = 'bunker_global' + '_' + c
                    df_input_gbunk['mode'] = 'M1'
                    df_input_gbunk['value'] = 1 # Default is 1 GWa
                    df_input = pd.concat([df_input, df_input_gbunk])
        
        df_input = df_input.drop_duplicates()
        parameter_outputs[tec]['flow']['input'] = df_input
                
    # Create base file: output
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['output'] = df_output = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]:
            print(flow_tec)
            # If bilaterally constrained (e.g., pipelines), technologies and outputs are bilateral
            if config_dict['flow_constraint'][tec] == "bilateral": 
                df_output_base = message_ix.make_df('output',
                                                    node_loc = network_setup[tec]['exporter'],
                                                    node_dest = network_setup[tec]['exporter'],
                                                    technology = flow_tec + '_' +\
                                                        network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                    commodity = config_dict['flow_commodity_output'][tec] + '_' +\
                                                        network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                    unit = config_dict['flow_units'][tec],
                                                    level = config_dict['trade_level'][tec],
                                                    **common_years, **common_cols)
                df_output = pd.concat([df_output, df_output_base])
                    
            # If globally constrained (e.g., shipping), technologies and outputs are global with diff modes
            if config_dict['flow_constraint'][tec] == 'global':
                df_output_base = message_ix.make_df('output',
                                                    node_loc = network_setup[tec]['exporter'],
                                                    node_dest = message_regions + '_GLB',
                                                    mode = 'M1',
                                                    technology = flow_tec,
                                                    commodity = config_dict['flow_commodity_output'][tec],
                                                    unit = config_dict['flow_units'][tec],
                                                    level = config_dict['trade_level'][tec],
                                                    time = 'year', time_dest = 'year',
                                                    **common_years)
                df_output = pd.concat([df_output, df_output_base])
                
            # For shipped trade, set up bunker fuels
            if config_dict['bunker_technology'][tec] is not None:
                # Global bunker
                df_output_gbunk = df_output.copy()
                df_output_gbunk['technology'] = 'bunker_global_' + config_dict['flow_fuel_input'][tec][flow_tec][0]
                df_output_gbunk['node_dest'] = message_regions + '_GLB'
                df_output_gbunk['level'] = 'bunker'
                df_output_gbunk['unit'] = 'GWa'
                df_output_gbunk['mode'] = 'M1'
                df_output_gbunk['commodity'] = config_dict['flow_fuel_input'][tec][flow_tec][0]
                df_output = pd.concat([df_output, df_output_gbunk])
            
            df_output = df_output.drop_duplicates()
        df_output['value'] = 1    
        parameter_outputs[tec]['flow']['output'] = df_output

    # Create base files for capacity constraints
    
    for tec in covered_tec:
        for par in ['growth_new_capacity', 'initial_new_capacity']:
            parameter_outputs[tec]['flow'][par + '_up'] = parameter_outputs[tec]['flow'][par + '_lo'] = df_con = pd.DataFrame()
        
            for flow_tec in config_dict['flow_technologies'][tec]:
                for t in ['lo', 'up']:
                    if config_dict['flow_constraint'][tec] == 'bilateral':
                        df_con = message_ix.make_df(par + '_' + t,
                                                     node_loc = network_setup[tec]['exporter'],
                                                     technology = flow_tec + '_' +\
                                                         network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                     unit = config_dict['flow_units'][tec],
                                                     **common_years, **common_cols)
                    elif config_dict['flow_constraint'][tec] == 'global':
                        df_con = message_ix.make_df(par + '_' + t,
                                                     node_loc = network_setup[tec]['exporter'],
                                                     technology = flow_tec,
                                                     unit = config_dict['flow_units'][tec],
                                                     **common_years, **common_cols)
                    parameter_outputs[tec]['flow'][par + '_' + t] = pd.concat([parameter_outputs[tec]['flow'][par + '_' + t],
                                                                                            df_con])
    # Create base file: costs for flow technology (Fix and Investment)
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['fix_cost'] = parameter_outputs[tec]['flow']['inv_cost'] = df_cost = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]:
            for cost_par in ['fix_cost', 'inv_cost']:
                
                if config_dict['flow_constraint'][tec] == 'bilateral':
                    df_cost = message_ix.make_df(cost_par,
                                                 node_loc = network_setup[tec]['exporter'],
                                                 technology = flow_tec + '_' +\
                                                     network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                 unit = 'USD/' + config_dict['flow_units'][tec],
                                                 **common_years, **common_cols)
                elif config_dict['flow_constraint'][tec] == 'global':
                    df_cost = message_ix.make_df(cost_par,
                                                 node_loc = network_setup[tec]['exporter'],
                                                 technology = flow_tec,
                                                 unit = 'USD/' + config_dict['flow_units'][tec],
                                                 **common_years, **common_cols)
                df_cost = df_cost.drop_duplicates()
                
                if "shipped" in tec:
                    df_cost['value'] = 0.04  # Default is 0.04 USD/GWa
                parameter_outputs[tec]['flow'][cost_par] = pd.concat([parameter_outputs[tec]['flow'][cost_par],
                                                                        df_cost])

    # Create base file: costs for flow technology (Variable)
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['var_cost'] = df_vcost_base = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]: 
            if config_dict['flow_constraint'][tec] == 'bilateral':
                df_vcost_base = message_ix.make_df('var_cost',
                                                   node_loc = network_setup[tec]['exporter'],
                                                   technology = flow_tec + '_' +\
                                                   network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                   unit = 'USD/' + config_dict['flow_units'][tec],
                                                   **common_years, **common_cols)
                    
            elif config_dict['flow_constraint'][tec] == 'global':
                df_vcost_base = message_ix.make_df('var_cost',
                                                   node_loc = network_setup[tec]['exporter'],
                                                   mode = network_setup[tec]['exporter'].str.replace(message_regions + '_', '') +\
                                                           '-' +\
                                                   network_setup[tec]['importer'].str.replace(message_regions + '_', ''),
                                                   technology = flow_tec,
                                                   unit = 'USD/' + config_dict['flow_units'][tec],
                                                   time = 'year',
                                                   **common_years)
            if "shipped" in tec:
                df_vcost_base['value'] = 0.002  # Default is 0.002 USD/Mt-km
            parameter_outputs[tec]['flow']['var_cost'] = pd.concat([parameter_outputs[tec]['flow']['var_cost'],
                                                                    df_vcost_base])
        
    # Create base file: capacity factor for flow technology
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['capacity_factor'] = df_cf = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]: 
            df_cf_base = build_parameterdf('capacity_factor',
                                           network_df = network_setup[tec],
                                           col_values = dict(value = 1,
                                                        unit = '%'),
                                           export_only = True)
            
            if config_dict['flow_constraint'][tec] == 'bilateral':
                df_cf_base['technology'] =  flow_tec + '_' + df_cf_base['technology'].str.lower().str.split('_').str[-1]     
            elif config_dict['flow_constraint'][tec] == 'global':
                df_cf_base['technology'] =  flow_tec
                
            df_cf_base = df_cf_base.drop_duplicates()
            
            if config_dict['bunker_technology'][tec] is not None:
                # Add global bunker fuel technology
                bdf = df_cf_base.copy()
                bdf['technology'] = 'bunker_global_' + config_dict['flow_fuel_input'][tec][flow_tec][0]
                df_cf_base = pd.concat([df_cf_base, bdf])
            
            df_cf = pd.concat([df_cf, df_cf_base])
            
        parameter_outputs[tec]['flow']['capacity_factor'] = df_cf
            
    # Create base file: technical lifetime for flow technology
    for tec in covered_tec:
        parameter_outputs[tec]['flow']['technical_lifetime'] = df_teclt = pd.DataFrame()
        
        for flow_tec in config_dict['flow_technologies'][tec]: 
            df_teclt_base =  build_parameterdf('technical_lifetime',
                                               network_df = network_setup[tec], 
                                               col_values = dict(value = 20, # Default is 20 years
                                                            unit = 'y'),
                                               export_only = True)
            
            if config_dict['flow_constraint'][tec] == 'bilateral':
                df_teclt_base['technology'] =  flow_tec + '_' + df_teclt_base['technology'].str.lower().str.split('_').str[-1]     
            elif config_dict['flow_constraint'][tec] == 'global':
                df_teclt_base['technology'] =  flow_tec
                
            df_teclt_base = df_teclt_base.drop_duplicates()
            
            if config_dict['bunker_technology'][tec] is not None:
                # Add global bunker fuel technology
                bdf = df_teclt_base.copy()
                bdf['technology'] = 'bunker_global_' + config_dict['flow_fuel_input'][tec][flow_tec][0]
                df_teclt_base = pd.concat([df_teclt_base, bdf])
            df_teclt = pd.concat([df_teclt, df_teclt_base])
            
        parameter_outputs[tec]['flow']['technical_lifetime'] = df_teclt
            
    # Add flow technology outputs as an input into the trade technology
    for tec in covered_tec:        
        for flow_tec in config_dict['flow_technologies'][tec]: 
            if config_dict['flow_constraint'][tec] == 'bilateral':
                df_input_flow = message_ix.make_df('input',
                                                   node_loc = network_setup[tec]['exporter'],
                                                   node_origin = network_setup[tec]['exporter'],
                                                   technology = network_setup[tec]['export_technology'],
                                                   commodity = config_dict['flow_commodity_output'][tec] + '_' +\
                                                       network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                   unit = config_dict['flow_units'][tec],
                                                   level = config_dict['trade_level'][tec],
                                                   **common_years, **common_cols).drop_duplicates()
            elif config_dict['flow_constraint'][tec] == 'global':
                df_input_flow = message_ix.make_df('input',
                                                   node_loc = network_setup[tec]['exporter'],
                                                   node_origin = message_regions + '_GLB',
                                                   technology = network_setup[tec]['export_technology'],
                                                   commodity = config_dict['flow_commodity_output'][tec],
                                                   unit = config_dict['flow_units'][tec],
                                                   level = config_dict['trade_level'][tec],
                                                   **common_years, **common_cols).drop_duplicates()
                
                # For shipped commodities, calculated capacities based on distance and energy content
                distance_df = pd.read_csv(os.path.join(data_path, "distances", message_regions + "_distances.csv"))
                energycontent_df = pd.read_excel(os.path.join(data_path, "specific_energy.xlsx"))
                if config_dict['trade_units'][tec] == 'GWa':
                    specificcontent = energycontent_df[energycontent_df['Commodity'] == config_dict['trade_commodity'][tec]]['Specific Energy (GWa/Mt)'].reset_index(drop = True)[0]
                else:
                    specificcontent = 1
                multiplier_df = distance_df.copy()
                multiplier_df['node_loc'] = multiplier_df['Node1']
                multiplier_df['technology'] = config_dict['trade_technology'][tec] + '_exp_' + multiplier_df['Node2'].str.lower().str.split('_').str[1]
                multiplier_df['energy_content'] = specificcontent
                multiplier_df['multiplier'] = multiplier_df['Distance_km'] / multiplier_df['energy_content'] #Mt-km/GWa
                multiplier_df = multiplier_df[['node_loc', 'technology', 'multiplier']].drop_duplicates()
                
                df_input_flow = df_input_flow.merge(multiplier_df, 
                                                    left_on = ['node_loc', 'technology'], right_on = ['node_loc', 'technology'],
                                                    how = 'left')
                df_input_flow['value'] = df_input_flow['multiplier']
                df_input_flow = df_input_flow[message_ix.make_df('input').columns]
            
            parameter_outputs[tec]['trade']['input'] = pd.concat([parameter_outputs[tec]['trade']['input'],
                                                                  df_input_flow])
     
    ## Export files
    for tec in covered_tec:
        log.info(f"Exporting trade parameters for {tec}")
        for parname in parameter_outputs[tec]['trade'].keys():
            parameter_outputs[tec]['trade'][parname].to_csv(os.path.join(data_path, tec, "edit_files", parname + '.csv'),
                                                            index=False)
            log.info(f"...trade {parname}")
        for parname in parameter_outputs[tec]['flow'].keys():
            parameter_outputs[tec]['flow'][parname].to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology",
                                                                        parname + ".csv"),
                                                           index = False)
            log.info(f"...flow {parname}")
            
    ## Transfer files from edit to bare if they do not already exist
    for tec in covered_tec:
        required_parameters = [os.path.join("capacity_factor.csv"),
                               os.path.join("input.csv"),
                               os.path.join("output.csv"),
                               os.path.join("technical_lifetime.csv"),
                               os.path.join("flow_technology", "capacity_factor.csv"),
                               os.path.join("flow_technology", "input.csv"),
                               os.path.join("flow_technology", "output.csv"),
                               os.path.join("flow_technology", "technical_lifetime.csv")]
        for reqpar in required_parameters:
            if not os.path.isfile(os.path.join(data_path, tec, "bare_files", reqpar)):
               base_file = os.path.join(data_path, tec, "edit_files", reqpar)
               dest_file = os.path.join(data_path, tec, "bare_files", reqpar)
               shutil.copy2(base_file, dest_file)
               log.info(f"Copied file from edit to bare: {reqpar}")
    ## Transfer cost parameters for flow technologies using shipping from edit to bare
    for tec in covered_tec:
        if 'shipping' in tec:
            required_parameters = [os.path.join("flow_technology", "var_cost.csv"),
                                   os.path.join("flow_technology", "inv_cost.csv")]
            for reqpar in required_parameters:
                if not os.path.isfile(os.path.join(data_path, tec, "bare_files", reqpar)):
                    base_file = os.path.join(data_path, tec, "edit_files", reqpar)
        
#%% Build out bare sheets
def build_parameter_sheets(log, 
                           project_name: str = None,
                           config_name: str = None):
    """
    Read the input csv files and build the tech sets and parameters.

    Args:
        project_name (str, optional): Project name (message_ix_models/project/[THIS]) 
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config_default.yaml
    Returns:
        outdict: Dictionary of parameter dataframes
    """
    # Load config
    config, config_path = load_config(project_name, config_name)

    covered_tec = config.get('covered_trade_technologies', {})
    
    outdict = dict()
    
    ya_list = config['timeframes']['year_act_list']
    yv_list = config['timeframes']['year_vtg_list']
    
    for tec in covered_tec:
        
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)
        
        data_dict = {}
        data_dict['trade'] = {}
        data_dict['flow'] = {}
        
        for ty in ['trade', 'flow']:
            if ty == 'trade': tpath = os.path.join(tecpath, 'bare_files')
            if ty == 'flow': tpath = os.path.join(tecpath, 'bare_files', 'flow_technology')
            
            csv_files = [f for f in Path(tpath).glob("*.csv")]
        
            for csv_file in csv_files:
                key = csv_file.stem
                data_dict[ty][key] = pd.read_csv(csv_file)

        # Broadcast the data   
        for ty in ['trade', 'flow']:
            
            for i in data_dict[ty].keys():
                if "year_rel" in data_dict[ty][i].columns:
                    log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_rel.")
                    if data_dict[ty][i]["year_rel"].iloc[0] == "broadcast":
                        data_dict[ty][i] = broadcast_years(df = data_dict[ty][i],
                                                           year_type = 'year_rel',
                                                           year_list = ya_list)
                        data_dict[ty][i]["year_act"] = data_dict[ty][i]["year_rel"]
                else:
                    pass
                
                if "year_vtg" in data_dict[ty][i].columns and "year_act" in data_dict[ty][i].columns:
                    if (data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                        and data_dict[ty][i]["year_act"].iloc[0] == "broadcast"):
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_vtg and year_act.")
                        data_dict[ty][i] = broadcast_yv_ya(df = data_dict[ty][i], 
                                                           ya_list = ya_list,
                                                           yv_list = yv_list,
                                                           tec_lifetime = data_dict[ty]['technical_lifetime'].copy())
                    elif (data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                        and data_dict[ty][i]["year_act"].iloc[0] != "broadcast"):
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_vtg.")
                        data_dict[ty][i] = broadcast_years(df = data_dict[ty][i],
                                                           year_type = 'year_vtg',
                                                           year_list = yv_list)                
                elif "year_vtg" in data_dict[ty][i].columns and "year_act" not in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_vtg.")
                        data_dict[ty][i] = broadcast_years(df = data_dict[ty][i],
                                                           year_type = 'year_vtg',
                                                           year_list = yv_list)   
                elif "year_vtg" not in data_dict[ty][i].columns and "year_act" in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_act"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_act.")
                        data_dict[ty][i] = broadcast_years(df = data_dict[ty][i],
                                                           year_type = 'year_act',
                                                           year_list = ya_list)                     
                else:
                    pass

        # Imports do not vintage
        for par in ['capacity_factor', 'input', 'output']:
            vdf = data_dict['trade'][par]
            vdf = vdf[((vdf['technology'].str.contains('_imp')) & (vdf['year_vtg'] == vdf['year_act']))|\
                      (vdf['technology'].str.contains('_exp_'))]
            data_dict['trade'][par] = vdf
        
        # Variable costs for flows should not broadcast
        for par in ['var_cost']:
            if par in list(data_dict['flow'].keys()):
                vdf = data_dict['flow'][par]
                vdf = vdf[vdf['year_act'] == vdf['year_vtg']]
                data_dict['flow'][par] = vdf
        
        outdict[tec] = data_dict
    
    return outdict
#%% Clone and update scenario
def clone_and_update(trade_dict, 
                     log,
                     solve = False,
                     to_gdx = False,
                     project_name: str = None,
                     config_name: str = None,
                     update_scenario_name:str = None,
                     additional_parameter_updates:dict = None,
                     gdx_location: str = os.path.join("C:", "GitHub", "message_ix", "message_ix", "model", "data"),
                     remove_pao_coal_constraint: bool = True):     
    """
    Clone and update scenario.
    
    Args:
        trade_dict: Dictionary of parameter dataframes
        log: Log file to track progress
        solve: If True, solve scenario
        to_gdx: If True, save scenario to a GDX file
    Optional Args:
        project_name: Name of project (message_ix_models/project/[THIS])
        config_name: Name of config file.
            If None, uses default config from data/bilateralize/config_default.yaml
        update_scenario_name: Name of scenario to update
        additional_parameter_updates: Dictionary of additional parameter updates
        gdx_location: Location to save GDX file
        remove_pao_coal_constraint: If True, remove PAO coal and gas constraints on primary energy
    """
    # Load config
    config_base, config_path, config_tec = load_config(project_name, config_name, load_tec_config=True)
       
    # Load the scenario
    mp = ixmp.Platform()
    start_model = config_base.get("scenario", {}).get("start_model")
    start_scen = config_base.get("scenario", {}).get("start_scen")
     
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {config_path}")
        log.error(error_msg)
        raise ValueError(error_msg)
    
    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")
     
    # Clone scenario
    target_model = config_base.get("scenario", {}).get("target_model", [])
    
    if update_scenario_name == None:
        target_scen = config_base.get("scenario", {}).get("target_scen", [])
    else:
        target_scen = update_scenario_name
        
    scen = base.clone(target_model, target_scen, keep_solution=False)
    scen.set_as_default()
    log.info("Scenario cloned.")
    
    # Add sets and parameters for each covered technology
    covered_tec = config_base.get('covered_trade_technologies')
    
    for tec in covered_tec:
        
        # Remove existing technologies related to trade
        base_tec_name = tec.replace("_shipped", "")
        base_tec_name = base_tec_name.replace("_piped", "")
        
        base_tec = [config_tec.get(tec).get(tec + '_trade').get('trade_commodity') + '_exp', # These may not exist but in case they do...
                    config_tec.get(tec).get(tec + '_trade').get('trade_commodity') + '_imp',
                    base_tec_name + '_exp', # These may not exist but in case they do...
                    base_tec_name + '_imp',]
        if tec == 'gas_piped':
            base_tec = base_tec + [i for i in scen.set('technology') if 
                                   config_tec.get(tec).get(tec + '_trade').get('trade_commodity') + '_exp_' in i]
        if 'crudeoil' in tec:
            base_tec = base_tec + ['oil_exp', 'oil_imp']
            
        base_tec = list(set(base_tec))
        
        with scen.transact("Remove base trade technologies for " + tec):
            for t in base_tec:
                if t in list(scen.set('technology')):
                    log.info('Removing base technology...' + t)
                    scen.remove_set('technology', t)
                    
        # Add to sets: technology, level, commodity, mode
        new_sets = dict()
        for s in ['technology', 'level', 'commodity', 'mode']:
            setlist = set(list(trade_dict[tec]['trade']['input'][s].unique()) +\
                          list(trade_dict[tec]['trade']['output'][s].unique()))
                
            if "input" in trade_dict[tec]['flow'].keys():
                setlist = setlist.union(set(list(trade_dict[tec]['flow']['input'][s].unique()) +\
                                            list(trade_dict[tec]['flow']['output'][s].unique())))
            setlist = list(setlist)
            
            new_sets[s] = setlist
        
        with scen.transact("Add new sets for " + tec):
            for s in ['technology', 'level', 'commodity', 'mode']:
                base_set = list(scen.set(s))
                for i in new_sets[s]:
                    if i not in base_set:
                        log.info("Adding set: " + s + "..." + i)
                        scen.add_set(s, i)
                    else:
                        pass
                
        # Add parameters
        new_parameter_list = list(set([i for i in list(trade_dict[tec]['trade'].keys()) + list(trade_dict[tec]['flow'].keys())
                                       if 'relation_' not in i])) # do relation activity/lower/upper separately

        with scen.transact("Add new parameters for " + tec):
            for p in new_parameter_list:
                log.info('Adding parameter for ' + tec + ': ' + p)
                pardf = pd.DataFrame()
                if trade_dict[tec]['trade'].get(p) is not None:
                    log.info('... parameter added for trade technology')
                    pardf = pd.concat([pardf, trade_dict[tec]['trade'][p]])
                if trade_dict[tec]['flow'].get(p) is not None: 
                    log.info('... parameter added for flow technology')
                    pardf = pd.concat([pardf, trade_dict[tec]['flow'][p]])
                pardf = pardf[pardf['value'].isnull() == False]
                scen.add_par(p, pardf)
    
        # Relation activity, upper, and lower
        rel_parameter_list = list(set([i for i in list(trade_dict[tec]['trade'].keys()) + list(trade_dict[tec]['flow'].keys())
                                       if 'relation_' in i]))

        if len(rel_parameter_list) > 0:
            for rel_par in ['relation_activity', 'relation_upper', 'relation_lower']:
                rel_par_df = pd.DataFrame()
                
                rel_par_list = list(set([i for i in list(trade_dict[tec]['trade'].keys()) + list(trade_dict[tec]['flow'].keys())
                                         if rel_par in i]))
                
                for r in rel_par_list:
                    if r in list(trade_dict[tec]['trade'].keys()):
                        rel_par_df = pd.concat([rel_par_df, trade_dict[tec]['trade'][r]])
                    if r in list(trade_dict[tec]['flow'].keys()):
                        rel_par_df = pd.concat([rel_par_df, trade_dict[tec]['flow'][r]])
            
                if rel_par == 'relation_activity':
                    with scen.transact('Adding new relation sets'):
                        new_relations = list(rel_par_df['relation'].unique())
                        for nr in new_relations:
                            if nr not in scen.set('relation').unique():
                                scen.add_set('relation', nr)
                if len(rel_par_df) > 0:       
                    with scen.transact('Add ' + rel_par + ' for ' + tec):
                        log.info('Adding ' + rel_par + ' for ' + tec)
                        rel_par_df = rel_par_df[rel_par_df['value'].isnull() == False]
                        scen.add_par(rel_par, rel_par_df)
        
        # Update bunker fuels
        bunker_tec = config_tec.get(tec).get(tec + '_trade').get('bunker_technology')
        if bunker_tec != None:
            for btec in bunker_tec.keys():
                bunkerdf_in = scen.par('input', filters = {'technology': bunker_tec[btec]})
                bunkerdf_out = bunkerdf_in.copy()
                bunkerdf_out['level'] = 'bunker'
                with scen.transact('Update bunker fuel for' + tec):
                    log.info('Updating bunker level for ' + tec)
                    scen.remove_par('input', bunkerdf_in)
                    scen.add_par('input', bunkerdf_out)
    
    if additional_parameter_updates != None:
        for par in additional_parameter_updates.keys():
            with scen.transact('Update additional parameter: ' + par):
                new_df = additional_parameter_updates[par]
                base_df = scen.par(par)
                
                if 'value' in new_df.columns:
                    rem_df = new_df[[c for c in new_df.columns if c != 'value']] # everything but value
                    base_df = base_df.merge(rem_df, left_on = list(rem_df.columns), right_on = list(rem_df.columns), how = 'inner')
                    add_df = base_df.copy().drop(['value'], axis = 1)
                    add_df = add_df.merge(new_df, left_on = list(rem_df.columns), right_on = list(rem_df.columns), how = 'left')
                
                if 'multiplier' in new_df.columns:
                    col_list = [c for c in new_df.columns if c != 'multiplier']
                    base_df = base_df.merge(new_df, left_on = col_list, right_on = col_list, how = 'inner')
                    add_df = base_df.copy()
                    base_df = base_df.drop(['multiplier'], axis = 1)
                    add_df['value'] = add_df['value'] * add_df['multiplier']
                    add_df = add_df.drop(['multiplier'], axis = 1)
                    
                scen.remove_par(par, base_df)
                scen.add_par(par, add_df)
    
    if remove_pao_coal_constraint == True:
        with scen.transact("Remove PAO coal and gas constraints on primary energy"):
            for rel in ['domestic_coal', 'domestic_gas']:
                log.info('Removing constraints on PAO primary energy: ' + rel)
                relact_df = scen.par('relation_activity', filters = {'relation': rel})
                relupp_df = scen.par('relation_upper', filters = {'relation': rel})
                scen.remove_par('relation_activity', relact_df)
                scen.remove_par('relation_upper', relupp_df)
                
    if (to_gdx == True) & (solve == False):
        save_to_gdx(mp = mp,
                    scenario = scen,
                    output_path = Path(os.path.join(gdx_location, 'MsgData_'+ target_model + '_' + target_scen + '.gdx')))     
        
    if solve == True:
        solver = "MESSAGE"
        scen.solve(solver, solve_options=dict(lpmethod=4))
        
        print("Unlock run ID of the scenario")
        runid = scen.run_id()
        mp._backend.jobj.unlockRunid(runid)                    
