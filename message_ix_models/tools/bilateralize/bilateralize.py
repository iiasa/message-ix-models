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
                config_name:str = None):
    
    # Load config
    if (project_name is None) & (config_name is None):
        config_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path("bilateralize")), 
                                                   "bilateralize", "config_default.yaml"))
    if project_name is not None:
        if config_name is None: config_name = "config.yaml"
        config_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path(project_name)), 
                                                   os.path.pardir, "project", project_name, config_name))
        
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) # safe_load is recommended over load for security
    
    return config, config_path

#%% Copy columns from template, if exists
def copy_template_columns(df, template, exclude_cols=["node_loc", "technology"]):
    for col in template.columns:
        if col not in exclude_cols:
            df[col] = template[col].iloc[0]
            
#%% Broadcast years to create vintage-activity year pairs.
def broadcast_yv_ya(df: pd.DataFrame, 
                    ya_list: list[int],
                    tec_lifetime: int):
    """
    Broadcast years to create vintage-activity year pairs.

    Parameters
    ----------
    df : pd.DataFrame
        Input parameter DataFrame
    ya_list : list[int]
        List of activity years to consider

    Returns
    -------
    pd.DataFrame
        DataFrame with expanded rows for each vintage-activity year pair
    """
    all_new_rows = []
    # Process each row in the original DataFrame
    for _, row in df.iterrows():
        # For each activity year
        for ya in ya_list:
            # Get all vintage years that are <= activity year for a period < technical lifetime
            yv_list = [yv for yv in ya_list if yv <= ya]
            yv_list = [yv for yv in yv_list if yv >= ya-tec_lifetime]

            # Create new rows for each vintage year
            for yv in yv_list:
                new_row = row.copy()
                new_row["year_act"] = int(ya)
                new_row["year_vtg"] = int(yv)
                all_new_rows.append(new_row)
    # Combine original DataFrame with new rows
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df

#%% Broadcast vintage years
def broadcast_yv(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """Broadcast vintage years."""
    all_new_rows = []
    for _, row in df.iterrows():
        for yv in ya_list:
            new_row = row.copy()
            new_row["year_vtg"] = int(yv)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df.drop_duplicates()
#%% Broadcast relation years
def broadcast_yl(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """Broadcast relation years."""
    all_new_rows = []
    for _, row in df.iterrows():
        for yv in ya_list:
            new_row = row.copy()
            new_row["year_rel"] = int(yv)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_rel"] != "broadcast"]
    return result_df.drop_duplicates()
#%% Broadcast activity years
def broadcast_ya(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """Broadcast vintage years."""
    all_new_rows = []
    for _, row in df.iterrows():
        for ya in ya_list:
            new_row = row.copy()
            new_row["year_act"] = int(ya)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_act"] != "broadcast"]
    return result_df.drop_duplicates()
#%% Write just the GDX files
def save_to_gdx(mp, scenario, output_path):
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
        config_name: str = None,
        message_regions: str = 'R12'):
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
    config, config_path = load_config(project_name, config_name)

    # Retrieve config sections    
    #define_parameters = config.get('define_parameters', {})
    
    covered_tec = config.get('covered_trade_technologies', {})

    config_dict = {}    
    for tec in covered_tec:
        tec_dict = config.get(tec + '_trade', {})
        for k in tec_dict.keys():
            if k not in config_dict.keys(): config_dict[k] = {}
            config_dict[k][tec] = tec_dict[k]
                 
    # Load the scenario
    scenario_info = config.get('scenario', {})
    start_model = scenario_info['start_model']
    start_scen = scenario_info['start_scen']
    target_model = scenario_info['target_model']
    target_scen = scenario_info['target_scen']
    
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
                                            unit = 'GWa',
                                            **common_years, **common_cols)
                                 
        # Import Level (piped/shipped to import)
        df_input_import = message_ix.make_df('input',
                                             node_origin = network_setup[tec]['importer'],
                                             node_loc = network_setup[tec]['importer'],
                                             technology = network_setup[tec]['import_technology'],
                                             commodity = config_dict['trade_commodity'][tec],
                                             level = config_dict['trade_level'][tec],
                                             value = 1,
                                             unit = 'GWa',
                                             **common_years, **common_cols)
        
        df_input = pd.concat([df_input_trade, df_input_import]).drop_duplicates()
        
        df_input.to_csv(os.path.join(data_path, tec, "edit_files", "input.csv"), index=False)
        log.info(f"Input csv generated at: {os.path.join(data_path, tec)}.")

    # Create base file: output
    for tec in covered_tec:  
        
        # Trade Level
        df_output_trade = message_ix.make_df('output',
                                             node_loc = network_setup[tec]['exporter'],
                                             node_dest = network_setup[tec]['importer'],
                                             technology = network_setup[tec]['technology'],
                                             commodity = config_dict['trade_commodity'][tec],
                                             level = config_dict['trade_level'][tec],
                                             value = 1,
                                             unit = 'GWa',
                                             **common_years, **common_cols)
        
        # Import Level
        df_output_import = message_ix.make_df('output',
                                              node_loc = network_setup[tec]['importer'],
                                              node_dest = network_setup[tec]['importer'],
                                              technology = network_setup[tec]['technology'],
                                              commodity = config_dict['trade_commodity'][tec],
                                              level = config_dict['import_level'][tec],
                                              value = 1,
                                              unit = 'GWa',
                                              **common_years, **common_cols)
    
        df_output = pd.concat([df_output_trade, df_output_import]).drop_duplicates()
        
        df_output.to_csv(os.path.join(data_path, tec, "edit_files", "output.csv"), index=False)
        log.info(f"Output csv generated at: {os.path.join(data_path, tec)}.")
            
    # Create base file: technical_lifetime
    for tec in covered_tec:
        df_teclt = build_parameterdf('technical_lifetime',
                                     network_df = network_setup[tec],
                                     col_values = dict(value = 10, # Make 10 years by default
                                                       unit = 'y'))
        
        df_teclt.to_csv(os.path.join(data_path, tec, "edit_files", "technical_lifetime.csv"), index=False)
        log.info(f"Technical Lifetime csv generated at: {os.path.join(data_path, tec)}.")
            
    # Create base files: inv_cost, fix_cost, var_cost
    for cost_par in ['inv_cost', 'fix_cost', 'var_cost']:
        for tec in covered_tec:
            df_cost = build_parameterdf(cost_par,
                                        network_df = network_setup[tec],
                                        col_values = dict(value = None,
                                                          unit = 'USD/GWa'))
        
        df_cost.to_csv(os.path.join(data_path, tec, "edit_files", cost_par + ".csv"), index=False)
        log.info(f"{cost_par} csv generated at: {os.path.join(data_path, tec)}.") 

    # Create base file: historical activity
    for tec in covered_tec:
        df_hist = pd.DataFrame()
        for y in list(range(2000, 2025, 5)):
            ydf =  build_parameterdf('historical_activity',
                                     network_df = network_setup[tec], 
                                     col_values = dict(unit = 'GWa'))
            ydf['year_act'] = y
            df_hist = pd.concat([df_hist, ydf])

        df_hist.to_csv(os.path.join(data_path, tec, "edit_files", "historical_activity.csv"), index=False)
        log.info(f"Historical activity csv generated at: {os.path.join(data_path, tec)}.")

    # Create base file: capacity_factor
    for tec in covered_tec: 
        df_cf = build_parameterdf('capacity_factor',
                                  network_df = network_setup[tec],
                                  col_values = dict(value = 1,
                                                    unit = '%'))
        
        df_cf.to_csv(os.path.join(data_path, tec, "edit_files", "capacity_factor.csv"), index=False) # Does not require edit
        log.info(f"Capacity factor csv generated at: {os.path.join(data_path, tec)}.")
            
    # Create base files for constraints
    for par_name in ['initial_activity', 'abs_cost_activity_soft',
                     'growth_activity', 'level_cost_activity_soft', 'soft_activity']:
        for t in ['lo', 'up']:
            for tec in covered_tec:
                df_con = build_parameterdf(par_name + '_' + t,
                                           network_df = network_setup[tec],
                                           col_values = dict(unit = 'GWa'))
                df_con.to_csv(os.path.join(data_path, tec, 'edit_files', par_name + '_' + t + '.csv'),
                              index = False)
                log.info(f"{par_name}_{t} csv generated at: {os.path.join(data_path, tec)}.")
  
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
                
            df_ef.to_csv(os.path.join(data_path, tec, "edit_files", "emission_factor.csv"), index=False) # Does not require edit
            log.info(f"Emission factor csv generated at: {os.path.join(data_path, tec)}.")
            
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
              
                df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_" + rel_act + ".csv"),
                              index=False)
                log.info(f"Relation activity ({rel_act}) csv generated at: {os.path.join(data_path, tec)}.")
                   
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

        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_CO2_emission.csv"), index=False)
        log.info(f"Relation activity (CO2_emission) csv generated at: {os.path.join(data_path, tec)}.")
    
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

        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_PE_total_traditional.csv"), index=False)
        log.info(f"Relation activity (PE_total_traditional) csv generated at: {os.path.join(data_path, tec)}.")
        
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

        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_global_aggregate.csv"), index=False)
        log.info(f"Relation activity (global_aggregate) csv generated at: {os.path.join(data_path, tec)}.")
        
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

        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_regionalexp.csv"), index=False)
        log.info(f"Relation activity (regionalexp) csv generated at: {os.path.join(data_path, tec)}.")
        
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
        
        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "relation_activity_regionalimp.csv"), index=False)
        log.info(f"Relation activity (regionalimp) csv generated at: {os.path.join(data_path, tec)}.")
    
    ## FLOW TECHNOLOGY
    for tec in covered_tec:  
        
        # Set up flow technology name
        if config_dict['flow_tech_suffix'][tec] != None:
            full_flow_tec = config_dict['flow_technology'][tec] + '_' + config_dict['flow_tech_suffix'][tec]
        else:
            full_flow_tec = config_dict['flow_technology'][tec]
        
        # Build by commodity input
        if 'shipped' in tec: 
            flow_unit = 'Mt-km'
        elif 'piped' in tec: 
            flow_unit = 'km'
        else: 
            flow_unit = None
            
        # List of commodity/material inputs
        flow_inputs = config_dict['flow_fuel_input'][tec]
        if config_dict['flow_material_input'][tec] is not None: 
            flow_inputs = flow_inputs + config_dict['flow_material_input'][tec]
            
        # Create bare file: input        
        df_input = pd.DataFrame()
            
        # Build by commodity input
        for c in flow_inputs:
            if c in config_dict['flow_fuel_input'][tec]: use_unit = 'GWa'
            elif c in config_dict['flow_material_input'][tec]: use_unit = 'Mt'
            
            df_input_base = message_ix.make_df('input',
                                               node_loc = network_setup[tec]['exporter'],
                                               node_origin = network_setup[tec]['exporter'],
                                               technology = full_flow_tec + '_' +\
                                                   network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                               commodity = c,
                                               unit = use_unit,
                                               **common_years, **common_cols)
            df_input = pd.concat([df_input, df_input_base])

        # For shipped trade, set up bunker fuels
        if config_dict['bunker_technology'][tec] is not None:
            
            df_input_bunk = df_input.copy()
            
            # Regional bunker
            df_input_rbunk = df_input_bunk.copy()
            df_input_rbunk['technology'] = 'bunker_regional'
            df_input_rbunk['level'] = None
            df_input = pd.concat([df_input, df_input_rbunk])
                
            # Global bunker
            df_input_gbunk = df_input_bunk.copy()
            df_input_gbunk['technology'] = 'bunker_global'
            #df_input_gbunk['node_loc'] = message_regions + '_GLB'
            df_input_gbunk['level'] = 'bunker'
            df_input = pd.concat([df_input, df_input_gbunk])
        
        df_input = df_input.drop_duplicates()
        df_input.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", "input.csv"), index=False)
        log.info(f"Input flow csv generated at: {os.path.join(data_path, tec)}.")
            
        # Create base file: output
        df_output = pd.DataFrame()
        
        for c in flow_inputs:
            df_output_base = message_ix.make_df('output',
                                                node_loc = network_setup[tec]['exporter'],
                                                node_dest = network_setup[tec]['exporter'],
                                                technology = full_flow_tec + '_' +\
                                                    network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                                commodity = config_dict['flow_commodity_output'][tec],
                                                unit = flow_unit,
                                                level = config_dict['trade_level'][tec],
                                                **common_years, **common_cols)
            df_output = pd.concat([df_output, df_output_base])
        
        # For shipped trade, set up bunker fuels
        if config_dict['bunker_technology'][tec] is not None:
            
            df_output_bunk = df_output.copy()
            
            # Regional bunker
            df_output_rbunk = df_output_bunk.copy()
            df_output_rbunk['technology'] = 'bunker_regional'
            df_output_rbunk['level'] = 'bunker'
            df_output = pd.concat([df_output, df_output_rbunk])
                
            # Global bunker
            df_output_gbunk = df_output_bunk.copy()
            df_output_gbunk['technology'] = 'bunker_global'
            df_output_gbunk['node_dest'] = message_regions + '_GLB'
            df_output_gbunk['level'] = 'bunker'
            df_output = pd.concat([df_output, df_output_gbunk])
        
        df_output = df_output.drop_duplicates()

        df_output.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", "output.csv"), index=False)
        log.info(f"Output csv generated at: {os.path.join(data_path, tec)}.")

        # Create base file: costs for flow technology
        for cost_par in ['fix_cost', 'var_cost', 'inv_cost']:
            df_cost = message_ix.make_df(cost_par,
                                         node_loc = network_setup[tec]['exporter'],
                                         technology = full_flow_tec + '_' +\
                                             network_setup[tec]['importer'].str.lower().str.split('_').str[1],
                                         unit = 'USD/' + flow_unit,
                                         **common_years, **common_cols)
            
            df_cost = df_cost.drop_duplicates()
                
            df_cost.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", cost_par + ".csv"), index=False)
            log.info(f"{cost_par} csv generated at: {os.path.join(data_path, tec)}.")  
    
        # Create base file: capacity factor for flow technology
        df_cf = build_parameterdf('capacity_factor',
                                  network_df = network_setup[tec],
                                  col_values = dict(value = 1,
                                                    unit = '%'),
                                  export_only = True)
        
        df_cf['technology'] =  full_flow_tec + '_' + df_cf['technology'].str.lower().str.split('_').str[-1]     
        df_cf = df_cf.drop_duplicates()
        
        if config_dict['bunker_technology'][tec] is not None:
            # Add regional bunker fuel technology
            bdf = df_cf.copy()
            bdf['technology'] = 'bunker_regional'
            bdf = bdf.drop_duplicates()
            df_cf = pd.concat([df_cf, bdf])
            
            # Add global bunker fuel technology
            bdf = bdf.copy()
            bdf['technology'] = 'bunker_global'
            df_cf = pd.concat([df_cf, bdf])
            
        df_cf.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", "capacity_factor.csv"), index=False) # Does not require edit
        log.info(f"Capacity factor csv generated at: {os.path.join(data_path, tec)}.")
            
        # Create base file: technical lifetime for flow technolgoy
        df_teclt =  build_parameterdf('technical_lifetime',
                                      network_df = network_setup[tec], 
                                      col_values = dict(value = 20, # Default is 20 years
                                                        unit = 'y'),
                                      export_only = True)
                
        df_teclt['technology'] =  full_flow_tec + '_' + df_teclt['technology'].str.lower().str.split('_').str[-1]
        df_teclt = df_teclt.drop_duplicates()
        
        if config_dict['bunker_technology'][tec] is not None:
            # Add regional bunker fuel technology
            bdf = df_teclt.copy()
            bdf['technology'] = 'bunker_regional'
            bdf = bdf.drop_duplicates()
            df_teclt = pd.concat([df_teclt, bdf])
            
            # Add global bunker fuel technology
            bdf = bdf.copy()
            bdf['technology'] = 'bunker_global'
            df_teclt = pd.concat([df_teclt, bdf])
            
        df_teclt.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", "technical_lifetime.csv"), index=False)
        log.info(f"Technical Lifetime csv generated at: {os.path.join(data_path, tec)}.")
            
        # Create relation to link exports to the flow technology   
        df_rel = message_ix.make_df('relation_activity',
                                    node_loc = network_setup[tec]['exporter'],
                                    node_rel = network_setup[tec]['exporter'],
                                    technology = network_setup[tec]['export_technology'],
                                    commodity = config_dict['trade_commodity'][tec],
                                    **common_years, **common_cols)
        
        if config_dict['flow_constraint'][tec] == 'bilateral':
            df_rel['relation'] = full_flow_tec + '_' + df_rel['technology'].str.lower().str.split('_').str[-1]
        elif config_dict['flow_constraint'][tec] == 'global':
            df_rel['relation'] = full_flow_tec
                
        df_rel_trade = df_rel.copy()
        df_rel_flow = df_rel.copy()
        
        df_rel_flow['technology'] = full_flow_tec + '_' + df_rel_flow['technology'].str.lower().str.split('_').str[-1]

        if config_dict['flow_constraint'][tec] == 'global':    
            distance_df = pd.read_excel(os.path.join(data_path, "distances.xlsx"), sheet_name = 'dummy') #TODO: Update dummy to distances
            energycontent_df = pd.read_excel(os.path.join(data_path, "specific_energy.xlsx"))
            energycontent = energycontent_df[energycontent_df['Commodity'] == config_dict['trade_commodity'][tec]]['Specific Energy (GWa/Mt)'][0]
            
            multiplier_df = distance_df.copy()
            multiplier_df['node_loc'] = multiplier_df['node_rel'] = multiplier_df['exporter']
            multiplier_df['technology'] = config_dict['trade_technology'][tec] + '_exp_' + multiplier_df['importer'].str.lower().str.split('_').str[1]
            multiplier_df['energy_content'] = energycontent
            multiplier_df['multiplier'] = multiplier_df['distance'] / multiplier_df['energy_content'] #Mt-km/GWa
            multiplier_df = multiplier_df[['node_loc', 'node_rel', 'technology', 'multiplier']].drop_duplicates()
            
            df_rel_trade = df_rel_trade.merge(multiplier_df, 
                                              left_on = ['node_loc', 'node_rel', 'technology'],
                                              right_on = ['node_loc', 'node_rel', 'technology'], how = 'left')
            df_rel_trade['value'] = df_rel_trade['multiplier'] * -1 # Sum of this and shipping technology should be >0

            df_rel_flow['value'] = 1
            
        dfcol = ['node_loc', 'technology', 'node_rel', 'relation',
                 'year_rel', 'year_act', 'mode', 'value', 'unit']
        df_rel_trade = df_rel_trade[dfcol]
        df_rel_flow = df_rel_flow[dfcol]
        
        df_rel = pd.concat([df_rel_trade, df_rel_flow]).drop_duplicates()
        df_rel['unit'] = flow_unit
        
        df_rel.to_csv(os.path.join(data_path, tec, "edit_files", "flow_technology", "relation_activity_flow.csv"), index=False)
        log.info(f"Relation activity (flow) csv generated at: {os.path.join(data_path, tec)}.")

    ## Transfer files from edit to bare if they do not already exist
    for tec in covered_tec:
        required_parameters = [os.path.join("capacity_factor.csv"),
                               os.path.join("input.csv"),
                               os.path.join("output.csv"),
                               os.path.join("technical_lifetime.csv"),
                               os.path.join("flow_technology", "capacity_factor.csv"),
                               os.path.join("flow_technology", "input.csv"),
                               os.path.join("flow_technology", "output.csv"),
                               os.path.join("flow_technology", "relation_activity_flow.csv"),
                               os.path.join("flow_technology", "technical_lifetime.csv")]
        for reqpar in required_parameters:
            if not os.path.isfile(os.path.join(data_path, tec, "bare_files", reqpar)):
               base_file = os.path.basename(os.path.join(data_path, tec, "edit_files", reqpar))
               dest_file = os.path.join(data_path, tec, "bare_files", reqpar)
               shutil.copy2(base_file, dest_file)
               log.info(f"Copied file from edit to bare: {reqpar}")
        
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
    """
    # Load config
    config, config_path = load_config(project_name, config_name)

    covered_tec = config.get('covered_trade_technologies', {})
    trade_lifetimes = config.get('trade_lifetimes', {})
    
    outdict = dict()
    
    ya_list = config['timeframes']['year_act_list']
    yv_list = config['timeframes']['year_vtg_list']
    
    for tec in covered_tec:
        config_tec = config.get(tec + '_trade', {})
        
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
        tec_lt = trade_lifetimes[tec]
        
        for ty in ['trade', 'flow']:
            for i in data_dict[ty].keys():
                if "year_rel" in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_rel"].iloc[0] == "broadcast":
                        data_dict[ty][i] = broadcast_yl(data_dict[ty][i], ya_list)
                        data_dict[ty][i]["year_act"] = data_dict[ty][i]["year_rel"]
                else:
                    pass
                
                if "year_vtg" in data_dict[ty][i].columns and "year_act" in data_dict[ty][i].columns:
                    if (data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                        and data_dict[ty][i]["year_act"].iloc[0] == "broadcast"):
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for yv and ya.")
                        data_dict[ty][i] = broadcast_yv_ya(data_dict[ty][i], ya_list, tec_lifetime = tec_lt)
                    elif (data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                        and data_dict[ty][i]["year_act"].iloc[0] != "broadcast"):
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for yv.")
                        data_dict[ty][i] = broadcast_yv(data_dict[ty][i], ya_list)
                elif "year_vtg" in data_dict[ty][i].columns and "year_act" not in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for yv.")
                        data_dict[ty][i] = broadcast_yv(data_dict[ty][i], yv_list) 
                elif "year_vtg" not in data_dict[ty][i].columns and "year_act" in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_act"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {tec} {ty} broadcasted for ya.")
                        data_dict[ty][i] = broadcast_ya(data_dict[ty][i], ya_list)                    
                else:
                    pass

        # Imports do not vintage
        for par in ['capacity_factor', 'input', 'output']:
            vdf = data_dict['trade'][par]
            vdf = vdf[((vdf['technology'].str.contains('_imp')) & (vdf['year_vtg'] == vdf['year_act']))|\
                      (vdf['technology'].str.contains('_exp_'))]
            data_dict['trade'][par] = vdf
        
        # Generate relation lower bound for flow technologies
        df = data_dict['flow']['relation_activity_flow'].copy()
        df['value'] = 0
        data_dict['flow']['relation_lower_flow'] = df
        
        outdict[tec] = data_dict
    
    return outdict
#%% Clone and update scenario
def clone_and_update(trade_dict, 
                     log,
                     solve = False,
                     to_gdx = False,
                     project_name: str = None,
                     config_name: str = None,
                     gdx_location: str = os.path.join("H:", "script", "message_ix", "message_ix", "model", "data")):     
    # Load config
    config, config_path = load_config(project_name, config_name)
       
    # Load the scenario
    mp = ixmp.Platform()
    start_model = config.get("scenario", {}).get("start_model")
    start_scen = config.get("scenario", {}).get("start_scen")
     
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {config_path}")
        log.error(error_msg)
        raise ValueError(error_msg)
    
    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")
     
    # Clone scenario
    target_model = config.get("scenario", {}).get("target_model", [])
    target_scen = config.get("scenario", {}).get("target_scen", [])
    scen = base.clone(target_model, target_scen, keep_solution=False)
    scen.set_as_default()
    log.info("Scenario cloned.")
    
    # Add sets and parameters for each covered technology
    covered_tec = config.get('covered_trade_technologies')
    
    for tec in covered_tec:
        
        # Remove existing technologies related to trade
        base_tec = [config.get(tec + '_trade').get('trade_commodity') + '_exp', # These may not exist but in case they do...
                    config.get(tec + '_trade').get('trade_commodity') + '_imp']
        if tec == 'gas_piped':
            base_tec = base_tec + [i for i in scen.set('technology') if 
                                   config.get(tec + '_trade').get('trade_commodity') + '_exp_' in i]
        base_tec = list(set(base_tec))
        
        with scen.transact("Remove base trade technologies for " + tec):
            for t in base_tec:
                if t in list(scen.set('technology')):
                    log.info('Removing base technology...' + t)
                    scen.remove_set('technology', t)
                    
        # Add to sets: technology, level, commodity
        new_sets = dict()
        for s in ['technology', 'level', 'commodity']:
            setlist = set(list(trade_dict[tec]['trade']['input'][s].unique()) +\
                          list(trade_dict[tec]['trade']['output'][s].unique()))
                
            if "input" in trade_dict[tec]['flow'].keys():
                setlist = setlist.union(set(list(trade_dict[tec]['flow']['input'][s].unique()) +\
                                            list(trade_dict[tec]['flow']['output'][s].unique())))
            setlist = list(setlist)
            
            new_sets[s] = setlist
        
        with scen.transact("Add new sets for " + tec):
            for s in ['technology', 'level', 'commodity']:
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
        bunker_tec = config.get(tec + '_trade').get('bunker_technology')
        if bunker_tec != None:
            bunkerdf_in = scen.par('input', filters = {'technology': bunker_tec})
            bunkerdf_out = bunkerdf_in.copy()
            bunkerdf_out['level'] = 'bunker'
            with scen.transact('Update bunker fuel for' + tec):
                log.info('Updating bunker level for ' + tec)
                scen.remove_par('input', bunkerdf_in)
                scen.add_par('input', bunkerdf_out)
            
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
