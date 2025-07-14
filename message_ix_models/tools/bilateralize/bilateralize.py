# -*- coding: utf-8 -*-
"""
Bilateralize trade flows
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
def load_config(full_path):
    with open(full_path, "r") as f:
        config = yaml.safe_load(f) # safe_load is recommended over load for security
    
    return config
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
        network_df: pd.DataFrame,
        columndict: dict):
    
    df = network_df[['exporter', 'export_technology']].drop_duplicates()
    df = df.rename(columns = {'exporter': 'node_loc',
                              'export_technology': 'technology'})
    for c in columndict.keys():
        df[c] = columndict[c]
    
    export_df = df.copy()
    
    df = network_df[['importer', 'import_technology']].drop_duplicates()
    df = df.rename(columns = {'importer': 'node_loc',
                              'import_technology': 'technology'})
    for c in columndict.keys():
        df[c] = columndict[c]
        
    import_df = df.copy() 
    
    df = pd.concat([export_df, import_df]).reset_index(drop = True)
    
    return df
#%% Main function to generate bare sheets
def generate_bare_sheets(
        log, mp,
        config_name: str = None,
        message_regions: str = 'R12'):
    """
    Generate bare sheets to collect (minimum) parameters

    Args:
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config.yaml
    """
    # Load config
    if config_name is None:
        config_name = "config.yaml"
        
    full_path = package_data_path("bilateralize", config_name)
    
    config_dir = os.path.dirname(full_path)
    config = load_config(full_path)
    log.info(f"Loading config from: {full_path}")

    # Retrieve config sections
    scenario_info = config.get('scenario', {})
    
    define_parameters = config.get('define_parameters', {})
    
    covered_tec = config.get('covered_trade_technologies', {})
    
    base_trade_technology = {}
    trade_technology = {}
    trade_tech_suffix = {}
    trade_commodity = {}
    trade_commodity_suffix = {}
    trade_form = {}
    export_level = {}
    trade_level = {}
    import_level = {}
    flow_technology = {}
    flow_tech_suffix = {}
    flow_constraint = {}
    flow_commodity_input = {}
    flow_commodity_output = {}
    supply_technologies = {}
    specify_network = {}
    trade_tech_number = {}
    tracked_emissions = {}
    
    for tec in covered_tec:
        tec_dict = config.get(tec + '_trade', {})
        
        base_trade_technology[tec] = tec_dict['base_trade_technology']
        trade_technology[tec] = tec_dict['trade_technology']
        trade_tech_suffix[tec] = tec_dict['trade_tech_suffix']
        trade_commodity[tec] = tec_dict['trade_commodity']
        trade_commodity_suffix[tec] = tec_dict['trade_commodity_suffix']
        trade_form[tec] = tec_dict['trade_form']
        export_level[tec] = tec_dict['export_level']
        trade_level[tec] = tec_dict['trade_level']
        import_level[tec] = tec_dict['import_level']
        flow_technology[tec] = tec_dict['flow_technology']
        flow_tech_suffix[tec] = tec_dict['flow_tech_suffix']
        flow_commodity_input[tec] = tec_dict['flow_commodity_input']
        flow_commodity_output[tec] = tec_dict['flow_commodity_output']
        flow_constraint[tec] = tec_dict['flow_constraint']
        supply_technologies[tec] = tec_dict['supply_technologies']
        specify_network[tec] = tec_dict['specify_network']
        trade_tech_number[tec] = tec_dict['trade_tech_number']
        tracked_emissions[tec] = tec_dict['tracked_emissions']
        
    # Load the scenario
    start_model = scenario_info['start_model']
    start_scen = scenario_info['start_scen']
    target_model = scenario_info['target_model']
    target_scen = scenario_info['target_scen']
    
    # if not start_model or not start_scen:
    #     error_msg = (
    #         "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
    #         f"Please check the config file at: {full_path}"
    #     )
    #     log.error(error_msg)
    #     raise ValueError(error_msg)
        
   # base_scenario = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    
    log.info(f"Loaded scenario: {start_model}/{start_scen}")
    
    # Generate folder for each trade technology
    for tec in covered_tec:
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)
        if not os.path.isdir(tecpath):
            os.makedirs(tecpath)
        if not os.path.isdir(os.path.join(tecpath, 'edit_files')):
            os.makedirs(os.path.join(tecpath, 'edit_files'))   
        if not os.path.isdir(os.path.join(tecpath, 'edit_files', 'flow_technology')):
            os.makedirs(os.path.join(tecpath, 'edit_files', 'flow_technology'))
        if not os.path.isdir(os.path.join(tecpath, 'bare_files')):
            os.makedirs(os.path.join(tecpath, 'bare_files'))
        if not os.path.isdir(os.path.join(tecpath, 'bare_files', 'flow_technology')):
            os.makedirs(os.path.join(tecpath, 'bare_files', 'flow_technology'))    
            
    # Generate full combination of nodes to build technology-specific network
    node_path = package_data_path("bilateralize", message_regions + "_node_list.yaml")
    with open(node_path, "r") as f:
        node_set = yaml.safe_load(f) 
    node_set = [r for r in node_set.keys() if r not in ['World', 'GLB']]

    # nodes_base_scenario = base_scenario.set("node")
    
    # node_set = {node for node in nodes_base_scenario
    #             if node.lower() != "world" and "glb" not in node.lower()} # Exclude GLB/World node
    
    node_df = pd.DataFrame(itertools.product(node_set, node_set))
    node_df.columns = ['exporter', 'importer']
    node_df = node_df[node_df['exporter'] != node_df['importer']]
    
    network_setup = {} # Dictionary for each covered technology
    for tec in covered_tec:
        
        # Specify network if needed
        node_df_tec = node_df.copy()
        
        node_df_tec['export_technology'] = trade_technology[tec] + '_exp'
        if trade_tech_suffix[tec] != None: 
            node_df_tec['export_technology'] = node_df_tec['export_technology'] + '_' + trade_tech_suffix[tec]
        node_df_tec['export_technology'] = node_df_tec['export_technology'] + '_' +\
                                           node_df_tec['importer'].str.lower().str.split('_').str[1]
        if (trade_tech_number[tec] != None) & (trade_tech_number[tec] != 1): 
            ndt_out = pd.DataFrame()
            for i in list(range(1, trade_tech_number[tec] + 1)):
                ndt = node_df_tec.copy()
                ndt['export_technology'] = ndt['export_technology'] + '_' + str(i)
                ndt_out = pd.concat([ndt_out, ndt])
            node_df_tec = ndt_out.copy()
            
        node_df_tec['import_technology'] = trade_technology[tec] + '_imp'
        node_df_tec['INCLUDE? (No=0, Yes=1)'] = ''
        
        if specify_network[tec] == True:
            try:
                specify_network_tec = pd.read_csv(
                    os.path.join(config_dir, tec, "specify_network_" + tec + ".csv"))
            except FileNotFoundError:
                node_df_tec.to_csv(os.path.join(config_dir, tec, "specify_network_" + tec + ".csv"), 
                                   index=False)
                raise Exception(
                    "The function stopped. Sheet specify_network_" + tec + ".csv has been generated. " +
                    "Fill in the specific pairs first and run again.")
            if any(specify_network_tec['INCLUDE? (No=0, Yes=1)'].notnull()) == False:
                raise Exception(
                    "The function stopped. Ensure that all values under 'INCLUDE? (No=0, Yes=1)' are filled")
                
        elif specify_network[tec] == False:
            specify_network_tec = node_df_tec.copy()
            specify_network_tec['INCLUDE? (No=0, Yes=1)'] = 1
   
        else:
            raise Exception("Please use True or False.")
        
        network_setup[tec] = specify_network_tec[specify_network_tec['INCLUDE? (No=0, Yes=1)'] == 1]

    # Create bare file: input
    for tec in covered_tec:  
        # Trade Level
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_origin',
                                  'export_technology': 'technology'})
        df['node_loc'] = df['node_origin']
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = export_level[tec]
        df["value"] = None
        df['time'] = 'year'
        df['time_origin'] = 'year'
        df['unit'] = None
        df = df[['node_origin', 'node_loc', 'technology', 'year_vtg', 'year_act',
                 'mode', 'commodity', 'level', 'value', 'time', 'time_origin', 'unit']]
        input_trade = df.copy()
        
        # Import Level
        df = network_setup[tec][['exporter', 'export_technology', 'importer', 'import_technology']]
        df = df.rename(columns = {'importer': 'node_loc',
                                  'import_technology': 'technology'})
        df["node_origin"] = df["node_loc"]
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = trade_level[tec]
        df["value"] = None
        df['time'] = 'year'
        df['time_origin'] = 'year'
        df['unit'] = None
        df = df[['node_origin', 'node_loc', 'technology', 'year_vtg', 'year_act',
                 'mode', 'commodity', 'level', 'value', 'time', 'time_origin', 'unit']]
        input_imports = df.copy()
    
        df = pd.concat([input_trade, input_imports])
        
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "input.csv"), index=False)
        log.info(f"Input pipe exp csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: output
    for tec in covered_tec:  
        # Trade Level
        df = network_setup[tec][['exporter', 'importer', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'importer': 'node_dest',
                                  'export_technology': 'technology'})
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = trade_level[tec]
        df["value"] = None
        df['time'] = 'year'
        df['time_dest'] = 'year'
        df['unit'] = None
        output_trade = df.copy()
        
        # Import Level
        df = network_setup[tec][['importer', 'import_technology']]
        df = df.rename(columns = {'importer': 'node_loc',
                                  'import_technology': 'technology'})
        df["node_dest"] = df['node_loc']
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = import_level[tec]
        df["value"] = 1 # DOES NOT REQUIRE EDIT
        df['time'] = 'year'
        df['time_dest'] = 'year'
        df['unit'] = None
        output_imports = df.copy()
    
        df = pd.concat([output_trade, output_imports])
        
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "output.csv"), index=False)
        log.info(f"Output csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: technical_lifetime
    for tec in covered_tec: # TODO: Check on why import technologies do not have technical lifetimes in base
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'value': None,
                                                 'unit': 'y'})   
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "technical_lifetime.csv"), index=False)
        log.info(f"Technical Lifetime csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: inv_cost
    for tec in covered_tec: #TODO: Imports do not have investment costs in global pool setup
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa'})
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "inv_cost.csv"), index=False)
        log.info(f"Investment cost csv generated at: {os.path.join(config_dir, tec)}.")        
      
    # Create base file: fix_cost
    for tec in covered_tec: #TODO: Global pool technologies do not have fixed costs in base scenario
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa'})   
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "fix_cost.csv"), index=False)
        log.info(f"Fixed cost csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: var_cost
    for tec in covered_tec:
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa',
                                                 'mode': 'M1',
                                                 'time': 'year'})   
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "var_cost.csv"), index=False)
        log.info(f"Variable cost csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: historical activity
    for tec in covered_tec:
        outdf = pd.DataFrame()
        for y in list(range(2000, 2025, 5)):
            ydf =  build_parameterdf(network_df = network_setup[tec], 
                                     columndict = {'year_act': y,
                                                   'value': None,
                                                   'unit': 'GWa',
                                                   'mode': 'M1',
                                                   'time': 'year'})   
            outdf = pd.concat([outdf, ydf])
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "historical_activity.csv"), index=False)
        log.info(f"Historical activity csv generated at: {os.path.join(config_dir, tec)}.")


    # Create base file: capacity_factor (DOES NOT REQUIRE EDIT)
    for tec in covered_tec: 
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': 1,
                                                 'unit': '%',
                                                 'time': 'year'})   
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "capacity_factor.csv"), index=False) # Does not require edit
        log.info(f"Capacity factor csv generated at: {os.path.join(config_dir, tec)}.")
    
    # Create base file: initial activity
    for t in ['lo', 'up']:
        for tec in covered_tec: 
            outdf =  build_parameterdf(network_df = network_setup[tec], 
                                       columndict = {'year_act': 'broadcast',
                                                     'value': 2,
                                                     'unit': 'GWa',
                                                     'time': 'year'})   
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "initial_activity_" + t + ".csv"), index=False) # Does not require edit
            log.info(f"Initial activity (" + t + ") csv generated at: {os.path.join(config_dir, tec)}.")
            
    # Create base file: abs_cost_activity_soft_up/lo
    for t in ['lo', 'up']:
        for tec in covered_tec: 
            outdf =  build_parameterdf(network_df = network_setup[tec], 
                                       columndict = {'year_act': 'broadcast',
                                                     'value': None,
                                                     'unit': 'GWa',
                                                     'time': 'year'})   
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "abs_cost_activity_soft_" + t + ".csv"), index=False) # Does not require edit
            log.info(f"Soft activity cost constraints (" + t + ") csv generated at: {os.path.join(config_dir, tec)}.")    

    # Create base file: emission_factor
    if tracked_emissions[tec] != None:
        for tec in covered_tec: 
            outdf = pd.DataFrame()
            for emission_type in tracked_emissions[tec]: 
                edf =  build_parameterdf(network_df = network_setup[tec], 
                                         columndict = {'year_vtg': 'broadcast',
                                                       'year_act': 'broadcast',
                                                       'value': None,
                                                       'unit': None,
                                                       'emission': emission_type,
                                                       'mode': 'M1'})
                outdf = pd.concat([outdf, edf])
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "emission_factor.csv"), index=False) # Does not require edit
            log.info(f"Emission factor csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: growth_activity_up/lo
    for t in ['lo', 'up']:
        for tec in covered_tec: 
            outdf =  build_parameterdf(network_df = network_setup[tec], 
                                       columndict = {'year_act': 'broadcast',
                                                     'value': None,
                                                     'unit': 'GWa',
                                                     'time': 'year'})   
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "growth_activity_" + t + ".csv"), index=False) # Does not require edit
            log.info(f"Growth activity constraints (" + t + ") csv generated at: {os.path.join(config_dir, tec)}.")    
            
    # Create base file: level_cost_activity_up/lo
    for t in ['lo', 'up']:
        for tec in covered_tec: 
            outdf =  build_parameterdf(network_df = network_setup[tec], 
                                       columndict = {'year_act': 'broadcast',
                                                     'value': None,
                                                     'unit': 'GWa',
                                                     'time': 'year'})   
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "level_cost_activity_soft_" + t + ".csv"), index=False) # Does not require edit
            log.info(f"Levelized cost activity constraints (" + t + ") csv generated at: {os.path.join(config_dir, tec)}.")    

    # Create base file: soft_activity_up/lo
    for t in ['lo', 'up']:
        for tec in covered_tec: 
            outdf =  build_parameterdf(network_df = network_setup[tec], 
                                       columndict = {'year_act': 'broadcast',
                                                     'value': None,
                                                     'unit': 'GWa',
                                                     'time': 'year'})   
            outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "soft_activity_" + t + ".csv"), index=False) # Does not require edit
            log.info(f"Soft activity constraints (" + t + ") csv generated at: {os.path.join(config_dir, tec)}.")    
    
    # Replicate base file: For gas- imports require relation to domestic_coal and domestic_gas
    for tec in covered_tec:  
        if tec in ['gas_piped']:
            for r in ['domestic_coal', 'domestic_gas']:
                df = network_setup[tec][['importer', 'import_technology']]
                df = df.rename(columns = {'importer': 'node_loc',
                                          'import_technology': 'technology'})
                df['node_rel'] = df['node_loc']
                df["year_rel"] = "broadcast"
                df["year_act"] = "broadcast"
                df["mode"] = "M1"
                df["commodity"] = trade_commodity[tec]
                df["value"] = -1
                df['unit'] = '???'
                df['relation'] = r
                df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_" + r + ".csv"), index=False)
                log.info(f"Relation activity (" + r + ") csv generated at: {os.path.join(config_dir, tec)}.")
                   
    # Replicate base file: Relation for CO2 emissions accounting
    for tec in covered_tec:  
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        df['node_rel'] = df['node_loc']
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = None
        df['unit'] = '???'
        df['relation'] = 'CO2_Emission'
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_CO2_emission.csv"), index=False)
        log.info(f"Relation activity (global aggregatin) csv generated at: {os.path.join(config_dir, tec)}.")
    
    # Replicate base file: Relation for primary energy total accounting
    for tec in covered_tec:  
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        df['node_rel'] = df['node_loc']
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = -1
        df['unit'] = '???'
        df['relation'] = 'PE_total_traditional'
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_PE_total_traditional.csv"), index=False)
        log.info(f"Relation activity (global aggregatin) csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: Relation to aggregate exports so global level can be calculated/calibrated
    for tec in covered_tec:  
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        df['node_rel'] = df['node_loc']
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = 1
        df['unit'] = '???'
        df['relation'] = trade_technology[tec] + '_exp_global'
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_global_aggregate.csv"), index=False)
        log.info(f"Relation activity (global aggregatin) csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: relation to link all pipelines/routes to an exporter
    for tec in covered_tec:         
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        df['node_rel'] = df['node_loc']
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = 1
        df['unit'] = '???'
        df['relation'] = trade_technology[tec] + '_exp_from_' + df['node_loc'].str.lower().str.split('_').str[1]
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_regionalexp.csv"), index=False)
        log.info(f"Relation activity (regional exports) csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: relation to link all pipelines/routes to an importer
    for tec in covered_tec:         
        df = network_setup[tec][['importer', 'import_technology']]
        df = df.rename(columns = {'importer': 'node_loc',
                                  'import_technology': 'technology'})
        df['node_rel'] = df['node_loc']
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = 1
        df['unit'] = '???'
        df['relation'] = trade_technology[tec] + '_imp_to_' + df['node_loc'].str.lower().str.split('_').str[1]
        df.to_csv(os.path.join(config_dir, tec, "edit_files", "relation_activity_regionalimp.csv"), index=False)
        log.info(f"Relation activity (regional imports) csv generated at: {os.path.join(config_dir, tec)}.")
    
    ## FLOW TECHNOLOGY

    # Create bare file: input
    for tec in covered_tec:  
        if flow_tech_suffix[tec] != None:
            full_flow_tec = tec + '_' + flow_technology[tec] + '_' + flow_tech_suffix[tec]
        else:
            full_flow_tec = tec + '_' + flow_technology[tec]
            
        df = network_setup[tec][['exporter', 'importer']].copy()
        
        if flow_constraint[tec] == 'bilateral':
            df['technology'] = full_flow_tec + '_' + df['importer'].str.lower().str.split('_').str[1]
        elif flow_constraint[tec] == 'global':
            df['technology'] = full_flow_tec + '_glb'

        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["level"] = None
        df["value"] = None
        df['time'] = 'year'
        df['time_origin'] = 'year'
        df['time_dest'] = 'year'
        df['unit'] = None
        
        input_df = pd.DataFrame()
        for c in flow_commodity_input[tec]:
            cdf = df.copy()
            cdf['commodity'] = c
            input_df = pd.concat([input_df, cdf])
        
        input_df['node_loc'] = input_df['exporter']
        input_df['node_origin'] = input_df['exporter']
        
        input_df = input_df[['node_origin', 'node_loc', 'technology', 'year_vtg', 'year_act',
                             'mode', 'commodity', 'level', 'value', 'time', 'time_origin', 'unit']]

        input_df.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "input.csv"), index=False)
        log.info(f"Input flow csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: output
        output_df = df.copy()
        output_df['commodity'] = flow_commodity_output[tec]
        output_df['node_loc'] = output_df['exporter']
        output_df['node_dest'] = output_df['importer']
        
        output_df = output_df[['node_loc', 'node_dest', 'technology', 'year_vtg', 'year_act',
                              'mode', 'commodity', 'level', 'value', 'time', 'time_dest', 'unit']]
        
        output_df.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "output.csv"), index=False)
        log.info(f"Output csv generated at: {os.path.join(config_dir, tec)}.")
    
    # Create base file: investment cost for flow technology
        inv_df =  build_parameterdf(network_df = network_setup[tec], 
                                    columndict = {'year_vtg': 'broadcast',
                                                  'value': None,
                                                  'unit': 'USD/km'})
        inv_df = inv_df[inv_df['technology'] != tec + '_imp']
        inv_df['technology'] =  full_flow_tec + '_' + inv_df['technology'].str.lower().str.split('_').str[-1]
        
        inv_df.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "inv_cost.csv"), index=False)
        log.info(f"Investment cost csv generated at: {os.path.join(config_dir, tec)}.")  
    
    # Create base file: capacity factor for flow technology
        cfdf =  build_parameterdf(network_df = network_setup[tec], 
                                  columndict = {'year_vtg': 'broadcast',
                                                'year_act': 'broadcast',
                                                'value': 1,
                                                'unit': '%',
                                                'time': 'year'})   
        cfdf = cfdf[cfdf['technology'] != tec + '_imp']
        cfdf['technology'] =  full_flow_tec + '_' + cfdf['technology'].str.lower().str.split('_').str[-1]
        
        cfdf.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "capacity_factor.csv"), index=False) # Does not require edit
        log.info(f"Capacity factor csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: technical lifetime for flow technolgoy
        tecdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'value': None,
                                                 'unit': 'y'})  
        tecdf = tecdf[tecdf['technology'] != tec + '_imp']
        tecdf['technology'] =  full_flow_tec + '_' + tecdf['technology'].str.lower().str.split('_').str[-1]
        
        tecdf.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "technical_lifetime.csv"), index=False)
        log.info(f"Technical Lifetime csv generated at: {os.path.join(config_dir, tec)}.")
      
    # Create relation to link exports to the flow technology       
        df = network_setup[tec][['exporter', 'importer', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        
        df["year_rel"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["value"] = None
        df['unit'] = None
        
        if flow_constraint[tec] == 'bilateral':
           # df['node_rel'] = df['importer']
            df['relation'] = full_flow_tec + '_' + df['importer'].str.lower().str.split('_').str[1]
        elif flow_constraint[tec] == 'global': #TODO: Test global
           # df['node_rel'] = message_regions + '_GLB'
            df['relation'] = full_flow_tec + '_GLB'
        
        df['node_rel'] = df['node_loc']
        
        trade_df = df.copy()
        flow_df = df.copy()
        
        if flow_constraint[tec] == 'bilateral':
            flow_df['technology'] = full_flow_tec + '_' + flow_df['importer'].str.lower().str.split('_').str[1]
        elif flow_constraint[tec] == 'global': #TODO: Test global            
            flow_df['technology'] = full_flow_tec
        
        dfcol = ['node_loc', 'technology', 'node_rel', 'relation',
                 'year_rel', 'year_act', 'mode', 'commodity', 'value', 'unit']
        trade_df = trade_df[dfcol]
        flow_df = flow_df[dfcol]
        
        outdf = pd.concat([trade_df, flow_df])
        
        outdf.to_csv(os.path.join(config_dir, tec, "edit_files", "flow_technology", "relation_activity_flow.csv"), index=False)
        
        log.info(f"Relation activity (flow) csv generated at: {os.path.join(config_dir, tec)}.")
#%% Build out bare sheets
def build_parameter_sheets(log, config_name: str = None):
    """
    Read the input csv files and build the tech sets and parameters.

    Args:
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config.yaml
    """
    # Load config
    if config_name is None:
        config_name = "config.yaml"
        
    full_path = package_data_path("bilateralize", config_name)
    
    config_dir = os.path.dirname(full_path)
    config = load_config(full_path)
    
    covered_tec = config.get('covered_trade_technologies', {})
    trade_lifetimes = config.get('trade_lifetimes', {})
    
    outdict = dict()
    
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
        ya_list = config_tec.get('year_act_list', [])
        yv_list = config_tec.get('year_vtg_list', [])
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
                        log.info(f"Parameter {i} in {ty} broadcasted for yv and ya.")
                        data_dict[ty][i] = broadcast_yv_ya(data_dict[ty][i], ya_list, tec_lifetime = tec_lt)
                    elif (data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                        and data_dict[ty][i]["year_act"].iloc[0] != "broadcast"):
                        log.info(f"Parameter {i} in {ty} broadcasted for yv.")
                        data_dict[ty][i] = broadcast_yv(data_dict[ty][i], ya_list)
                elif "year_vtg" in data_dict[ty][i].columns and "year_act" not in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {ty} broadcasted for yv.")
                        data_dict[ty][i] = broadcast_yv(data_dict[ty][i], yv_list) 
                elif "year_vtg" not in data_dict[ty][i].columns and "year_act" in data_dict[ty][i].columns:
                    if data_dict[ty][i]["year_act"].iloc[0] == "broadcast":
                        log.info(f"Parameter {i} in {ty} broadcasted for ya.")
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
                     log, mp, 
                     solve = False,
                     to_gdx = False,
                     config_name: str = None,
                     gdx_location: str = os.path.join("H:", "script", "message_ix", "message_ix", "model", "data")):     
    # Load config
    if config_name is None:
        config_name = "config.yaml"
    full_path = package_data_path("bilateralize", config_name)
    config_dir = os.path.dirname(full_path)
    config = load_config(full_path)
       
    # Load the scenario
    mp = ixmp.Platform()
    start_model = config.get("scenario", {}).get("start_model")
    start_scen = config.get("scenario", {}).get("start_scen")
     
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {full_path}")
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
