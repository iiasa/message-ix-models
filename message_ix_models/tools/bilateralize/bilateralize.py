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

# Connect to ixmp
mp = ixmp.Platform()

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
log = get_logger(__name__)
#%% Load config yaml
def load_config(full_path):
    with open(full_path, "r") as f:
        config = yaml.safe_load(f) # safe_load is recommended over load for security
    
    return config
#%% Search for trade technology in base scenario. If exists, build template.
def get_template(scen_base, par_name, base_trade_technology):
    template = pd.DataFrame()
    template = (
        scen_base.par(par_name, filters={"technology": base_trade_technology})
        .head()
        .iloc[0]
        .to_frame()
        .T
    )
    if template.empty:
        log.warning(
            f"Technology {base_trade_technology} does not have {par_name} in {scen_base}."
        )
    return template
#%% Copy columns from template, if exists
def copy_template_columns(df, template, exclude_cols=["node_loc", "technology"]):
    for col in template.columns:
        if col not in exclude_cols:
            df[col] = template[col].iloc[0]
#%% Broadcast years to create vintage-activity year pairs.
def broadcast_yv_ya(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
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
            # Get all vintage years that are <= activity year
            yv_list = [yv for yv in ya_list if yv <= ya]

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
    return result_df
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
    return result_df
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
    config_name: str = None,
):
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
    supply_technologies = {}
    specify_network = {}
    trade_tech_number = {}
    
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
        supply_technologies[tec] = tec_dict['supply_technologies']
        specify_network[tec] = tec_dict['specify_network']
        trade_tech_number[tec] = tec_dict['trade_tech_number']
    # Load the scenario
    start_model = scenario_info['start_model']
    start_scen = scenario_info['start_scen']
    target_model = scenario_info['target_model']
    target_scen = scenario_info['target_scen']
    
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {full_path}"
        )
        log.error(error_msg)
        raise ValueError(error_msg)
        
    base_scenario = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    
    log.info(f"Loaded scenario: {start_model}/{start_scen}")
    
    # Generate folder for each trade technology
    for tec in covered_tec:
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)
        if not os.path.isdir(tecpath):
            os.makedirs(tecpath)
        if not os.path.isdir(os.path.join(tecpath, 'use_files')):
            os.makedirs(os.path.join(tecpath, 'use_files'))
            
    # Generate full combination of nodes to build technology-specific network
    nodes_base_scenario = base_scenario.set("node")
    
    node_set = {node for node in nodes_base_scenario
                if node.lower() != "world" and "glb" not in node.lower()} # Exclude GLB/World node
    
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
                    Path(package_data_path("bilateralize"))/("specify_network_" + tec + ".csv"))
            except FileNotFoundError:
                node_df_tec.to_csv(os.path.join(config_dir, "specify_network_" + tec + ".csv"), 
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
        
        network_setup[tec] = specify_network_tec

    # Create bare file: input
    for tec in covered_tec:
        template = get_template(base_scenario, "input", base_trade_technology[tec])
  
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
        df = df[template.columns]
        input_trade = df.copy()
        
        # Import Level
        df = network_setup[tec][['exporter', 'importer', 'import_technology']]
        df = df.rename(columns = {'exporter': 'node_origin',
                                  'importer': 'node_loc',
                                  'import_technology': 'technology'})
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = trade_level[tec]
        df["value"] = 1 # DOES NOT REQUIRE EDIT
        df['time'] = 'year'
        df['time_origin'] = 'year'
        df['unit'] = None
        df = df[template.columns]
        input_imports = df.copy()
    
        df = pd.concat([input_trade, input_imports])
        
        df.to_csv(os.path.join(config_dir, tec, "input.csv"), index=False)
        log.info(f"Input pipe exp csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: output
    for tec in covered_tec:
        template = get_template(base_scenario, "output", base_trade_technology[tec])
  
        # Trade Level
        df = network_setup[tec][['exporter', 'export_technology']]
        df = df.rename(columns = {'exporter': 'node_loc',
                                  'export_technology': 'technology'})
        df['node_dest'] = df['node_loc']
        df["year_vtg"] = "broadcast"
        df["year_act"] = "broadcast"
        df["mode"] = "M1"
        df["commodity"] = trade_commodity[tec]
        df["level"] = trade_level[tec]
        df["value"] = None
        df['time'] = 'year'
        df['time_dest'] = 'year'
        df['unit'] = None
        df = df[template.columns]
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
        df = df[template.columns]
        output_imports = df.copy()
    
        df = pd.concat([output_trade, output_imports])
        
        df.to_csv(os.path.join(config_dir, tec, "output.csv"), index=False)
        log.info(f"Output csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: technical_lifetime
    for tec in covered_tec: # TODO: Check on why import technologies do not have technical lifetimes in base
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'value': None,
                                                 'unit': 'y'})   
        outdf.to_csv(os.path.join(config_dir, tec, "technical_lifetime.csv"), index=False)
        log.info(f"Technical Lifetime csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: inv_cost
    for tec in covered_tec: #TODO: Imports do not have investment costs in global pool setup
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa'})   
        outdf.to_csv(os.path.join(config_dir, tec, "inv_cost.csv"), index=False)
        log.info(f"Investment cost csv generated at: {os.path.join(config_dir, tec)}.")        
      
    # Create base file: fix_cost
    for tec in covered_tec: #TODO: Global pool technologies do not have fixed costs in base scenario
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa'})   
        outdf.to_csv(os.path.join(config_dir, tec, "fix_cost.csv"), index=False)
        log.info(f"Fixed cost csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: var_cost
    for tec in covered_tec:
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': None,
                                                 'unit': 'USD/GWa'})   
        outdf.to_csv(os.path.join(config_dir, tec, "var_cost.csv"), index=False)
        log.info(f"Variable cost csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: capacity_factor (DOES NOT REQUIRE EDIT)
    for tec in covered_tec: 
        outdf =  build_parameterdf(network_df = network_setup[tec], 
                                   columndict = {'year_vtg': 'broadcast',
                                                 'year_act': 'broadcast',
                                                 'value': 1,
                                                 'unit': '%'})   
        outdf.to_csv(os.path.join(config_dir, tec, "capacity_factor.csv"), index=False) # Does not require edit
        log.info(f"Capacity factor csv generated at: {os.path.join(config_dir, tec)}.")

    # Create base file: Relation to aggregate exports so global level can be calculated/calibrated
    for tec in covered_tec:
        template = get_template(base_scenario, "relation_activity", base_trade_technology[tec])
  
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
        df = df[template.columns]
        df.to_csv(os.path.join(config_dir, tec, "relation_activity_global_aggregate.csv"), index=False)
        log.info(f"Relation activity (global aggregatin) csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: relation to link all pipelines/routes to an exporter
    for tec in covered_tec:
        template = get_template(base_scenario, "relation_activity", base_trade_technology[tec])
         
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
        df = df[template.columns]
        df.to_csv(os.path.join(config_dir, tec, "relation_activity_regionalexp.csv"), index=False)
        log.info(f"Relation activity (regional exports) csv generated at: {os.path.join(config_dir, tec)}.")
        
    # Create base file: relation to link all pipelines/routes to an importer
    for tec in covered_tec:
        template = get_template(base_scenario, "relation_activity", base_trade_technology[tec])
         
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
        df = df[template.columns]
        df.to_csv(os.path.join(config_dir, tec, "relation_activity_regionalimp.csv"), index=False)
        log.info(f"Relation activity (regional imports) csv generated at: {os.path.join(config_dir, tec)}.")
#%% Build out bare sheets
def build_parameter_sheets(config_name: str = None):
    """
    Read the input csv files and build the tech sets and parameters.

    Args:
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/inter_pipe/config.yaml
    """
    # Read all CSV files that do not contain "edit" in the name
    csv_files = [
        f
        for f in Path(package_data_path("inter_pipe")).glob("*.csv")
        if "edit" not in f.name
    ]
    data_dict = {}
    for csv_file in csv_files:
        key = csv_file.stem
        data_dict[key] = pd.read_csv(csv_file)

    # Load the config
    if config_name is None:
        config_name = "inter_pipe"
    full_path = package_data_path("inter_pipe", "config.yaml")
    config = load_config(full_path)
    log.info(f"Loading config from: {full_path}")

    # Generate par_list and set_list
    par_list = [
        "input",
        "output",
        "technical_lifetime",
        "inv_cost",
        "fix_cost",
        "var_cost",
        "capacity_factor",
        "relation_activity",
        "relation_upper",
        "relation_lower",
    ]
    set_list = [
        "technology",
        # "commodity",
        "level",
        "relation",
    ]

    # Broadcast the data
    ya_list = [
        2030,
        2035,
        2040,
        2045,
        2050,
        2055,
        2060,
        2070,
        2080,
        2090,
        2100,
        2110,
    ]  # TODO: get from config
    for i in data_dict.keys():
        if "year_rel" in data_dict[i].columns:
            if data_dict[i]["year_rel"].iloc[0] == "broadcast":
                data_dict[i] = broadcast_yl(data_dict[i], ya_list)
                data_dict[i]["year_act"] = data_dict[i]["year_rel"]
                log.info(f"Parameter {i} Broadcasted.")
        else:
            pass
        if "year_vtg" in data_dict[i].columns and "year_act" in data_dict[i].columns:
            if (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] == "broadcast"
            ):
                data_dict[i] = broadcast_yv_ya(data_dict[i], ya_list)
                log.info(f"Parameter {i} Broadcasted.")
            elif (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] != "broadcast"
            ):
                data_dict[i] = broadcast_yv(data_dict[i], ya_list)
                log.info(f"Parameter {i} Broadcasted.")
        else:
            pass

    # Generate relation upper and lower
    for i in [k for k in data_dict.keys() if "relation_activity" in k]:
        if i != "relation_activity_ori":
            key_name = i
            df = data_dict[i][["relation", "node_rel", "year_rel", "unit"]].assign(
                value=0
            )
            df = broadcast_yl(df, ya_list)
            key_name_upper = key_name.replace("activity", "upper")
            data_dict[key_name_upper] = df.copy()

            key_name_lower = key_name.replace("activity", "lower")
            data_dict[key_name_lower] = df.copy()
        else:
            pass

    # Load the scenario
    start_model = str(config.get("scenario", {}).get("start_model", [])[0])
    start_scen = str(config.get("scenario", {}).get("start_scen", [])[0])
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {full_path}"
        )
        log.error(error_msg)
        raise ValueError(error_msg)
    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")

    # Clone scenario
    target_model = str(config.get("scenario", {}).get("target_model", [])[0])
    target_scen = str(config.get("scenario", {}).get("target_scen", [])[0])
    scen = base.clone(target_model, target_scen, keep_solution=False)
    scen.set_as_default()
    log.info("Scenario cloned.")

    # Add set and parameter
    with scen.transact("Added"):
        for i in set_list:
            if i in data_dict:
                i_str = (
                    data_dict[i]
                    .apply(lambda row: row.astype(str).str.cat(sep=", "), axis=1)
                    .tolist()
                )  # str or list of str only
                scen.add_set(i, i_str)
                log.info(f"Set {i} added.")
            else:
                pass
        for i in par_list:
            # Find all keys in data_dict that contain the parameter name
            matching_keys = [k for k in data_dict.keys() if i in k]
            if matching_keys:
                # Combine all matching DataFrames
                combined_df = pd.concat(
                    [data_dict[k] for k in matching_keys], ignore_index=True
                )
                scen.add_par(i, combined_df)
                log.info(f"Parameter {i} from {matching_keys} added.")
            else:
                pass

    return scen
