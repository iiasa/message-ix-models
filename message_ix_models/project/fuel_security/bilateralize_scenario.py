# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.fuel_security.liquefaction_calibration import *
from message_ix_models.project.fuel_security.adjust_reexports import *

import os
from ixmp import Platform

# Clear bare files
def clean_bare_files(data_path):
    """
    Clear bare files for all technologies
    """
    for tec in config['covered_trade_technologies']:
        if os.path.exists(os.path.join(data_path, tec, "bare_files")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
                os.remove(os.path.join(data_path, tec, "bare_files", file))
        if os.path.exists(os.path.join(data_path, tec, "bare_files", "flow_technology")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
                    os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))

# Add scenario updates for project
def add_scenario_updates(project_name, config_name, data_path):
    """
    Add scenario updates for project
    """
    print("Add scenario updates for project")
    config = load_config(project_name = project_name, config_name = config_name)
    for tec in config['constrained_tec']:
        print(f"...{tec}")
        if os.path.exists(package_data_path(project_name, "scenario_updates", tec)):
            for file in os.listdir(package_data_path(project_name, "scenario_updates", tec)):
                base_file = package_data_path(project_name, "scenario_updates", tec, file)
                if ".csv" in str(base_file):
                    dest_file = os.path.join(data_path, tec, "bare_files", file)
                    shutil.copy2(base_file, dest_file)
                    print(f"Copied file from scenario_updates to bare: {file}")

def bilateralize_scenario(project_name, config_name, scenario):
    """
    Bilateralize a given scenario
    """
    # Load config
    config, config_name = load_config(project_name = project_name, config_name = config_name)
    data_path = package_data_path("bilateralize")

    # Clear bare files
    clean_bare_files(data_path)

    # Prepare edit files (parameters)
    prepare_edit_files(project_name = project_name, 
                       config_name = config_name,
                       P_access = True)
    
    # Add scenario updates for project
    add_scenario_updates(project_name = project_name,
                         config_name = config_name,
                         data_path = data_path)
    
    # Move data from bare files to a dictionary to update a MESSAGEix scenario
    trade_dict = bare_to_scenario(project_name = project_name, 
                                  config_name = config_name,
                                  p_drive_access = True)

    # Additional liquefaction calibration
    liquefaction_parameters = update_liquefaction_input(message_regions = "R12",
                                                        project_name = project_name,
                                                        config_name = config_name)

    # Clone and set up base scenario
    print(f"Base model: {scenario.model}/{scenario.scenario}")
    print(f"Target model: fuel_security/{scenario.scenario}")

    print("Setting up scenario")
    load_and_solve(trade_dict = trade_dict,
                   solve = False,
                   project_name = project_name, 
                   config_name = config_name, 
                   start_model = scenario.model,
                   start_scen = scenario.scenario,
                   target_model = project_name,
                   target_scen = scenario.scenario,
                   extra_parameter_updates = liquefaction_parameters)

    # Update extraction constraints
    print("Updating extraction constraints")
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model=project_name, scenario=scenario.scenario)
    out_scenario = base_scenario.clone(project_name, scenario.scenario)
    out_scenario.set_as_default()

    for g in ['growth_activity_up']:
        updf = out_scenario.par(g)
        updf = updf[(updf['technology'].str.contains('gas_extr_mpen'))]
        updf = updf[updf['node_loc'].isin(['R12_WEU'])]
    
        remdf = updf.copy()
        if g == 'growth_activity_up':
            updf['value'] = 0.01
        elif g == 'growth_activity_lo':
            updf['value'] = -0.01
            
        with out_scenario.transact("update growth activity to gas_extr_mpen"):
            out_scenario.remove_par(g, remdf)
            out_scenario.add_par(g, updf)

    # Add balance equality sets
    print("Add balance equality sets")
    be_df = out_scenario.par("output", filters = {"technology": config['covered_trade_technologies']})
    be_df = be_df[be_df['level'].isin(['piped', 'shipped'])]
    be_df = be_df[['commodity', 'level']].drop_duplicates()

    with out_scenario.transact("add balance equality sets"):
        out_scenario.add_set("balance_equality", be_df)

    # Adjust re-exports for lightoil and fueloil
    print("Adjust re-exports for lightoil and fueloil")
    adjust_reexports(base_scenario = out_scenario,
                     trade_commodity_list = ['lightoil', 'fueloil'],
                     base_level = 'secondary')

    print("Solve scenario")
    out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})
    mp.close_db()