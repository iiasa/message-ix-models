# -*- coding: utf-8 -*-
"""
Add friction sensitivities
"""
# Import packages
from typing import Any

import logging
import numpy as np
import pandas as pd
import ixmp
from ixmp import Platform
import message_ix

# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

import os

def friction_dictionary(sensitivity_scenario: str):

    # Import scenario and models
    config, config_path = load_config(project_name = 'alps_hhi', config_name = 'config.yaml')
    data_path = package_data_path("bilateralize")

    # Clear bare files
    print("Clearing bare files for " + sensitivity_scenario)
    for tec in config['covered_trade_technologies']:
        if os.path.exists(os.path.join(data_path, tec, "bare_files")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
                    if 'technical_lifetime' not in file:
                        os.remove(os.path.join(data_path, tec, "bare_files", file))
        if os.path.exists(os.path.join(data_path, tec, "bare_files", "flow_technology")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
                    os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))

    sens_i = config['sensitivities'][sensitivity_scenario]['exporters']
    sens_j = config['sensitivities'][sensitivity_scenario]['importers']
    sens_techs = config['sensitivities'][sensitivity_scenario]['technologies']

    for tec in sens_techs:
        sens_j_ref = [j.replace("R12_", "").lower() for j in sens_j]
        tec_list = [tec + '_exp_' + j for j in sens_j_ref]

        base_var_cost = pd.read_csv(os.path.join(data_path, tec, "edit_files", "var_cost.csv"))
        base_var_cost = base_var_cost[base_var_cost['node_loc'].isin(sens_i)]
        base_var_cost = base_var_cost[base_var_cost['technology'].isin(tec_list)]

        base_var_cost['value'] = 100

        base_var_cost.to_csv(os.path.join(data_path, tec, "bare_files", "var_cost.csv"), index=False)
    
    trade_dict_sens = bare_to_scenario(project_name = 'alps_hhi', 
                                       config_name = 'config.yaml',
                                       p_drive_access = False)
    
    return trade_dict_sens

def run_friction_scenario(base_scenario_name: str,
                          sensitivity_scenario: str):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'alps_hhi', config_name = 'config.yaml')

    # Build dictionary
    trade_dict_sens = friction_dictionary(sensitivity_scenario)

    mp = ixmp.Platform()

    base_scenario = message_ix.Scenario(mp, model = 'alps_hhi', scenario = base_scenario_name)
    target_scenario = base_scenario.clone('alps_hhi', base_scenario_name + '_' + sensitivity_scenario, keep_solution = False)
    target_scenario.set_as_default()

    for tec in config['sensitivities'][sensitivity_scenario]['technologies']:
        with target_scenario.transact(f"Add friction sensitivity for {tec}"):
            target_scenario.add_par('var_cost', trade_dict_sens[tec]['trade']['var_cost'])

    if "HC" in base_scenario_name:
        target_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)
    elif "WS" in base_scenario_name:
        target_scenario.solve(gams_args = ['--HHI_WS=1'], quiet = False)
    else:
        target_scenario.solve(quiet = False)


