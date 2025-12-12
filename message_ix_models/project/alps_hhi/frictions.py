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
from itertools import product

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

    bound_out = pd.DataFrame()
    for tec in sens_techs:
        sens_j_ref = [j.replace("R12_", "").lower() for j in sens_j]
        tec_list = [tec + '_exp_' + j for j in sens_j_ref]

        basedf = pd.DataFrame(product(sens_i, tec_list,
                                  [2030, 2035, 2040, 2045, 2050, 2055,
                                   2060, 2070, 2080, 2090, 2100],
                                  ["M1"],
                                  ["year"]))
        basedf.columns = ['node_loc', 'technology', 'year_act', 'mode', 'time']

        bounddf = message_ix.make_df(
            "bound_activity_up",
              node_loc = basedf['node_loc'],
              technology = basedf['technology'],
              value = 0,
              year_act = basedf['year_act'],
              mode = basedf['mode'],
              time = basedf['time'],
              unit = '-')

        bound_out = pd.concat([bound_out, bounddf])
        
    return bound_out

def run_friction_scenario(base_scenario_name: str,
                          sensitivity_scenario: str):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'alps_hhi', config_name = 'config.yaml')

    # Build dictionary
    bound_out = friction_dictionary(sensitivity_scenario)

    mp = ixmp.Platform()

    base_scenario = message_ix.Scenario(mp, model = 'alps_hhi', scenario = base_scenario_name)
    target_scenario = base_scenario.clone('alps_hhi', base_scenario_name + '_' + sensitivity_scenario, keep_solution = False)
    target_scenario.set_as_default()

    with target_scenario.transact(f"Add friction sensitivity"):
        target_scenario.add_par('bound_activity_up', bound_out)

    with target_scenario.transact("Remove constraints on shocked technologies"):
        for par in ["growth_activity_lo", "growth_activity_up", "initial_activity_lo", "initial_activity_up"]:
            basepar = target_scenario.par(par, filters = {"technology": bound_out['technology'],
                                                          "node_loc": bound_out['node_loc']})
            if len(basepar) != 0:
                print(f"...{par}")
                target_scenario.remove_par(par, basepar)

    if "HC" in base_scenario_name:
        target_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)
    elif "WS" in base_scenario_name:
        target_scenario.solve(gams_args = ['--HHI_WS=1'], quiet = False)
    else:
        target_scenario.solve(quiet = False)

for alps_base in ['SSP2', 'SSP2_hhi_HC_supply', 'SSP2_hhi_HC_imports', 'SSP2_hhi_WS_l90p_supply', 'SSP2_hhi_WS_l90p_imports']:
    run_friction_scenario(alps_base, 'FSU_EUR_frictions')

