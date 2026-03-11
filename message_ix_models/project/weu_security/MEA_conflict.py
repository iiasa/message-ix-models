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

def conflict_dictionary(snesitivity_scenario:str):
    
def friction_dictionary(sensitivity_scenario: str):

    # Import scenario and models
    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')
    data_path = package_data_path("bilateralize")

    sens_i = config['mea_conflict'][sensitivity_scenario]['exporters']
    sens_j = config['mea_conflict'][sensitivity_scenario]['importers']
    sens_techs_in = config['mea_conflict'][sensitivity_scenario]['technologies']
    sens_techs = []
    for j in sens_j:
        jt = j.str.replace("R12_", "")
        jt = jt.str.lower()
        sens_techs_t = [s + f"_{jt}" for s in sens_tech_in]
        sens_techs = sens_techs + sens_techs_t

    base_years = [2030, 2035]
    
    bound_out = pd.DataFrame()
    for tec in sens_techs:
        tec_list = sens_techs
        
        basedf = pd.DataFrame(product(sens_i, tec_list,
                                  fric_years,
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
                          sensitivity_scenario: str,
                          solve_scenario = True):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')

    # Build dictionary
    bound_out = friction_dictionary(sensitivity_scenario)

    mp = ixmp.Platform()

    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    target_scenario = base_scenario.clone('weu_security',
                                          sensitivity_scenario, 
                                          keep_solution = False)
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
                
    if solve_scenario == True:
        target_scenario.solve(quiet = False)

    mp.close_db()

# Run scenarios
run_friction_scenario('SSP2', 'MEACON', 2100)

