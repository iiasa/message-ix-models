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

def collect_base_activity(base_scenario:message_ix.Scenario,
                          conf_i: list[str],
                          conf_tec: list[str],
                          conf_years: list[int]):

    base_act = base_scenario.var("ACT", filters = {'node_loc': conf_i,
                                                   'technology': conf_tec,
                                                   'year_act': conf_years})
    
    base_act = base_act.groupby(['node_loc', 'technology', 'year_act', 'mode', 'time'])['lvl'].sum().reset_index()
    base_act = base_act.rename(columns = {'lvl': 'base_level'})

    return base_act

def add_conflict(use_scenario:message_ix.Scenario,
                 base_scenario:message_ix.Scenario,
                 conf_level: float = 1.0):

    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')

    conf_i = config['mea_conflict']['MEA']['exporters']
    conf_j = config['mea_conflict']['MEA']['importers']
    conf_techs_in = config['mea_conflict']['MEA']['technologies']

    conf_years = [2030, 2035]

    base_input = use_scenario.par('input', filters = {'node_loc': conf_i})
    base_input = base_input[base_input['technology'].str.contains('shipped_exp')]
    conf_tec = base_input['technology'].unique()

    base_levels = collect_base_activity(base_scenario, conf_i, conf_tec, conf_years)

    basedf = pd.DataFrame(product(conf_i, conf_tec,
                         conf_years,
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

    bounddf = bounddf.merge(base_levels, on = ['node_loc', 'technology', 'year_act', 'mode', 'time'], how = 'left')
    bounddf['value'] = bounddf['base_level'] - bounddf['base_level'] * conf_level
    bounddf = bounddf.drop(columns = ['base_level'])
    print(bounddf)

    return bounddf

def run_friction_scenario(base_scenario_name:str,
                          conf_level: float = 1.0):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')

    mp = ixmp.Platform()

    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    use_scenario = base_scenario.clone('weu_security',
                                        f"{base_scenario_name}_MEACON_{str(conf_level)}", 
                                        keep_solution = False)
    use_scenario.set_as_default()

    conflict_df = add_conflict(use_scenario, base_scenario, conf_level)
    
    with use_scenario.transact("Add MEA conflict"):
        use_scenario.add_par('bound_activity_up', conflict_df)

    with use_scenario.transact("Remove constraints on shocked technologies"):
        for par in ["growth_activity_lo", "growth_activity_up", "initial_activity_lo", "initial_activity_up"]:
            basepar = use_scenario.par(par)
            basepar_exp = basepar[(basepar['technology'].str.contains("_shipped_exp")) & (basepar['node_loc'] == 'R12_MEA')]
            basepar_imp = basepar[basepar['technology'].str.contains("_shipped_imp")]
            
            if len(basepar) != 0:
                print(f"...{par}")
                use_scenario.remove_par(par, basepar_exp)
                use_scenario.remove_par(par, basepar_imp)

    if "INDC" in base_scenario_name:
        print("loosen emission bounds on non-Europe")
        with use_scenario.transact("Loosen emission bounds on non-Europe"):
            remdf = use_scenario.par("bound_emission")
            remdf = remdf[remdf['node'].isin(['R12_WEU', 'R12_EEU', 'R12_GLB']) == False]
            use_scenario.remove_par("bound_emission", remdf)
            
    use_scenario.solve(quiet = False, solve_options={"scaind":"-1"})

    mp.close_db()

# Run scenarios
#for scen in ['FSU2100', 'FSU2040']: # Add back SSP2
#    for conflict in [1.0, 0.9, 0.8, 0.75, 0.5, 0.25]:
#        print(f"Build and run: Base scenario = {scen}, Impact Level = {conflict}")
#        run_friction_scenario(scen, conf_level = conflict)

# Add decarbonization sensitivities
for scen in ["INDC2030", "INDC2030_FSU2040", "INDC2030_FSU2100"]:
    print(f"Build and run: Base scenario = {scen}, Impact Level = 1.0")
    run_friction_scenario(scen, conf_level = 1.0)

