# -*- coding: utf-8 -*-
"""
Add MEA conflict shock sensitivities
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

def collect_base_activity(base_scenario: message_ix.Scenario,
                          conf_i: list[str],
                          conf_tec: list[str],
                          conf_years: list[int]):

    base_act = base_scenario.var("ACT", filters = {'node_loc': conf_i,
                                                   'technology': conf_tec,
                                                   'year_act': conf_years})

    base_act = base_act.groupby(['node_loc', 'technology', 'year_act', 'mode', 'time'])['lvl'].sum().reset_index()
    base_act = base_act.rename(columns = {'lvl': 'base_level'})

    return base_act

def add_conflict(use_scenario: message_ix.Scenario,
                 base_scenario: message_ix.Scenario,
                 conf_level: float = 1.0):

    config, config_path = load_config(project_name = 'fuel_security', config_name = 'config.yaml')

    conf_i = config['mea_conflict']['MEA']['exporters']
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

    return bounddf

def run_mea_conflict_scenario(base_scenario: message_ix.Scenario,
                              conf_level: float = 1.0,
                              solve_scenario: bool = True):
    """Clone `base_scenario`, apply an MEA conflict shock, and solve.

    Args:
        base_scenario: Already-solved scenario to shock (e.g. a bilateralized or
            FSU-restricted fuel_security scenario).
        conf_level: Fraction of baseline MEA export activity retained
            (1.0 = no shock, 0.25 = 75% reduction).
        solve_scenario: Whether to solve the resulting scenario.

    Returns:
        The cloned, shocked (and, if `solve_scenario`, solved) scenario.
    """
    target_scenario_name = f"{base_scenario.scenario}_MEACON_{conf_level}"
    target_scenario = base_scenario.clone('fuel_security', target_scenario_name, keep_solution = False)
    target_scenario.set_as_default()

    conflict_df = add_conflict(target_scenario, base_scenario, conf_level)

    with target_scenario.transact("Add MEA conflict"):
        target_scenario.add_par('bound_activity_up', conflict_df)

    with target_scenario.transact("Remove constraints on shocked technologies"):
        for par in ["growth_activity_lo", "growth_activity_up", "initial_activity_lo", "initial_activity_up"]:
            basepar = target_scenario.par(par)
            basepar_exp = basepar[(basepar['technology'].str.contains("_shipped_exp")) & (basepar['node_loc'] == 'R12_MEA') & (basepar['year_act'].isin([2030, 2035, 2040]))]
            basepar_imp = basepar[(basepar['technology'].str.contains("_shipped_imp")) & (basepar['year_act'].isin([2030, 2035, 2040]))]

            if len(basepar) != 0:
                print(f"...{par}")
                target_scenario.remove_par(par, basepar_exp)
                target_scenario.remove_par(par, basepar_imp)

    if "INDC" in base_scenario.scenario:
        print("loosen emission bounds on non-Europe")
        with target_scenario.transact("Loosen emission bounds on non-Europe"):
            remdf = target_scenario.par("bound_emission")
            remdf = remdf[remdf['node'].isin(['R12_WEU', 'R12_EEU', 'R12_GLB']) == False]
            target_scenario.remove_par("bound_emission", remdf)

    if solve_scenario:
        target_scenario.solve(quiet = False, model = 'MESSAGE', solve_options={"scaind":"-1"})

    return target_scenario
