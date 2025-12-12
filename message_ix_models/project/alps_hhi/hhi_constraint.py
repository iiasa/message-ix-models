# -*- coding: utf-8 -*-
"""
HHI with hard constraint
"""
# Import packages
from typing import Any

import logging
import numpy as np
import pandas as pd
import ixmp
from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config, get_logger

def hhi_constraint_run(project_name: str, 
                       config_name: str,
                       base_model: str,
                       base_scenario: str,
                       hhi_config_name: str,
                       target_scenario_add: str = None,
                       hhi_commodities: list | None = None):
    """Run HHI constraint"""
    """
    Parameters
    ----------
    project_name: str
        Name of the project
    config_name: str
        Name of the config file
    hhi_config_name: str
        Name of the HHI config file
    """
    log = get_logger(__name__)

    # Import configurations
    config, config_path = load_config(project_name = project_name,
                                      config_name = config_name)
    hhi_config, hhi_config_path = load_config(project_name = project_name,
                                              config_name = hhi_config_name)

    # Create platform
    mp = ixmp.Platform()

    log.info(f"HHI option: HC")

    target_model_name = 'alps_hhi'
    target_scen_name = base_scenario + '_hhi_HC'
    if target_scenario_add is not None:
        target_scen_name = target_scen_name + '_' + target_scenario_add
        
    log.info(f"Base scenario: {base_model}/{base_scenario}")
    log.info(f"Target scenario: {target_model_name}/{target_scen_name}")

    base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scenario)
    hhi_scenario = base_scenario.clone(target_model_name, target_scen_name, 
                                       keep_solution = False)
    hhi_scenario.set_as_default()

    if hhi_commodities is None:
        hhi_commodities = list(hhi_config.keys())

    with hhi_scenario.transact("Add HHI commodity and level"):
        hhi_scenario.add_set('commodity', hhi_commodities)
        hhi_scenario.add_set('level', 'hhi')

    hhi_output = pd.DataFrame()
    for k in hhi_commodities:
        log.info(f"Building HHI pseudo output for {k}")
        df = hhi_scenario.par('output')
        df = df[df['technology'].isin(hhi_config[k]['technologies'])]
        df = df[(df['node_loc'].isin(hhi_config[k]['nodes'])) |\
            (df['technology'].str.contains('exp'))]
        df['commodity'] = k
        df['level'] = 'hhi'
        df['unit'] = '???'
        hhi_output = pd.concat([hhi_output, df])

    with hhi_scenario.transact(f"Add HHI pseudo output"):
        hhi_scenario.add_par('output', hhi_output)

    log.info("Add growth constraint to coal_gas for WEU")
    if "weu_gas_supply" in hhi_commodities:
        growth_up = base_scenario.par("growth_activity_up", filters = {'technology': 'coal_gas', 
                                                                      'node_loc': 'R12_SAS'})
        initial_up = base_scenario.par("initial_activity_up", filters = {'technology': 'coal_gas', 
                                                                         'node_loc': 'R12_SAS'})
        growth_up['node_loc'] = 'R12_WEU'
        initial_up['node_loc'] = 'R12_WEU'
        initial_up['value'] = 0.01

        with hhi_scenario.transact("Add growth constraint to coal_gas for WEU"):
            hhi_scenario.add_par("growth_activity_up", growth_up)
            hhi_scenario.add_par("initial_activity_up", initial_up)
    
    log.info("Aggregate technology for gas extraction")
    if "weu_gas_supply" in hhi_commodities:
        tec_aggregation(scenario = hhi_scenario,
                        tec_list_base = ["gas_extr_1", "gas_extr_2", "gas_extr_3", "gas_extr_4",
                                         "gas_extr_5", "gas_extr_6", "gas_extr_7"],
                        output_commodity_base = "gas",
                        output_level_base = "primary",
                        output_commodity0 = "gas",
                        output_level0 = "extraction",
                        output_technology = "gas_extr_agg",
                        output_commodity1 = "gas",
                        output_level1 = "primary")
            
    log.info(f"Adding HHI limit")
    hhi_limit_df = hhi_output[['node_loc', 'commodity',
                               'level', 'year_act',
                               'value', 'unit']].drop_duplicates().reset_index(drop = True)
    hhi_limit_df = hhi_limit_df.rename(columns = {'node_loc': 'node'})
    hhi_limit_df = hhi_limit_df[hhi_limit_df['year_act'] > 2025]
    for k in hhi_commodities:
        hhi_limit_df.loc[hhi_limit_df['commodity'] == k, 'value'] = hhi_config[k]['value']
    hhi_limit_df['time'] = 'year'

    with hhi_scenario.transact("Add HHI limit"):
        hhi_scenario.add_par('hhi_limit', hhi_limit_df)

    log.info(f"Solving HHI scenario {k}")
    hhi_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)

    mp.close_db()