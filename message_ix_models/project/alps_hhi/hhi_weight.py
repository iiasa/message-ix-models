# -*- coding: utf-8 -*-
"""
HHI with weighted sum
"""
# Import packages
from typing import Any


import numpy as np
import pandas as pd
import ixmp
#from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config, get_logger

def hhi_weightsum_run(project_name: str, 
                      config_name: str,
                      base_model: str,
                      base_scenario: str,
                      hhi_config_name: str,
                      hhi_commodities: list[str] = None,
                      lambda_ws: float = 1,
                      hhi_max_total: float = 1,
                      hhi_scale: float = 0.002,
                      target_scen_add: str = None):

    """Run HHI weighted sum"""
    """
    Parameters
    ----------
    project_name: str
        Name of the project
    config_name: str
        Name of the config file
    hhi_config_name: str
        Name of the HHI config file
    hhi_commodities: list[str]
        List of commodities to include in the HHI
    lambda_ws: float
        Weight for weighted sum
    cost_max_total: float
        Maximum cost for total cost
    hhi_max_total: float
        Maximum HHI for total HHI
    hhi_scale: float
        Scale for HHI 
    """
    log = get_logger(__name__)

    # Import configurations
    config, config_path = load_config(project_name = project_name,
                                      config_name = config_name)
    hhi_config, hhi_config_path = load_config(project_name = project_name,
                                              config_name = hhi_config_name)

    # Create platform
    mp = ixmp.Platform()

    log.info(f"HHI option: Weighted Sum")

    base_model_name = 'alps_hhi'
    base_scen_name = base_scenario
    target_model_name = 'alps_hhi'
    if target_scen_add is None:
        target_scen_name = base_scenario + '_hhi_WS'
    else:
        target_scen_name = base_scenario + '_hhi_WS_' + target_scen_add

    log.info(f"Base scenario: {base_model}/{base_scenario}")
    log.info(f"Target scenario: {target_model_name}/{target_scen_name}")

    base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
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
    
    log.info(f"Adding HHI indicator parameter")
    hhi_indic = hhi_output[['node_loc', 'commodity', 'level', 'value', 'unit']]
    hhi_indic = hhi_indic.drop_duplicates().reset_index(drop = True)
    hhi_indic = hhi_indic.rename(columns = {'node_loc': 'node'})

    log.info("Collating base scenario activity levels")
    base_act = list()
    for k in hhi_commodities:
        base_act_k = base_scenario.var("ACT", filters = {'technology': hhi_config[k]['technologies']})
        base_act_k = base_act_k[(base_act_k['technology'].str.contains('exp'))|(base_act_k['node_loc'].isin(hhi_config[k]['nodes']))]
        base_act_k = base_act_k['lvl'].sum()
        base_act.append(base_act_k)
    hhi_scale_denom = np.mean(base_act)*2
    hhi_scale = round(1/hhi_scale_denom, 8)
    
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

    log.info("Collating base scenario objective")
    cost_max_total = base_scenario.var("OBJ")['lvl']

    with hhi_scenario.transact("Add HHI indicator parameter"):
        hhi_scenario.add_par('include_commodity_hhi', hhi_indic)
    
    with hhi_scenario.transact("Add scalar parameters"):
        hhi_scenario.init_scalar("lambda_ws", lambda_ws, "-")
        hhi_scenario.init_scalar("cost_max_total", cost_max_total, "USD")
        hhi_scenario.init_scalar("hhi_max_total", hhi_max_total, "-")
        hhi_scenario.init_scalar("hhi_scale", hhi_scale, "-")

    hhi_scenario.solve(gams_args = ['--HHI_WS=1'], quiet = False)

    mp.close_db()