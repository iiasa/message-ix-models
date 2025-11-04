# -*- coding: utf-8 -*-
"""
HHI with weighted sum
"""
# Import packages
from typing import Any


import numpy as np
import pandas as pd
import ixmp
from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config, get_logger

def hhi_weight_build(hhi_scenario: message_ix.Scenario,
                     hhi_config: dict):
    """Add parameters for HHI weighted sum to scenario
    Parameters
    ----------
    """
    # Build HHI constraint dataframe
    hhi_limit_df = pd.DataFrame()
    for k in hhi_config.keys():
        lno = len(hhi_config['hhi_constraint'][k]['nodes'])
        hhi_limit_df = pd.concat([hhi_limit_df, pd.DataFrame({
            "commodity": [hhi_config['hhi_constraint'][k]['commodity']]*lno,
            "level": [hhi_config['hhi_constraint'][k]['level']]*lno,
            "node": hhi_config['hhi_constraint'][k]['nodes'],
            "value": [hhi_config['hhi_constraint'][k]['value']]*lno,
            "time": ["year"]*lno
        })])

    year_list = [y for y in list(hhi_scenario.set("year"))
                if y > 2025]
    hhi_limit_df = hhi_limit_df.assign(key=1).merge(
        pd.DataFrame({'year': year_list, 'key': 1}),
        on = 'key').drop('key', axis = 1)

    return hhi_limit_df

def hhi_constraint_run(project_name: str, 
                       config_name: str,
                       hhi_config_name: str,
                       hhi_scenario_name: str | None = None):
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
    hhi_scenario_name: str | None
        Name of the HHI scenario (appended to base scenario name)
    """
    # Import configurations
    config, config_path = load_config(project_name = project_name,
                                      config_name = config_name)
    hhi_config, hhi_config_path = load_config(project_name = project_name,
                                              config_name = hhi_config_name)

    # Create platform
    mp = ixmp.Platform()

    for k in config['models_scenario'].keys():
        base_model_name = config['models_scenario'][k]['model']
        base_scen_name = config['models_scenario'][k]['scenario']
        target_model_name = config['models_scenario'][k]['model']
        target_scen_name = config['models_scenario'][k]['scenario']

        if hhi_scenario_name is None:
            target_scen_name = target_scen_name + '_hhi_HC'
        else:
            target_scen_name = target_scen_name + hhi_scenario_name

        base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
        hhi_scenario = base_scenario.clone(target_model_name, target_scen_name, 
                                           keep_solution = False)
        hhi_scenario.set_as_default()

        hhi_limit_df = hhi_constraint_build(hhi_scenario, hhi_config)

        with hhi_scenario.transact("Add HHI constraint"):
            hhi_scenario.add_par('hhi_limit', hhi_limit_df)

        hhi_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)