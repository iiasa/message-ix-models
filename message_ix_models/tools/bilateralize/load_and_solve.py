# -*- coding: utf-8 -*-
"""
Update MESSAGEix scenario(s) with bilateralized dictionary

This script is the third step in implementing the bilateralize tool.
It updates a specified MESSAGEix scenario with the bilateralized dictionary. 
It then has options to solve the scenario within the ixmp database or save as a GDX data file for direct solve in GAMS.
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
import pickle

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *

def load_and_solve(project_name: str = None, 
                   config_name: str = None,
                   solve_scenario: bool = True,
                   start_model_name: str = None,
                   start_scenario_name: str = None,
                   target_model_name: str = None,
                   target_scenario_name: str = None,
                   gdx_location: str = None,
                   scenario_parameter_name: str = "scenario_parameters.pkl"):
    """
    Load and solve a MESSAGEix scenario
    
    Args:
        project_name: Name of the project (e.g., 'newpathways')
        config_name: Name of the config file (e.g., 'config.yaml')
        solve_scenario: If True, sovle scenario using ixmp
        gdx_location: Location of the GDX data file (e.g., 'C:/GitHub/message_ix/message_ix/model/data')
        scenario_parameter_name: Name of scenario parameter name (default is 'scenario_parameters.pkl')
    """
    
    # Bring in configuration
    config, config_path = load_config(project_name = project_name, config_name = config_name)

    covered_tec = config['covered_trade_technologies']
    message_regions = config['scenario']['regions']

    # Get logger
    log = get_logger(__name__)

    # Import pickle of parameter definitions
    tdf = os.path.join(os.path.dirname(config_path), scenario_parameter_name)
    trade_parameters = pd.read_pickle(tdf)

    # Update scenario: default values
    if solve_scenario == True: 
        to_gdx = False
    else:
        to_gdx = True

    clone_and_update(trade_dict=trade_parameters,
                    project_name = project_name,
                    config_name = config_name,
                    log=log,
                    to_gdx = to_gdx,
                    solve = solve_scenario,
                    gdx_location = gdx_location,
                    start_model_name = start_model_name,
                    start_scenario_name = start_scenario_name,
                    target_model_name = target_model_name,
                    target_scenario_name = target_scenario_name)

