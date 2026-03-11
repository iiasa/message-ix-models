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

def add_conflict(use_scenario:message_ix.Scenario):
    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')

    conf_i = config['mea_conflict']['MEA']['exporters']
    conf_j = config['mea_conflict']['MEA']['importers']
    conf_techs_in = config['mea_conflict']['MEA']['technologies']

    con_years = [2030, 2035]

    base_var = use_scenario.par('var_cost', filters = {"technology": conf_techs_in,
                                                        "node_loc": conf_i})

    use_var = base_var.copy()
    use_var['value'] = use_var['value'] * 20 # Make it 20 times more expensive to ship from MEA to other regions

    with use_scenario.transact("Add MEA conflict on tanker shipping"):
        use_scenario.remove_par('var_cost', base_var)
        use_scenario.add_par('var_cost', use_var)

def run_friction_scenario(base_scenario_name: str):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')

    mp = ixmp.Platform()

    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    use_scenario = base_scenario.clone('weu_security',
                                        base_scenario_name + "_MEACON", 
                                        keep_solution = False)
    use_scenario.set_as_default()

    add_conflict(use_scenario)

    use_scenario.solve(quiet = False)

    mp.close_db()

# Run scenarios
run_friction_scenario('SSP2')

