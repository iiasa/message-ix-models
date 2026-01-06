# -*- coding: utf-8 -*-
"""
HHI workflow
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

# Import scenario and models
config, config_path = load_config(project_name = 'alps_hhi', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Create platform
mp = ixmp.Platform()

base_model_name = 'alps_hhi'
base_scen_name = 'SSP2'
target_model_name = 'alps_hhi'
target_scen_name = 'SSP2_update'

base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
hhi_scenario = base_scenario.clone(target_model_name, target_scen_name, 
                                   keep_solution = False)
hhi_scenario.set_as_default()

updf = hhi_scenario.par('initial_activity_up')
updf = updf[(updf['technology'].str.contains('gas_piped_exp'))]
updf = updf[updf['node_loc'].isin(['R12_AFR', 'R12_FSU', 'R12_LAM', 'R12_MEA']) == False]

with hhi_scenario.transact("add initial activity up to gas_extr_mpen"):
    hhi_scenario.remove_par('initial_activity_up', updf)
    
hhi_scenario.solve()
mp.close_db()
