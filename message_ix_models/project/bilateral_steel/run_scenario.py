# -*- coding: utf-8 -*-
"""
Run scenario
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.bilateral_steel.steel_yearbook import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'bilateral_steel', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

base_model = 'steel_trade'
base_scen = 'only_steel'

mp = ixmp.Platform()
base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
base_scenario.solve()
mp.close_db()
