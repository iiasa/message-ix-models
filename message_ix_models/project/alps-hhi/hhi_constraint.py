# -*- coding: utf-8 -*-
"""
HHI with hard constraint
"""
# Import packages
import numpy as np
import pandas as pd
from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config

# Import configurations
config, config_path = load_config(project_name = 'alps-hhi', config_name = 'config.yaml')
hhi_config, hhi_config_path = load_config(project_name = 'alps-hhi', config_name = 'hhi_config.yaml')

mp = ixmp.Platform()

base_model_name = 'alps-hhi'
base_scen_name = 'baseline'
target_model_name = 'alps-hhi'
target_scen_name = 'baseline_hhi_HC'

base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
hhi_scenario = base_scenario.clone(target_model_name, target_scen_name)
hhi_scenario.set_as_default()

# Build HHI constraint dataframe

year_list = list(hhi_scenario.set("year"))
hhi_limit_df = 


with hhi_scenario.transact("Add HHI constraint"):
    year_list = list(hhi_scenario.set("year"))
    hhi_limit_df = pd.DataFrame()
    for y in year_list:


    )

