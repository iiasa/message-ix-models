# -*- coding: utf-8 -*-
"""
HHI with hard constraint
"""
# Import packages
from typing import Any


import numpy as np
import pandas as pd
from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config

# Import configurations
config, config_path = load_config(project_name = 'alps-hhi', config_name = 'config.yaml')
hhi_config, hhi_config_path = load_config(project_name = 'alps-hhi', config_name = 'hhi_config.yaml')

#mp = ixmp.Platform()

base_model_name = 'alps_hhi'
base_scen_name = 'baseline'
target_model_name = 'alps_hhi'
target_scen_name = 'baseline_hhi_HC'

#base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
#hhi_scenario = base_scenario.clone(target_model_name, target_scen_name)
#hhi_scenario.set_as_default()

# Build HHI constraint dataframe
hhi_limit_df = pd.DataFrame()
for k in hhi_config['hhi_constraint'].keys():
    lno = len(hhi_config['hhi_constraint'][k]['nodes'])
    hhi_limit_df = pd.concat([hhi_limit_df, pd.DataFrame({
        "commodity": [hhi_config['hhi_constraint'][k]['commodity']]*lno,
        "level": [hhi_config['hhi_constraint'][k]['level']]*lno,
        "node_loc": hhi_config['hhi_constraint'][k]['nodes'],
        "value": [hhi_config['hhi_constraint'][k]['value']]*lno,
    })])

year_list = [2020, 2030, 2040, 2050] #list[Any](hhi_scenario.set("year"))
hhi_limit_df = hhi_limit_df.assign(key=1).merge(
    pd.DataFrame({'year_act': year_list, 'key': 1}),
    on = 'key').drop('key', axis = 1)


with hhi_scenario.transact("Add HHI constraint"):
    hhi_scenario.add_par('hhi_limit', hhi_limit_df)

hhi_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)