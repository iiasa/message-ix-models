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

# Create platform
mp = ixmp.Platform()

base_model_name = 'alps_hhi'
base_scen_name = 'SSP2'
target_model_name = 'alps_hhi'
target_scen_name = 'SSP2u'

base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
hhi_scenario = base_scenario.clone(target_model_name, target_scen_name, 
                                   keep_solution = False)
hhi_scenario.set_as_default()

pardf = hhi_scenario.par('growth_activity_up', filters = {'technology': '
hhi_scenario.solve()

mp.close_db()
