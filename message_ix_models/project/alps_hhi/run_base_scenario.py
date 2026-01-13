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

for smip_model in ['SSP_SSP2_v6.2', 'SSP_SSP3_v6.2']:
    base_scenario = message_ix.Scenario(mp, model=smip_model, scenario='baseline')
    new_scenario = base_scenario.clone('alps_hhi', smip_model, 
                                       keep_solution = False)
    new_scenario.set_as_default()
    
    new_scenario.solve()

mp.close_db()
