# -*- coding: utf-8 -*-
"""
Resource Supply Curves (RSC) from a scenario
"""

# -*- coding: utf-8 -*-
"""
Diagnostics on bilateralization
"""
# Import packages
import os
import sys
import pandas as pd
import numpy as np
import logging
import yaml
import message_ix
import ixmp
import itertools
import plotly.graph_objects as go

from message_ix_models.util import package_data_path
from ixmp import Platform

# Connect to ixmp
mp = ixmp.Platform()

scenario_name = 'pipelines_LNG'
model_name = 'NP_SSP2'
gas_commodities = {1: 'Identified Reserves',
                   2: 'Undiscovered Natural Gas (Mode)',
                   3: 'UND (Diff between Mode and 5%)',
                   4: 'Estimated enhanced recovery',
                   5: 'Non-conventional reserves (20% CBM, 15% FS, 15% TF)',
                   6: 'Non-conventional reserves (80% CBM, 60%)',
                   7: 'Non-conventional reserves (80% CBM, 40%)'}

# Connect to scenario
scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)

# Pull resource
res_vol = scen.par('resource_volume', 
                   filters = {'commodity': ['gas_' + str(i) for i in list(gas_commodities.keys())]})

# Pull costs
inv_cost = scen.par('inv_cost',
                    filters = {'technology': ['gas_extr_' + str(i) for i in list(gas_commodities.keys())]})
var_cost = scen.par('var_cost',
                    filters = {'technology': ['gas_extr_' + str(i) for i in list(gas_commodities.keys())]})

