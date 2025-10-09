# -*- coding: utf-8 -*-
"""
Test HHI code
"""
# Import packages
import message_ix
import ixmp
from ixmp import Platform
import pandas as pd

# Load the Westeros scenario
mp = ixmp.Platform()
scen = message_ix.Scenario(mp, model='NP_HHI', scenario='Westeros')

base = make_westeros(mp, emissions=True, solve=False)
scen = base.clone(model='NP_HHI', scenario='Westeros', keep_solution = False)

# Add hhi parameters
cost_base_total = 1
cost_max_total = "500000"
hhi_min_total = "0"
hhi_max_total = "1"

include_commodity_hhi = {'node': ['Westeros'],
                         'commodity': ['electricity'],
                         'level': ['secondary']}
include_commodity_hhi = pd.DataFrame.from_dict(include_commodity_hhi)

hhi_parameters = {'cost_base_total': cost_base_total,
                  'cost_max_total': cost_max_total,
                  'hhi_min_total': hhi_min_total,
                  'hhi_max_total': hhi_max_total}

with scen.transact("Add cost_base_total"):
    scen.add_par("cost_base_total", cost_base_total)
