'''
Run Baseline Step 14 (Before Calibration
'''

import ixmp
import message_ix
import pandas as pd
import plotnine
import numpy as np

# Run scenarios over SSPs
model_name = f"SSP_SSP2_v6.5_rep_upd2"
scen_name = f"SSP2 - High Emissions"

# Load scenario
mp = ixmp.Platform()
slist = mp.scenario_list()
slist = slist[slist['model'] == model_name]
print(slist)
mp.close_db()
#base = message_ix.Scenario(mp, model_name, scen_name)
#scen = base.clone("oil-test", scen_name, keep_solution = False)
#scen.set_as_default()

# Run scenario
#scen.solve()
