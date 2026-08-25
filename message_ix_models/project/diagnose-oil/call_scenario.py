'''
Diagnose issues with crude oil trade (global pool)
'''

import ixmp
import message_ix
import pandas as pd
import plotnine
import numpy as np

# Run scenarios over SSPs
for ssp in [1, 2, 3, 4, 5]:
    model_name = f"SSP_SSP{ssp}_v6.5_rep_upd2"
    scen_name = f"SSP{ssp} - High Emissions"

    # Load scenario
    mp = ixmp.Platform()
    base = message_ix.Scenario(mp, model_name, scen_name)
    scen = base.clone("oil-test", scen_name)

    # Run scenario
    scen.solve()