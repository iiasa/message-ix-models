'''
Diagnose issues with crude oil trade (global pool)
'''

import ixmp
import message_ix
import pandas as pd
import plotnine
import numpy as np

# Run a scenario that worked in the past
model_name = "SSP_SSP1_v6.5_rep_upd2" #"SSP_SSP1_v5.3"
scen_name = "baseline_DEFAULT_step_14"

mp = ixmp.Platform()
base = message_ix.Scenario(mp, model_name, scen_name)
scen = base.clone("oil-test", f"SSP1_baseline_DEFAULT_step_14", keep_solution = False)
scen.set_as_default()
scen.solve(quiet = False, solve_options={"scaind":"-1"})

# Run scenarios over SSPs
for ssp in [1]:
    model_name = f"SSP_SSP{ssp}_v6.5_rep_upd2"
    scen_name = f"baseline_DEFAULT_step_14"

    # Load scenario
    mp = ixmp.Platform()
    base = message_ix.Scenario(mp, model_name, scen_name)
    scen = base.clone("oil-test", f"SSP{ssp}_baseline_DEFAULT_step_14_UPDATE", keep_solution = False)
    scen.set_as_default()

    # Add growth constraints for 2020 values, from 2025 values
    for par in ['growth_activity_up', 'growth_activity_lo', 'initial_activity_up', 'initial_activity_lo']:
        pardf = scen.par(par, filters = {'technology': ['oil_exp', 'oil_imp'],
                                         'year_act': 2025})
        pardf['year_act'] = 2020
        if par in ['growth_activity_up', 'growth_activity_lo']:
            pardf['value'] = pardf['value']*2 # give more slack
        
        with scen.transact(f"Add {par} for 2020"):
            scen.add_par(par, pardf)
            
    # Run scenario
    scen.solve(quiet = False, solve_options={"scaind":"-1"})
mp.close_db()