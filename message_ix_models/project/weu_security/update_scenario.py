# -*- coding: utf-8 -*-
"""
Update scenario
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.weu_security.liquefaction_calibration import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'weu_security', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

base_model = 'weu_security'
base_scen = 'FSU2100'

mp = ixmp.Platform()

base_scenario = message_ix.Scenario(mp, model=base_model, scenario=base_scen)
out_scenario = base_scenario.clone('weu_security', "myopic-test", keep_solution = False)
out_scenario.set_as_default()

out_scenario.solve(quiet = False, solve_options={"scaind":"-1"}, gams_args=["--foresight=11"])

# Remove relaxation
#print("Remove relaxation")
#base_par = out_scenario.par("soft_activity_up")
#base_par = base_par[base_par['technology'].isin(['LNG_shipped_exp_weu', 'LNG_shipped_exp_eeu'])]

#with out_scenario.transact("remove relaxation on LNG shipping constraints"):
#    out_scenario.remove_par("soft_activity_up", base_par)

# Tighten growth bound for long-term contract (non-flexible) LNG producers (MEA)
#print("Tighten growth bound for long-term contract (non-flexible) LNG producers (MEA)")
#base_par = out_scenario.par("growth_activity_up")
#base_par = base_par[base_par['node_loc'].isin(['R12_MEA'])]
#base_par = base_par[base_par['technology'].isin(['LNG_shipped_exp_weu', 'LNG_shipped_exp_eeu'])]

#new_par = base_par.copy()
#new_par['value'] = 0.02

#with out_scenario.transact("Tigten growth constraints"):
#    out_scenario.remove_par("growth_activity_up", base_par)
#    out_scenario.add_par("growth_activity_up", new_par)
    
#out_scenario.solve(quiet = False, solve_options={"scaind":"-1"})

mp.close_db()
