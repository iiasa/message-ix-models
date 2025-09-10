# -*- coding: utf-8 -*-
"""
Run baseline

This clones the non-bilateralized version of the model/scenario (base model)
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

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *
from message_ix_models.tools.bilateralize.pull_gem import *
from message_ix_models.tools.bilateralize.mariteam_calibration import *

# Get logger
log = get_logger(__name__)

# Import configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')

covered_tec = config['covered_trade_technologies']
message_regions = config['scenario']['regions']
active_years = config['timeframes']['year_act_list']

node_path = package_data_path("bilateralize", "node_lists", message_regions + "_node_list.yaml")
with open(node_path, "r") as f:
    node_set = yaml.safe_load(f) 
node_set = [r for r in node_set.keys() if r not in ['World', 'GLB']]

# Import pickle of parameter definitions
tdf = os.path.join(os.path.dirname(config_path), 'scenario_parameters.pkl')
trade_parameters = pd.read_pickle(tdf)

# Load the scenario
mp = ixmp.Platform()

base = message_ix.Scenario(mp, model='NP_SSP2_6.2', scenario='pipelines_LNG')
scen = base.clone(model='NP_SSP2_6.2', scenario='HHI_0.9_gas_CHN', keep_solution = False)
scen.set_as_default()

# Prepare outputs for HHI
hhi_tec_nodes = {'LNG_shipped_exp_chn': {'R12_AFR': 1,
                                         'R12_EEU': 1,
                                         'R12_FSU': 1,
                                         'R12_LAM': 1,
                                         'R12_MEA': 1,
                                         'R12_NAM': 1,
                                         'R12_PAO': 1,
                                         'R12_PAS': 1,
                                         'R12_RCPA': 1,
                                         'R12_SAS': 1,
                                         'R12_WEU': 1},
                 'gas_piped_exp_chn': {'R12_PAS': 1,
                                       'R12_FSU': 1}}

def add_HHI(scen: message_ix.Scenario,
            hhi_tec_nodes: dict,
            hhi_portfolio_node: str, 
            hhi_limit: float = 1,
            first_model_year: int = 2030):
    
    # Add HHI outputs for selected technologies
    outputdf = pd.DataFrame()
    for tec in hhi_tec_nodes.keys():
        odf = scen.par('output', filters = {'technology': tec,
                                            'node_loc': hhi_tec_nodes[tec].keys()})
        odf = odf[['node_loc', 'technology', 'year_vtg', 'year_act', 'mode',
                   'time', 'time_dest']].drop_duplicates() # Omit commodity, level, value, unit, node_dest
        odf['commodity'] = 'HHI'
        odf['level'] = 'HHI'
        odf['unit'] = '-'
        odf['value'] = 0
        odf['node_dest'] = hhi_portfolio_node # Where is the portfolio being calculated?
        
        odf = odf.drop_duplicates()
        
        for node in hhi_tec_nodes[tec].keys():
            odf['value'] = np.where(odf['node_loc'] == node, hhi_tec_nodes[tec][node], odf['value'])
        
        outputdf = pd.concat([outputdf, odf])
    
    with scen.transact("Add HHI output"):
        scen.add_set('commodity', 'HHI')
        scen.add_set('level', 'HHI')
        scen.add_par('output', outputdf)
   
    # Add HHI limit
    hhi_df = outputdf[['node_dest', 'commodity', 'level', 'year_act', 'time', 'unit']].copy().drop_duplicates()
    hhi_df = hhi_df.rename(columns = {'node_dest': 'node',
                                      'year_act': 'year'})
    hhi_df['value'] = hhi_limit
    hhi_df['value'] = np.where(hhi_df['year'] < first_model_year, 1, hhi_df['value'])
    
    with scen.transact("Add HHI limit"):
        scen.add_par("hhi_limit", hhi_df)
        
# Add HHI limit
add_HHI(scen=scen,
        hhi_tec_nodes=hhi_tec_nodes,
        hhi_portfolio_node="R12_CHN",
        hhi_limit=0.6)

# Solve
solver = "MESSAGE"
scen.solve(solver, 
           solve_options=dict(lpmethod = 4,
                              scaind = -1,
                              threads = 16,
                              iis = 1,
                              gams_args = ["--HHI=1"]))

mp.close_db()
