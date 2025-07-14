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

from message_ix_models.tools.bilateralize.build_sankey import *
from message_ix_models.util import package_data_path
from ixmp import Platform

# Connect to ixmp
mp = ixmp.Platform()
full_path = package_data_path("bilateralize", "config.yaml")
config_dir = os.path.dirname(full_path)

# Build Sankey dataframes
def build_sankeydf(commodities:dict,
                   model_name:str, 
                   scenario_name:str):
    
    scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    
    activity = scen.var("ACT")
    activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
    activity = activity.groupby(['node_loc', 'technology', 'year_act'])['lvl'].sum().reset_index()
    
    slist = [c + '_exp_' for c in commodities.keys()]
    sdf = activity[activity['technology'].str.contains('|'.join(slist))].copy()
    
    sdf['fuel'] = ''
    for c in commodities.keys():
        sdf['fuel'] = np.where(sdf['technology'].str.contains(c),
                               commodities[c],
                               sdf['fuel'])
        
    sdf['importer'] = 'R12_' + sdf['technology'].str.upper().str.split('_').str[-1]
    sdf = sdf.rename(columns = {'node_loc': 'exporter',
                                'lvl': 'value',
                                'year_act': 'year'})
    sdf = sdf[['year', 'exporter', 'importer', 'fuel', 'value']]
    
    return sdf

sankeydf = build_sankeydf(commodities = {'gas_piped': 'Pipeline Gas'},
                          model_name = "NP_SSP2", scenario_name = "pipelines_only")


# Load data
# Create sankey visualizer
sankey = InteractiveSankey(sankeydf)

#Filter data
#sankey.filter_data(year=2023, fuel='Solar Panels')

#Create visualization
# fig = sankey.create_sankey(title="Trade Flows")

# #Show in Jupyter notebook
# fig.show()

# #Or save to HTML
# fig.write_html(os.path.join(config_dir, "diagnostics", "sankey_diagram.html"))

# Create dashboard
dashboard = sankey.create_dashboard()
dashboard.write_html(os.path.join(config_dir, "diagnostics", "sankey_diagram.html"))
