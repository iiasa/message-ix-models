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
import bokeh
import holoviews as hv
from holoviews import dim, opts

from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.build_sankey import *
from message_ix_models.util import package_data_path
from ixmp import Platform

# Bring in configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')
data_path = os.path.dirname(config_path)

# Connect to ixmp
mp = ixmp.Platform()

# Build Sankey dataframes
def build_sankeydf(commodities:dict,
                   model_name:str, 
                   scenario_name:str):
    
    scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    
    activity = scen.var("ACT")
    activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
    activity = activity.groupby(['node_loc', 'technology', 'year_act'])['lvl'].sum().reset_index()
    
    hist_activity = scen.par('historical_activity')
    hist_activity = hist_activity[['node_loc', 'technology', 'year_act', 'value']].drop_duplicates().reset_index()
    hist_activity = hist_activity.groupby(['node_loc', 'technology', 'year_act'])['value'].sum().reset_index()
    hist_activity = hist_activity.rename(columns = {'value': 'lvl'})
    activity = pd.concat([hist_activity, activity])
    
    
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

# Build data
df = build_sankeydf(commodities = {'gas_piped': 'Pipeline Gas',
                                   'LNG_shipped': 'Shipped LNG'},
                    model_name = "NP_SSP2_6.2", scenario_name = "pipelines_LNG")
df = df[df['value'] > 0.5]

# Create sankey
sankey = InteractiveSankey(df)
dashboard = sankey.create_dashboard()
dashboard.write_html(os.path.join(data_path, "diagnostics", "sankey.html"))

# Create chord
fuel = 'Shipped LNG'
year = 2030

cdf = df[df['year'] == year]
cdf = cdf[cdf['fuel'] == fuel]
cdf = cdf[['exporter', 'importer', 'value']]

cdf_nodes = pd.DataFrame(set(list(cdf['exporter'].unique()) + list(cdf['importer'].unique()))).reset_index()
cdf_nodes.columns = ['node', 'message_region']

cdf = cdf.merge(cdf_nodes, left_on = 'exporter', right_on = 'message_region', how = 'left')
cdf = cdf.merge(cdf_nodes, left_on = 'importer', right_on = 'message_region', how = 'left')

cdf = cdf.rename(columns = {'node_x': 'source',
                            'node_y': 'target'})
cdf = cdf[['source', 'target', 'value']]

cdf_nodes = hv.Dataset(cdf_nodes, 'index')

chord_out = hv.Chord((cdf, cdf_nodes))
chord_out.opts(opts.Chord(cmap='Category20', edge_cmap='Category20', edge_color=dim('source').str(),
               labels='name', node_color=dim('index').str()))

hv.save(chord_out, os.path.join(data_path, "diagnostics", "chord.svg"), fmt = 'svg')



def create_chord(year:int, fuel:str):
    cdf = df[df['year'] == year]
    cdf = cdf[cdf['fuel'] == fuel]
    cdf = cdf[['exporter', 'importer', 'value']]
  
    cdf_nodes = pd.DataFrame(set(list(cdf['exporter'].unique()) + list(cdf['importer'].unique())))
    cdf_nodes = cdf_nodes.sort_values(by = 0).reset_index(drop = True)
    cdf_nodes = cdf_nodes.reset_index()
    cdf_nodes.columns = ['node', 'message_region']
  
    cdf = cdf.merge(cdf_nodes, left_on = 'exporter', right_on = 'message_region', how = 'left')
    cdf = cdf.merge(cdf_nodes, left_on = 'importer', right_on = 'message_region', how = 'left')
  
    cdf = cdf.rename(columns = {'node_x': 'source',
                  'node_y': 'target'})
    cdf = cdf[['source', 'target', 'value']]
    cdf['value'] = round(cdf['value'],0)
  
    # Inflate dataframe
    inflate_cdf = pd.DataFrame()
    for i in list(cdf.index):
        nr = cdf['value'][i].astype(int)
        idf = pd.DataFrame(np.empty((nr, 3)) * 1.0) 
        idf.columns = ['source', 'target', 'value']
        idf['source'] = cdf['source'][i]
        idf['target'] = cdf['target'][i]
        idf['value'] = 1
        inflate_cdf = pd.concat([inflate_cdf, idf])
      
    inflate_cdf = inflate_cdf.reset_index(drop = True)
    
    cdf_nodes = hv.Dataset(cdf_nodes, 'index')
    
    chord_out = hv.Chord((inflate_cdf, cdf_nodes))
    chord_out.opts(opts.Chord(edge_cmap = 'Category20', edge_color=dim('source').str(),
                              node_cmap = 'Category20', node_color=dim('node').str(),
                              labels='message_region'))

    return chord_out
  
    
    

