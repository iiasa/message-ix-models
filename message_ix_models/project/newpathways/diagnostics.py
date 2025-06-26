# -*- coding: utf-8 -*-
"""
Diagnostics on bilateralization
"""
# Import packages
import os
import sys
import pandas as pd
import logging
import yaml
import message_ix
import ixmp
import itertools
import plotly.graph_objects as go

from message_ix_models.util import package_data_path
#from message_data.tools.post_processing import iamc_report_hackathon
from ixmp import Platform
#from ixmp.report import configure, Key, Reporter

# Connect to ixmp
mp = ixmp.Platform()

full_path = package_data_path("bilateralize", "config.yaml")
config_dir = os.path.dirname(full_path)

# def report_scenario(mp, model_name, scenario_name):
#     scen = message_ix.Scenario(mp, 
#                                model=model_name, 
#                                scenario=scenario_name)
    
#     iamc_report_hackathon.report(mp=mp, 
#                                  scen=scenario_name, 
#                                  merge_hist=False, 
#                                  run_config="materials_daccs_run_config.yaml")

# Run Reporter
def activity_to_csv(base_export_tec,
                    base_import_tec,
                    model_name, 
                    scenario_name):
    
    scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)

    activity = scen.var("ACT")
    activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
    
    exports = activity[(activity['technology'].str.contains(base_export_tec))].copy()
    imports = activity[(activity['technology'] == base_import_tec)].copy()
    
    exports['IMPORTER'] = 'R12_' + exports['technology'].str.upper().str.split('_').str[2]
    exports = exports.rename(columns = {'node_loc': 'EXPORTER',
                                        'lvl': 'LEVEL',
                                        'year_act': 'YEAR',
                                        'technology': 'MESSAGETEC'})
    exports = exports[['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGETEC', 'LEVEL']]
    exports.to_csv(os.path.join(config_dir, 'diagnostics', base_export_tec + '_' + model_name + '_' + scenario_name + '.csv'),
                   index = False)
    
    imports = imports.rename(columns = {'node_loc': 'IMPORTER',
                                        'lvl': 'LEVEL',
                                        'year_act': 'YEAR',
                                        'technololgy': 'MESSAGETEC'})
    imports.to_csv(os.path.join(config_dir, 'diagnostics', base_import_tec + '_' + model_name + '_' + scenario_name + '.csv'),
                   index = False)

# Retrieve trade flow activities
# activity_to_csv(base_export_tec = 'gas_exp', base_import_tec = 'gas_imp',
#                 model_name = 'NP_SSP2', scenario_name = 'base_scenario')
# activity_to_csv(base_export_tec = 'gas_exp', base_import_tec = 'gas_imp',
#                 model_name = 'NP_SSP2', scenario_name = 'test')

# Build Sankey
model_name = 'NP_SSP2',
scenario_name = 'test'
base_export_tec = 'gas_exp'

def build_sankey(base_export_tec,
                 model_name, scenario_name):
    scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    
    activity = scen.var("ACT")
    activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
    activity = activity.groupby(['node_loc', 'technology', 'year_act'])['lvl'].sum().reset_index()
    
    sdf = activity[(activity['technology'].str.contains(base_export_tec))].copy()
    
    sdf['IMPORTER'] = 'R12_' + sdf['technology'].str.upper().str.split('_').str[2]
    sdf = sdf.rename(columns = {'node_loc': 'EXPORTER',
                                'lvl': 'LEVEL',
                                'year_act': 'YEAR',
                                'technology': 'MESSAGETEC'})
    sdf = sdf[['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGETEC', 'LEVEL']]
    
    unique_source = list(set(list(sdf['EXPORTER']) + list(sdf['IMPORTER'])))
    mapdict = {k: v for v, k in enumerate(unique_source)}
    linksdf = sdf[sdf['YEAR'] == 2030].copy()
    linksdf = linksdf[['EXPORTER', 'IMPORTER', 'LEVEL']]
    linksdf = linksdf.rename(columns = {'EXPORTER': 'source',
                                        'IMPORTER': 'target',
                                        'LEVEL': 'value'})
    
    linksdf['source'] = linksdf['source'].map(mapdict)
    linksdf['target'] = linksdf['target'].map(mapdict)
    link_dict = linksdf.to_dict(orient = 'list')
    
    #Sankey Diagram Code 
    fig = go.Figure(data=[go.Sankey(
        node = dict(
            pad = 15,
            thickness = 20,
            line = dict(color = "black", width = 0.5),
            label = unique_source,),
        link = dict(
            source = link_dict["source"],
            target = link_dict["target"],
            value = link_dict["value"],
      ))])
    
    fig.update_layout(title_text="TEST", font_size=10,width=1000, height=600)
    fig.write_image(os.path.join(config_dir, 'diagnostics', 'gas_exp' + '_' + 'NP_SSP2' + '_' + 'base_scenario' + '.png'))
