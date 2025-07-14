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
#from message_data.tools.post_processing import iamc_report_hackathon
from ixmp import Platform

# Connect to ixmp
mp = ixmp.Platform()

full_path = package_data_path("bilateralize", "config.yaml")
config_dir = os.path.dirname(full_path)

# Trade Reporter
def activity_to_csv(trade_tec,
                    flow_tec,
                    trade_commodity,
                    flow_commodity,
                    model_scenario_dict,
                    first_model_year = 2030):
    
    mp = ixmp.Platform()
    
    exports_out = pd.DataFrame(); imports_out = pd.DataFrame(); flows_out = pd.DataFrame()
    for model_name in model_scenario_dict.keys():
        scenario_name = model_scenario_dict[model_name]
        scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
        
        activity = scen.var("ACT")
        activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
        activity = activity[activity['year_act'] >= first_model_year]
        activity['activity_type'] = 'modeled'
        
        inputdf = scen.par("input")
        inputdf = inputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()
        
        outputdf = scen.par("output")
        outputdf = outputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()
        
        hist_activity = scen.par('historical_activity')
        hist_activity = hist_activity[['node_loc', 'technology', 'year_act', 'value']].drop_duplicates().reset_index()
        hist_activity = hist_activity.rename(columns = {'value': 'lvl'})
        hist_activity['activity_type'] = 'historical'
        activity = pd.concat([hist_activity, activity])

        activity['MODEL'] = model_name
        activity['SCENARIO'] = scenario_name
        
        act_in = activity.merge(inputdf, 
                                 left_on = ['node_loc', 'technology', 'year_act'],
                                 right_on = ['node_loc', 'technology', 'year_act'],
                                 how = 'left')
        act_in['value'] = np.where((act_in['activity_type'] == 'historical')&(act_in['value'].isnull()), 1, act_in['value'])
        act_in['level'] = act_in['lvl']*act_in['value']
        act_in = act_in.groupby(['MODEL', 'SCENARIO', 'node_loc', 'technology', 'commodity', 'year_act', 'unit'])['level'].sum().reset_index()
        
        act_out = activity.merge(outputdf,
                                 left_on = ['node_loc', 'technology', 'year_act'],
                                 right_on = ['node_loc', 'technology', 'year_act'],
                                 how = 'left')
        act_out['value'] = np.where((act_out['activity_type'] == 'historical')&(act_out['value'].isnull()), 1, act_out['value'])
        act_out['level'] = act_out['lvl']*act_out['value']
        act_out = act_out.groupby(['MODEL', 'SCENARIO', 'node_loc', 'technology', 'commodity', 'year_act', 'unit'])['level'].sum().reset_index()
            
        exports = act_in[(act_in['technology'].str.contains(trade_tec + '_exp'))].copy()
        imports = act_in[(act_in['technology'].str.contains(trade_tec + '_imp'))].copy()
        flow_input = act_in[(act_in['technology'].str.contains(flow_tec))].copy()
        flow_output = act_out[(act_out['technology'].str.contains(flow_tec))].copy()
        
        exports['IMPORTER'] = 'R12_' + exports['technology'].str.upper().str.split('_').str[-1]
        exports = exports.rename(columns = {'node_loc': 'EXPORTER',
                                            'level': 'LEVEL',
                                            'year_act': 'YEAR',
                                            'technology': 'MESSAGETEC',
                                            'unit': 'UNITS'})
        exports = exports[['MODEL', 'SCENARIO', 'YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
        exports['COMMODITY'] = trade_commodity
        exports_out = pd.concat([exports_out, exports])
        
        imports = imports.rename(columns = {'node_loc': 'IMPORTER',
                                            'level': 'LEVEL',
                                            'year_act': 'YEAR',
                                            'technology': 'MESSAGETEC',
                                            'unit': 'UNITS'})
        imports = imports[['MODEL', 'SCENARIO', 'YEAR', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
        imports['COMMODITY'] = trade_commodity
        imports_out = pd.concat([imports_out, imports])
        
        for flowdf in [flow_input, flow_output]:
            
            flowdf['IMPORTER'] = 'R12_' + flowdf['technology'].str.upper().str.split('_').str[-1]
            flowdf = flowdf.rename(columns = {'node_loc': 'EXPORTER',
                                              'level': 'LEVEL',
                                              'year_act': 'YEAR',
                                              'technology': 'MESSAGETEC',
                                              'commodity': 'COMMODITY',
                                              'unit': 'UNITS'})
            flowdf = flowdf[['MODEL', 'SCENARIO', 'YEAR', 'EXPORTER', 'IMPORTER', 
                             'MESSAGETEC', 'COMMODITY', 'LEVEL', 'UNITS']]
            flows_out = pd.concat([flows_out, flowdf])
        
        
    exports_out.to_csv(os.path.join(config_dir, 'diagnostics', trade_tec + '_exp.csv'),
                       index = False)
    imports_out.to_csv(os.path.join(config_dir, 'diagnostics', trade_tec + '_imp.csv'),
                       index = False)
    flows_out.to_csv(os.path.join(config_dir, 'diagnostics', flow_tec + '.csv'),
                     index = False)
    
# Retrieve trade flow activities
models_scenarios = {'NP_SSP2': 'pipelines_only'}
 
activity_to_csv(trade_tec = "gas_piped", 
                flow_tec = "gas_piped_pipe",
                trade_commodity = 'gas (GWa)',
                flow_commodity = 'gas pipeline (km)',
                model_scenario_dict = models_scenarios)

# Build Sankey
def build_sankey(base_export_tec, year_list,
                 model_name, scenario_name,
                 out_title):
    
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
    
    for year in year_list:
        linksdf = sdf[sdf['YEAR'] == year].copy()
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
        
        fig.update_layout(title_text=out_title + '(' + str(year) + ')', font_size=10,width=1000, height=600)
        fig.write_image(os.path.join(config_dir, 'diagnostics', 'figures', base_export_tec + '_' + model_name + '_' + scenario_name + '_' + str(year) + '.png'))
    
    
# build_sankey(base_export_tec = "gas_exp", year_list = [2030, 2040, 2050, 2100],
#              model_name = "NP_SSP2", scenario_name = "pipelines_only",
#              out_title = "Pipeline Gas, after bilateralization ")