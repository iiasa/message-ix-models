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
from datetime import datetime

from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.util import package_data_path
from ixmp import Platform

# Connect to ixmp
mp = ixmp.Platform()

config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')
out_path = config_path.replace('config.yaml', 'diagnostics')

#full_path = package_data_path("bilateralize", "config.yaml")
#config_dir = os.path.dirname(full_path)

# Trade Reporter
def activity_to_csv(trade_tec,
                    flow_tec,
                    trade_commodity,
                    flow_commodity,
                    flow_unit,
                    model_scenario_dict,
                    first_model_year = 2030):
    
    mp = ixmp.Platform()
    
    exports_out = pd.DataFrame(); imports_out = pd.DataFrame(); flows_out = pd.DataFrame()
    
    for scenario_name in model_scenario_dict.keys():
        model_name = model_scenario_dict[scenario_name]
        
        print("COMPILING SCENARIO [" + scenario_name + "]")
        print("COMPILING MODEL [" + model_name + "]")
        
        scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
        
        activity = scen.var("ACT")
        activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
        activity = activity[activity['year_act'] >= first_model_year]
        activity['activity_type'] = 'modeled'
        
        inputdf = scen.par("input")
        inputdf = inputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()
        
        outputdf = scen.par("output")
        outputdf = outputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()
        
        capacity = scen.var("CAP_NEW")
        capacity = capacity[['node_loc', 'technology', 'year_vtg', 'lvl']].drop_duplicates()
        capacity = capacity.rename(columns = {'lvl': 'level'})
        capacity['commodity'] = flow_commodity
        capacity['unit'] = flow_unit
        
        hist_activity = scen.par('historical_activity')
        hist_activity = hist_activity[['node_loc', 'technology', 'year_act', 'value']].drop_duplicates().reset_index()
        hist_activity = hist_activity.rename(columns = {'value': 'lvl'})
        hist_activity['activity_type'] = 'historical'
        activity = pd.concat([hist_activity, activity])

        activity['MODEL'] = capacity['MODEL'] = model_name
        activity['SCENARIO'] = capacity['SCENARIO'] = scenario_name
        
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
            
        exports = act_in[(act_in['technology'].str.contains(trade_tec))&\
                         (act_in['technology'].str.contains('_exp'))].copy()
        imports = act_in[(act_in['technology'].str.contains(trade_tec))&\
                         (act_in['technology'].str.contains('_imp'))].copy()
         
        flow_fuel = act_in[(act_in['technology'].str.contains(flow_tec)) &\
                           (act_in['technology'].str.contains('_exp') == False) &\
                           (act_in['technology'].str.contains('_imp') == False)].copy()
        flow_fuel['VARIABLETYPE'] = 'Fuel Input'
        
        flow_newcap = capacity[(capacity['technology'].str.contains(flow_tec)) &\
                               (capacity['technology'].str.contains('_exp') == False) &\
                               (capacity['technology'].str.contains('_imp') == False)].copy()
        flow_newcap['VARIABLETYPE'] = 'New Capacity'

        flow_output = exports[exports['commodity'].str.contains(flow_commodity)].copy()
        flow_output['VARIABLETYPE'] = 'Flow Activity'
        
        exports['IMPORTER'] = 'R12_' + exports['technology'].str.upper().str.split('_').str[-1]
        exports = exports.rename(columns = {'node_loc': 'EXPORTER',
                                            'level': 'LEVEL',
                                            'year_act': 'YEAR',
                                            'technology': 'MESSAGETEC',
                                            'unit': 'UNITS'})
        exports = exports[exports['UNITS'] == 'GWa']
        exports = exports[['MODEL', 'SCENARIO', 'YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
        exports['COMMODITY'] = trade_commodity
        exports_out = pd.concat([exports_out, exports])
        
        imports = imports.rename(columns = {'node_loc': 'IMPORTER',
                                            'level': 'LEVEL',
                                            'year_act': 'YEAR',
                                            'technology': 'MESSAGETEC',
                                            'unit': 'UNITS'})
        imports = imports[['MODEL', 'SCENARIO', 'YEAR', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
        imports = imports[imports['UNITS'] == 'GWa']
        imports['COMMODITY'] = trade_commodity
        imports_out = pd.concat([imports_out, imports])
        
        for flowdf in [flow_fuel, flow_newcap, flow_output]:
            
            flowdf['IMPORTER'] = 'R12_' + flowdf['technology'].str.upper().str.split('_').str[-1]
            flowdf = flowdf.rename(columns = {'node_loc': 'EXPORTER',
                                              'level': 'LEVEL',
                                              'year_act': 'YEAR',
                                              'year_vtg': 'YEAR',
                                              'technology': 'MESSAGETEC',
                                              'commodity': 'COMMODITY',
                                              'unit': 'UNITS'})
            flowdf = flowdf[['MODEL', 'SCENARIO', 'VARIABLETYPE', 'YEAR', 'EXPORTER', 'IMPORTER', 
                             'MESSAGETEC', 'COMMODITY', 'LEVEL', 'UNITS']]
            flowdf['PAIR'] = flowdf['EXPORTER'] + '-' + flowdf['IMPORTER']
            flows_out = pd.concat([flows_out, flowdf])
        
        exports['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')
        imports_out['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')
        flows_out['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')

        exports_out.to_csv(os.path.join(out_path, 'data', trade_tec + '_exp.csv'),
                           index = False)
        imports_out.to_csv(os.path.join(out_path, 'data', trade_tec + '_imp.csv'),
                           index = False)
        flows_out.to_csv(os.path.join(out_path, 'data', flow_tec + '.csv'),
                         index = False)
    
# Retrieve trade flow activities
scenarios_models = {#'base_scenario': 'NP_SSP2_6.2',
                    'pipelines_LNG': 'NP_SSP2_6.2',
                    'HHI_0.9_gas_CHN': 'NP_SSP2_6.2'}
 
activity_to_csv(trade_tec = "gas", 
                flow_tec = "gas_pipe",
                trade_commodity = 'gas (GWa)',
                flow_commodity = 'gas_pipeline_capacity',
                flow_unit = 'km',
                model_scenario_dict = scenarios_models)

activity_to_csv(trade_tec = "LNG", 
                flow_tec = "LNG_tanker",
                trade_commodity = 'LNG (GWa)',
                flow_commodity = 'LNG_tanker_capacity',
                flow_unit = 'Mt-km',
                model_scenario_dict = scenarios_models)
