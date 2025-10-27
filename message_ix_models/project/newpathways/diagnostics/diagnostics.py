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

# Trade Reporter
def import_model_data(
    model_name: str, 
    scenario_name: str,
    first_model_year: int = 2030):
    """Import model data from a scenario
    Args:
        model_name: str, name of the model
        scenario_name: str, name of the scenario
    Returns:
        act_in: pandas.DataFrame, input activity
        act_out: pandas.DataFrame, output activity
        capacity: pandas.DataFrame, capacity
    """

    mp = ixmp.Platform()
    scen = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    
    # Activity
    activity = scen.var("ACT")
    activity = activity[['node_loc', 'technology', 'year_act', 'lvl']].drop_duplicates().reset_index()
    activity = activity[activity['year_act'] >= first_model_year]
    activity['activity_type'] = 'modeled'

    # Input    
    inputdf = scen.par("input")
    inputdf = inputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()
    
    # Output
    outputdf = scen.par("output")
    outputdf = outputdf[['node_loc', 'technology', 'commodity', 'year_act', 'value', 'unit']].drop_duplicates().reset_index()

    # Capacity
    capacity = scen.var("CAP_NEW")
    capacity = capacity[['node_loc', 'technology', 'year_vtg', 'lvl']].drop_duplicates()
    capacity = capacity.rename(columns = {'lvl': 'level'})
        
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
    act_in['level'] = round(act_in['level'], 1)
        
    act_out = activity.merge(outputdf,
                                left_on = ['node_loc', 'technology', 'year_act'],
                                right_on = ['node_loc', 'technology', 'year_act'],
                                how = 'left')
    act_out['value'] = np.where((act_out['activity_type'] == 'historical')&(act_out['value'].isnull()), 1, act_out['value'])
    act_out['level'] = act_out['lvl']*act_out['value']
    act_out = act_out.groupby(['MODEL', 'SCENARIO', 'node_loc', 'technology', 'commodity', 'year_act', 'unit'])['level'].sum().reset_index()
    act_out['level'] = round(act_out['level'], 1)

    return act_in, act_out, capacity

def trade_flow_dicts(
    model_dict: dict,
    trade_flows: dict):
    """Export trade flow activities to csv
    Args:
        trade_flows: dict, trade flows
    Returns:
        None
    """
    exports_out = dict.fromkeys(trade_flows.keys(), pd.DataFrame()) 
    imports_out = dict.fromkeys(trade_flows.keys(), pd.DataFrame()) 
    flows_out = dict.fromkeys(trade_flows.keys(), pd.DataFrame())

    for scenario_name, model_name in model_dict.items():
        print("COMPILING SCENARIO [" + scenario_name + "]")
        print("COMPILING MODEL [" + model_name + "]")
        act_in, act_out, capacity = import_model_data(model_name, scenario_name)

        for trade_tec in trade_flows.keys():
            print("COMPILING TRADE TECHNOLOGY [" + trade_tec + "]")
            exp_technologies = [t for t in act_in['technology'].unique() if "_exp" in t] # If trade tec not bilateralized, use base trade name
            if any(trade_tec in i for i in exp_technologies) == False:
                trade_tec_use = trade_tec.replace("_shipped", "")
                trade_tec_use = trade_tec_use.replace("_piped", "")
            else:
                trade_tec_use = trade_tec
                
            exports = act_in[(act_in['technology'].str.contains(trade_tec_use))&\
                            (act_in['technology'].str.contains('_exp'))].copy()
            imports = act_in[(act_in['technology'].str.contains(trade_tec_use))&\
                            (act_in['technology'].str.contains('_imp'))].copy()
            
            if (trade_tec == 'crudeoil_shipped') & (scenario_name == 'base_scenario'):
                exports = act_in[act_in['technology'] == 'oil_exp'].copy()
                imports = act_in[act_in['technology'] == 'oil_imp'].copy()
                    
            flow_fuel = act_in[(act_in['technology'].str.contains(trade_flows[trade_tec]['flow_tec'])) &\
                            (act_in['technology'].str.contains('_exp') == False) &\
                            (act_in['technology'].str.contains('_imp') == False)].copy()
            flow_fuel['VARIABLETYPE'] = 'Fuel Input'
            
            flow_newcap = capacity[(capacity['technology'].str.contains(trade_flows[trade_tec]['flow_tec'])) &\
                                (capacity['technology'].str.contains('_exp') == False) &\
                                (capacity['technology'].str.contains('_imp') == False)].copy()
            flow_newcap['commodity'] = trade_flows[trade_tec]['flow_commodity']
            flow_newcap['unit'] = trade_flows[trade_tec]['flow_unit']
            flow_newcap['VARIABLETYPE'] = 'New Capacity'

            flow_output = exports[exports['commodity'].str.contains(trade_flows[trade_tec]['flow_commodity'])].copy()
            flow_output['VARIABLETYPE'] = 'Flow Activity'
        
            exports['IMPORTER'] = 'R12_' + exports['technology'].str.upper().str.split('_').str[-1]
            exports = exports.rename(columns = {'node_loc': 'EXPORTER',
                                                'level': 'LEVEL',
                                                'year_act': 'YEAR',
                                                'technology': 'MESSAGETEC',
                                                'unit': 'UNITS'})
            exports = exports[exports['UNITS'] == 'GWa']
            exports = exports[['MODEL', 'SCENARIO', 'YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
            exports['COMMODITY'] = trade_flows[trade_tec]['trade_commodity']
            exports_out[trade_tec] = pd.concat([exports_out[trade_tec], exports])
        
            imports = imports.rename(columns = {'node_loc': 'IMPORTER',
                                                'level': 'LEVEL',
                                                'year_act': 'YEAR',
                                                'technology': 'MESSAGETEC',
                                                'unit': 'UNITS'})
            imports = imports[['MODEL', 'SCENARIO', 'YEAR', 'IMPORTER', 'MESSAGETEC', 'LEVEL', 'UNITS']]
            imports = imports[imports['UNITS'] == 'GWa']
            imports['COMMODITY'] = trade_flows[trade_tec]['trade_commodity']
            imports_out[trade_tec] = pd.concat([imports_out[trade_tec], imports])
        
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
                flows_out[trade_tec] = pd.concat([flows_out[trade_tec], flowdf])
        
            exports_out[trade_tec]['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')
            imports_out[trade_tec]['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')
            flows_out[trade_tec]['PULL DATE'] = pd.Timestamp.today().strftime('%Y-%m-%d')
    
    return exports_out, imports_out, flows_out

def export_trade_flow_csv(
    exports_out: dict,
    imports_out: dict,
    flows_out: dict):
    """Export trade flow activities to csv
    Args:
        exports_out: dict, exports
        imports_out: dict, imports
        flows_out: dict, flows
    Returns:
        None
    """
    for trade_tec in trade_flows.keys():
        exports_out[trade_tec].to_csv(os.path.join(out_path, 'data', trade_tec + '_exp.csv'),
                                      index = False)
        imports_out[trade_tec].to_csv(os.path.join(out_path, 'data', trade_tec + '_imp.csv'),
                                      index = False)
        flows_out[trade_tec].to_csv(os.path.join(out_path, 'data', trade_tec + '.csv'),
                                    index = False)

# Dictionaries
scenarios_models = {'base_scenario': 'NP_SSP2_6.2',
                    'nocosts_noconstraints': 'NP_SSP2_6.2',
                    'costs_noconstraints': 'NP_SSP2_6.2',
                    'costs_constraints': 'NP_SSP2_6.2'
                    }
        
trade_flows = {'LNG_shipped': {'trade_commodity': 'LNG (GWa)',
                              'flow_tec': 'LNG_tanker',
                              'flow_commodity': 'LNG_tanker_capacity', 
                              'flow_unit': 'Mt-km'},
              'gas_piped': {'trade_commodity': 'gas (GWa)',
                            'flow_tec': 'gas_pipe',
                            'flow_commodity': 'LNG_tanker_capacity',
                            'flow_unit': 'km'},
              'coal_shipped': {'trade_commodity': 'Coal (GWa)',
                              'flow_tec': 'energy_bulk_carrier',
                              'flow_commodity': 'energy_bulk_carrier_capacity',
                              'flow_unit': 'Mt-km'},
              'crudeoil_piped': {'trade_commodity': 'Crude (GWa)',
                                 'flow_tec': 'oil_pipe',
                                 'flow_commodity': 'oil_pipeline_capacity',
                                 'flow_unit': 'km'},
              'crudeoil_shipped': {'trade_commodity': 'Crude (GWa)',
                                   'flow_tec': 'oil_tanker',
                                   'flow_commodity': 'oil_tanker_capacity',
                                   'flow_unit': 'Mt-km'},
              'eth_shipped': {'trade_commodity': 'Ethanol (GWa)',
                              'flow_tec': 'oil_tanker',
                              'flow_commodity': 'oil_tanker_capacity',
                              'flow_unit': 'Mt-km'},
              'foil_shipped': {'trade_commodity': 'Fuel Oil (GWa)',
                               'flow_tec': 'oil_tanker',
                               'flow_commodity': 'oil_tanker_capacity',
                               'flow_unit': 'Mt-km'},
              'foil_piped': {'trade_commodity': 'Fuel Oil (GWa)',
                             'flow_tec': 'oil_pipe',
                             'flow_commodity': 'oil_pipeline_capacity',
                             'flow_unit': 'km'},
              'loil_shipped': {'trade_commodity': 'Light Oil (GWa)',
                               'flow_tec': 'oil_tanker',
                               'flow_commodity': 'oil_tanker_capacity',
                               'flow_unit': 'Mt-km'},
              'loil_piped': {'trade_commodity': 'Light Oil (GWa)',
                             'flow_tec': 'oil_pipe',
                             'flow_commodity': 'oil_pipeline_capacity',
                             'flow_unit': 'km'},
              'biomass_shipped': {'trade_commodity': 'Biomass (GWa)',
                                   'flow_tec': 'energy_bulk_carrier',
                                   'flow_commodity': 'energy_bulk_carrier_capacity',
                                   'flow_unit': 'Mt-km'},
              'lh2_shipped': {'trade_commodity': 'LH2 (GWa)',
                              'flow_tec': 'lh2_tanker',
                              'flow_commodity': 'lh2_tanker_capacity',
                              'flow_unit': 'Mt-km'}}

exports_dict, imports_dict, flows_dict = trade_flow_dicts(model_dict = scenarios_models,
                                                          trade_flows = trade_flows)
export_trade_flow_csv(exports_out = exports_dict,
                      imports_out = imports_dict,
                      flows_out = flows_dict)