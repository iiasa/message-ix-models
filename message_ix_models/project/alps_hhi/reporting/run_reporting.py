# -*- coding: utf-8 -*-
"""
HHI reporting
"""
from typing import List

import message_ix
import numpy as np
import pandas as pd
import pyam
import ixmp

from message_ix.report import Reporter
from message_ix_models.util import broadcast, package_data_path

from message_ix_models.project.alps_hhi.reporting.config import Config

def load_config(name: str) -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(name)

def pyam_df_from_rep(
    rep: message_ix.Reporter, reporter_var: str, mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """Queries data from Reporter and maps to IAMC variable names.

    Parameters
    ----------
    rep
        message_ix.Reporter to query
    reporter_var
        Registered key of Reporter to query, e.g. "out", "in", "ACT", "emi", "CAP"
    mapping_df
        DataFrame mapping Reporter dimension values to IAMC variable names
    """
    filters_dict = {
        col: list(mapping_df.index.get_level_values(col).unique())
        for col in mapping_df.index.names
    }
    base_tec_list = filters_dict['t']
    new_tec_list = [v for v in scenario.set('technology')
                        if any(v.startswith(prefix) for prefix in base_tec_list)]
    filters_dict['t'] = new_tec_list
    
    for bt in base_tec_list:
        base_index = mapping_df.index[mapping_df.index.get_level_values('t') == bt] #gas_piped_exp
        for nt in [i for i in new_tec_list if bt in i]:
            add_index = [(*item[:-1], nt) for item in base_index]
            add_index = pd.MultiIndex.from_tuples(add_index, names = mapping_df.index.names)
            new_rows = mapping_df.loc[base_index].copy()
            new_rows.index = add_index
            mapping_df = pd.concat([mapping_df, new_rows])
        if bt not in new_tec_list:
            mapping_df = mapping_df.drop(base_index)
    
    rep.set_filters(**filters_dict)
    
    if reporter_var == 'out':
        df_hist = pd.DataFrame(rep.get(f"out:nl-nd-t-ya-yv-m-c-l:historical+current"))
        df_model = pd.DataFrame(rep.get(f"out:nl-nd-t-ya-m-c-l"))

        df_out = pd.DataFrame()
        for dfv in [df_hist, df_model]:
            df = (
                    dfv.join(mapping_df[["iamc_name", "unit"]])
                    .dropna()
                    .groupby(["nl", "nd", "ya", "iamc_name"])
                    .sum(numeric_only=True)
                )
            # Adjust df to include exporters in iamc_name for trade variables
            dfn = df.index.to_frame(index = False)
            ndiff = dfn['nl'] != dfn['nd']
            dfn.loc[ndiff, 'iamc_name'] = dfn.loc[ndiff, 'iamc_name'] + dfn.loc[ndiff, 'nl']
            dfn.loc[ndiff, 'nl'] = dfn.loc[ndiff, 'nd'] # We are looking at imports to dest
            df.index = pd.MultiIndex.from_frame(dfn)
            df_out = pd.concat([df_out, df])
    else:
        df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-t-ya-m-c-l"))
        df_out = (
            df_var.join(mapping_df[["iamc_name", "unit"]])
            .dropna()
            .groupby(["nl", "ya", "iamc_name"])
            .sum(numeric_only=True)
        ) 
        
    rep.set_filters()
    
    return df_out

# Full reporting output for gas supply
def gas_supply_reporting(rep: Reporter, scenario: message_ix.Scenario) -> pd.DataFrame:
    supply_config = load_config('gas_supply')
    full_df = pd.DataFrame()
    for var in ['out']:
        rdf = pyam_df_from_rep(rep, var, supply_config.mapping)
        rdf = rdf.reset_index()
        rdf = rdf.drop_duplicates()
        full_df = pd.concat([full_df, rdf])
    df = full_df.copy().reset_index()
    df = df.rename(columns = {0:'value'})
    df['model'] = scenario.model
    df['scenario'] = scenario.scenario
    df['region'] = df['nl']
    df['variable'] = df['iamc_name']
    df['year'] = df['ya']
    df['value'] = df['value'] * .03154
    df['unit'] = 'EJ/yr'
    df = df[['model', 'scenario', 'region', 'variable', 'unit', 'year', 'value']]
    df = df.drop_duplicates()

    # Add HHI
    df_tot_com = df.groupby(['model', 'scenario', 'region', 'unit', 'year'])['value'].sum().reset_index()
    df_tot_com = df_tot_com.rename(columns = {'value': 'total_com'})
    df_hhi = df.merge(df_tot_com, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left').reset_index()
    df_hhi['HHI'] = (df_hhi['value'] / df_hhi['total_com'])**2
    df_hhi = df_hhi.groupby(['model', 'scenario', 'region', 'unit', 'year'])['HHI'].sum().reset_index()

    # Add share
    df_tot = df.groupby(['model', 'scenario', 'region', 'unit', 'year'])['value'].sum().reset_index()
    df_tot = df_tot.rename(columns = {'value': 'total'})
    df = df.merge(df_tot, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left')
    df['share'] = df['value'] / df['total']
    df.drop(columns = ['total'], inplace = True)
    
    df = df.merge(df_hhi, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left')
    
    # Add labels
    df['supply_type'] = np.where(df['variable'].str.contains('Domestic'), 'Domestic', 'Imports')
    df['fuel_type'] = np.where(df['variable'].str.contains('LNG'), 'LNG', 'Piped Gas')

    return df 

# Call reporter
mp = ixmp.Platform()

gas_supply_out = pd.DataFrame()
for mod, scen in [('alps_hhi', 'SSP_SSP2_v6.2'),

                  ('alps_hhi', 'SSP2'),
                  ('alps_hhi', 'SSP2_nopcosts'), #REMOVE LATER

                  ('alps_hhi', 'SSP2_hhi_HC_supply'),
                  ('alps_hhi', 'SSP2_hhi_WS_l90p_supply'),

                  ('alps_hhi', 'SSP2_hhi_HC_imports'),
                  ('alps_hhi', 'SSP2_hhi_WS_l90p_imports'),

                  ('alps_hhi', 'SSP2_FSU_EUR_2110'),
                  ('alps_hhi', 'SSP2_FSU_EUR_2040'),

                  ('alps_hhi', 'SSP2_hhi_HC_imports_FSU_EUR_2110'),
                  ('alps_hhi', 'SSP2_hhi_WS_l90p_supply_FSU_EUR_2110'),

                  ('alps_hhi', 'SSP2_FSU_EUR_NAM_PAO_2110'),
                  
                  ]:
    print(f"COMPILING {mod}/{scen}")
    print(f"--------------------------------")
    scenario = message_ix.Scenario(mp, model = mod, scenario = scen)
    rep = Reporter.from_scenario(scenario)
    
    # Collect all gas supply reporting
    gas_supply_df = gas_supply_reporting(rep, scenario)
    gas_supply_out = pd.concat([gas_supply_out, gas_supply_df])

gas_supply_out['variable'] = gas_supply_out['variable'].str.replace('Gas Supply|Domestic|', 'Domestic|')
gas_supply_out['variable'] = gas_supply_out['variable'].str.replace('Gas Supply|Imports|', '')

gas_supply_out['exporter'] = ''
gas_supply_out['exporter'] = np.where(gas_supply_out['variable'].str.contains('Piped Gas'),
                                      gas_supply_out['variable'].str.replace('Piped Gas|', ''),
                                      gas_supply_out['exporter'])
gas_supply_out['exporter'] = np.where(gas_supply_out['variable'].str.contains('Shipped LNG'),
                                      gas_supply_out['variable'].str.replace('Shipped LNG|', ''),
                                      gas_supply_out['exporter'])

gas_supply_out['legend'] = gas_supply_out['exporter'] + ' (' + gas_supply_out['fuel_type'] + ')'
gas_supply_out['legend'] = np.where(gas_supply_out['variable'].str.contains('Domestic'),
                                    "Domestic (" + gas_supply_out['variable'].str.replace('Domestic|', '') + ")",
                                    gas_supply_out['legend'])

gas_supply_out_tot = gas_supply_out.groupby(['model', 'scenario', 'region', 'unit', 'year'])['value'].sum().reset_index()
gas_supply_out_tot = gas_supply_out_tot.rename(columns = {'value': 'total'})
gas_supply_out = gas_supply_out.merge(gas_supply_out_tot, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left')

gas_exports = gas_supply_out.copy()
gas_exports = gas_exports[['exporter', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'value', 'year']]
gas_exports = gas_exports.groupby(['exporter', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'year'])['value'].sum().reset_index()
gas_exports = gas_exports[gas_exports['supply_type'] == 'Imports']
gas_exports['variable'] = 'Exports|' + gas_exports['fuel_type']
gas_exports = gas_exports.rename(columns = {'exporter': 'region'})
gas_exports['exporter'] = ''
gas_exports['supply_type'] = 'Exports'
gas_exports['value'] *= -1 # Set to negative

gas_supply_out = gas_supply_out[['exporter', 'fuel_type', 'model', 'region', 'scenario', 'supply_type', 'unit', 'value', 'variable', 'year']]
gas_exports = gas_exports[['exporter', 'fuel_type', 'model', 'region', 'scenario', 'supply_type', 'unit', 'value', 'variable', 'year']]
gas_supply_out = pd.concat([gas_supply_out, gas_exports])

gas_supply_out.to_csv(package_data_path('alps_hhi', 'reporting', 'reporting.csv'))

# Compare across scenarios
gas_supply_out = pd.read_csv(package_data_path('alps_hhi', 'reporting', 'reporting.csv'))
gas_wide = gas_supply_out[['model', 'scenario', 'region',
                           'unit', 'year', 'variable', 'value']].drop_duplicates().copy()
gas_wide = gas_wide.pivot_table(index = ['model', 'region', 'unit', 'year', 'variable'],
                                columns = 'scenario',
                                values = 'value',
                                aggfunc = 'sum')
gas_wide = gas_wide.reset_index()

gas_wide.to_csv(package_data_path('alps_hhi', 'reporting', 'reporting_wide.csv'))

mp.close_db()