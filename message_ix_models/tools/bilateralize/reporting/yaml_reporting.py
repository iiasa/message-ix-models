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

from message_ix_models.tools.bilateralize.reporting.yaml_config import Config

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
        df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-nd-t-ya-m-c-l"))
        df = (
                df_var.join(mapping_df[["iamc_name", "unit"]])
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
    else:
        df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-t-ya-m-c-l"))
        df = (
            df_var.join(mapping_df[["iamc_name", "unit"]])
            .dropna()
            .groupby(["nl", "ya", "iamc_name"])
            .sum(numeric_only=True)
        ) 
        
    rep.set_filters()
    
    return df

# Call reporter
mp = ixmp.Platform()

outdf = pd.DataFrame()
for mod, scen in [('alps_hhi', 'SSP2'),
                  ('alps_hhi', 'SSP2_hhi_HC'),
                  ('alps_hhi', 'SSP2_hhi_WS')]:
                  #('SSP_SSP2_v6.2', 'baseline')]:
    scenario = message_ix.Scenario(mp, model = mod, scenario = scen)
    rep = Reporter.from_scenario(scenario)
    #base_df = rep.get("message::default").data 
    supply_config = load_config('gas_supply')
    df = pyam_df_from_rep(rep, supply_config.var, supply_config.mapping)
    df = df.reset_index()
    df = df.rename(columns = {0:'value'})
    df['model'] = scenario.model
    df['scenario'] = scenario.scenario
    df['region'] = df['nl']
    df['variable'] = df['iamc_name']
    df['year'] = df['ya']
    df['value'] = df['value'] * .03154
    df['unit'] = 'EJ/yr'
    df = df[['model', 'scenario', 'region', 'variable', 'unit', 'year', 'value']]
    
    # Add HHI
    df_tot_com = df.groupby(['model', 'scenario', 'region', 'unit', 'year'])['value'].sum().reset_index()
    df_tot_com = df_tot_com.rename(columns = {'value': 'total_com'})
    df_hhi = df.merge(df_tot_com, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left').reset_index()
    df_hhi['HHI'] = (df_hhi['value'] / df_hhi['total_com'])**2
    df_hhi = df_hhi.groupby(['model', 'scenario', 'region', 'unit', 'year'])['HHI'].sum().reset_index()

    df = df.merge(df_hhi, on = ['model', 'scenario', 'region', 'unit', 'year'], how = 'left')
    
    # Add labels
    df['supply_type'] = np.where(df['variable'].str.contains('Domestic'), 'Domestic', 'Imports')
    df['fuel_type'] = np.where(df['variable'].str.contains('LNG'), 'LNG', 'Gas')

    outdf = pd.concat([outdf, df])

outdf.to_csv(package_data_path('bilateralize', 'reporting', 'reporting.csv'))

mp.close_db()