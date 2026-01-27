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

from message_ix_models.project.led_china.reporting.config import Config

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
        dfn.loc[ndiff, 'iamc_name'] = dfn.loc[ndiff, 'iamc_name']
        dfn.loc[ndiff, 'nl'] = dfn.loc[ndiff, 'nl'] + ">" +dfn.loc[ndiff, 'nd'] # We are looking at imports to dest
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

#scen = 'SSP2_Baseline'
for scen in ['SSP2_Baseline',]: #'SSP2_2C', 'SSP2_Commitments',
             #'LED_Baseline', 'LED_2C', 'LED_Commitments']:
    scenario = message_ix.Scenario(mp, model = "china_security", scenario = scen)
    rep = Reporter.from_scenario(scenario)
    
    # Gross Imports
    supply_config = load_config('trade')
    df = pyam_df_from_rep(rep, supply_config.var, supply_config.mapping)
    df = df.reset_index()
    df = df.rename(columns = {0:'value'})
    
    df['model'] = scenario.model
    df['scenario'] = scenario.scenario
    df['region'] = df['nl']
    df['variable'] = df['iamc_name']
    df['year'] = df['ya']
    df['unit'] = 'GWa'
    df = df[['model', 'scenario', 'region', 'variable', 'unit', 'year', 'value']]


    # Net imports


    outdf = pd.concat([outdf, imdf, exdf])

outdf.to_csv(package_data_path('led_china', 'reporting', 'reporting.csv'))

mp.close_db()