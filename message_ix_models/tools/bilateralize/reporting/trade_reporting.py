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
import os

from message_ix.report import Reporter
from message_ix_models.util import broadcast, package_data_path

from message_ix_models.tools.bilateralize.reporting.config import Config

def load_config(name: str) -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(name)

def pyam_df_from_rep(
    rep: message_ix.Reporter, scenario: message_ix.Scenario, reporter_var: str, mapping_df: pd.DataFrame
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
    base_tec_exp = [v for v in base_tec_list if v.endswith('_exp')]
    base_tec_dom = [v for v in base_tec_list if v not in base_tec_exp]
    
    new_tec_list = [v for v in scenario.set('technology')
                        if any(v.startswith(prefix) for prefix in base_tec_list)]
    filters_dict['t'] = new_tec_list

    for bt in base_tec_list:
        base_index = mapping_df.index[mapping_df.index.get_level_values('t') == bt].drop_duplicates() #gas_piped_exp
        for nt in [i for i in new_tec_list if (bt in base_tec_exp and bt in i) or (bt in base_tec_dom and bt == i)]:
            add_index = [(*item[:-1], nt) for item in base_index]
            add_index = pd.MultiIndex.from_tuples(add_index, names = mapping_df.index.names)
            new_rows = mapping_df.loc[base_index].copy().drop_duplicates()
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
            df = dfv.join(mapping_df[['iamc_name', 'unit']])
            df = (df.dropna()
                  .groupby(["nl", "nd", "ya", "t", "iamc_name"])
                  .sum(numeric_only=True)
                )
            dfn = df.index.to_frame(index = False)
            dfn = dfn.drop(columns = ['t'])

            # Adjust df to include exporters in iamc_name for trade variables
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
def bilat_trade_reporting(rep: Reporter,
                          scenario: message_ix.Scenario,
                          config_name: str,) -> pd.DataFrame:
    supply_config = load_config(config_name)
    full_df = pd.DataFrame()
    for var in ['out']:
        rdf = pyam_df_from_rep(rep, scenario, var, supply_config.mapping)
        rdf = rdf.reset_index()
        rdf = rdf.drop_duplicates()
        full_df = pd.concat([full_df, rdf])
    df = full_df.copy().reset_index()
    df = df.rename(columns = {0:'value'})
    df['Model'] = scenario.model
    df['Scenario'] = scenario.scenario
    df['Region'] = df['nl']
    df['Variable'] = df['iamc_name']
    df['Unit'] = 'EJ/yr'
    df['year'] = df['ya']
    df['value'] = df['value'] * .03154
    df = df[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'year', 'value']]
    df = df.groupby(['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'year'])['value'].sum().reset_index()

    # Make wide
    df = df.pivot(index = ['Model', 'Scenario', 'Region', 'Variable', 'Unit'], columns = 'year', values = 'value')
    df = df.drop_duplicates()
    
    return df 

# Call reporter
def trade_reporting(mp: ixmp.Platform,
                    scenario: message_ix.Scenario,
                    out_dir:str,):

    mp = ixmp.Platform()

    print(f"compiling trade reporting for {scenario.model}/{scenario.scenario}")
    print(f"--------------------------------")

    rep = Reporter.from_scenario(scenario)
    
    primarydf = bilat_trade_reporting(rep, scenario, 'primary_energy_trade')
    secondarydf = bilat_trade_reporting(rep, scenario, 'secondary_energy_trade')
    df = pd.concat([primarydf, secondarydf])

    df.to_csv(os.path.join(out_dir, scenario.model + '_' + scenario.scenario + '.csv'))

    mp.close_db()