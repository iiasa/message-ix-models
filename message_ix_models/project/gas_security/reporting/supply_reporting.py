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

from message_ix_models.project.gas_security.reporting.config import Config

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
def fuel_supply_reporting(rep: Reporter, scenario: message_ix.Scenario, config_name: str) -> pd.DataFrame:
    supply_config = load_config(config_name)
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

    return df 

# Call reporter
mp = ixmp.Platform()

fuel_supply_out = pd.DataFrame()
for mod, scen in [('gas_security', 'SSP2'),
                  ('gas_security', 'FSU2040'),
                  ('gas_security', 'FSU2100'),
                  ('gas_security', 'NAM500'),
                  ('gas_security', 'NAM1000'),
                  ('gas_security', 'FSU2040_NAM500'),
                  ('gas_security', 'FSU2040_NAM1000'),
                  ('gas_security', 'FSU2100_NAM500'),
                  ('gas_security', 'FSU2100_NAM1000'),
                  ]:
    print(f"COMPILING {mod}/{scen}")
    print(f"--------------------------------")
    scenario = message_ix.Scenario(mp, model = mod, scenario = scen)
    rep = Reporter.from_scenario(scenario)
    
    # Collect all gas supply reporting
    for fuel in ['biomass', 'coal', 'crude', 'ethanol', 'fueloil', 'gas', 'h2', 'lightoil', 'methanol']:
        fuel_supply_df = fuel_supply_reporting(rep, scenario, f'{fuel}_supply')
        fuel_supply_out = pd.concat([fuel_supply_out, fuel_supply_df])

    fuel_supply_out['fuel_type'] = fuel_supply_out['variable'].str.split('|').str[0]
    fuel_supply_out['fuel_type'] = fuel_supply_out['fuel_type'].str.replace(' Supply', '')

    fuel_supply_out['supply_type'] = fuel_supply_out['variable'].str.split('|').str[1]
    fuel_supply_out['exporter'] = np.where(fuel_supply_out['supply_type'] == 'Imports',
                                            fuel_supply_out['variable'].str.split('|').str[-1], '')
    fuel_exports = fuel_supply_out.copy()
    fuel_exports = fuel_exports[['exporter', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'value', 'year']]
    fuel_exports = fuel_exports.groupby(['exporter', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'year'])['value'].sum().reset_index()
    fuel_exports = fuel_exports[fuel_exports['supply_type'] == 'Imports']
    fuel_exports['variable'] = 'Exports|' + fuel_exports['fuel_type']
    fuel_exports = fuel_exports.rename(columns = {'exporter': 'region'})
    fuel_exports['exporter'] = ''
    fuel_exports['supply_type'] = 'Exports'
    fuel_exports['value'] *= -1 # Set to negative

    fuel_supply_out = fuel_supply_out[['region', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'value', 'variable', 'exporter', 'year']].drop_duplicates()
    fuel_exports = fuel_exports[['region', 'fuel_type', 'model', 'scenario', 'supply_type', 'unit', 'value', 'variable', 'exporter', 'year']].drop_duplicates()
    fuel_supply_out = pd.concat([fuel_supply_out, fuel_exports])

fuel_supply_out.to_csv(package_data_path('gas_security', 'reporting', 'fuel_supply_out.csv'))

mp.close_db()