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

from genno.core.exceptions import MissingKeyError
from message_ix.report import Reporter
from message_ix_models.util import broadcast, package_data_path

from message_ix_models.tools.bilateralize.reporting.config import Config

VALUE_UNIT = "billion US$/yr" # Value unit instead of physical volume unit, which is used in the reporting yaml files. This will be used to convert "out" with PRICE_COMMODITY.

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

    base_tec_set = set(base_tec_list)
    for bt in base_tec_list:
        base_index = mapping_df.index[mapping_df.index.get_level_values('t') == bt].drop_duplicates()
        for nt in [i for i in new_tec_list if (bt in base_tec_exp and bt in i and i != bt and i not in base_tec_set) or (bt in base_tec_dom and bt == i)]:
            new_rows = mapping_df.loc[base_index].copy().drop_duplicates()
            add_index = [(*item[:-1], nt) for item in new_rows.index]
            add_index = pd.MultiIndex.from_tuples(add_index, names=mapping_df.index.names)
            new_rows.index = add_index
            mapping_df = pd.concat([mapping_df, new_rows])
        if bt not in new_tec_list:
            mapping_df = mapping_df.drop(base_index)

    rep.set_filters(**filters_dict)
    
    if reporter_var == 'out':
        try:
            df_hist = pd.DataFrame(rep.get(f"out:nl-nd-t-ya-yv-m-c-l:historical+current"))
        except MissingKeyError:
            df_hist = pd.DataFrame()
        df_model = pd.DataFrame(rep.get(f"out:nl-nd-t-ya-m-c-l"))

        # When VALUE UNIT is used: map entries for monetary trade. Fetch PRICE_COMMODITY once for commodity/level pairs
        # involved and join to 'out'. 
        is_value_entry = mapping_df['unit'] == VALUE_UNIT
        if is_value_entry.any():
            price_df = scenario.var(
                "PRICE_COMMODITY",
                filters={
                    "commodity": list(
                        mapping_df.index.get_level_values('c')[is_value_entry].unique()
                    ),
                    "level": list(
                        mapping_df.index.get_level_values('l')[is_value_entry].unique()
                    ),
                },
            )
            price_df = price_df.rename(
                columns={"node": "nl", "commodity": "c", "level": "l", "year": "ya", "lvl": "price"}
            )[["nl", "c", "l", "ya", "price"]]
        else:
            price_df = pd.DataFrame(columns=["nl", "c", "l", "ya", "price"])

        df_out = pd.DataFrame()
        for dfv in [df_hist, df_model]:
            if dfv.empty:
                continue
            df = dfv.join(mapping_df[['iamc_name', 'unit']]).dropna()

            is_value = (df['unit'] == VALUE_UNIT).to_numpy()
            if is_value.any():
                idx = df.index.to_frame(index=False)
                idx["_row"] = range(len(idx))
                # PRICE_COMMODITY is USD_2010/kWa; `out` is native GWa
                # (1 GWa = 1e6 kWa). Dividing by 1e3 lands on billion US$/yr.
                price = (
                    idx[["_row", "nl", "c", "l", "ya"]]
                    .merge(price_df, on=["nl", "c", "l", "ya"], how="left")
                    .sort_values("_row")["price"]
                    .fillna(0)
                    .to_numpy()
                )
                new_vals = df[0].to_numpy(copy=True)
                new_vals[is_value] = new_vals[is_value] * price[is_value] / 1e3
                df[0] = new_vals

            df = (
                df.groupby(["nl", "nd", "ya", "t", "iamc_name"])
                  .sum(numeric_only=True)
                )
            dfn = df.index.to_frame(index = False)
            dfn = dfn.drop(columns = ['t'])

            # Adjust df to include exporters in iamc_name for trade variables
            ndiff = dfn['nl'] != dfn['nd']
            dfn.loc[ndiff, 'iamc_name'] = dfn.loc[ndiff, 'iamc_name']
            dfn.loc[ndiff, 'nl'] = dfn.loc[ndiff, 'nl'] + ">" + dfn.loc[ndiff, 'nd'] # We are looking at imports to dest
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
    df['Unit'] = df['unit']
    df['year'] = df['ya']
    # Volumes come out of the Reporter in native GWa; convert to EJ/yr. Value
    # rows (Unit == VALUE_UNIT) were already converted in pyam_df_from_rep.
    is_volume = df['Unit'] != VALUE_UNIT
    df.loc[is_volume, 'value'] = df.loc[is_volume, 'value'] * .03154
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