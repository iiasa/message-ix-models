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

from message_ix_models.project.weu_security.reporting.config import Config
from message_ix_models.project.weu_security.reporting.supply_reporting import load_config, pyam_df_from_rep

# Full reporting output for gas supply
def infrastructure_reporting(rep: Reporter, scenario: message_ix.Scenario) -> pd.DataFrame:
    infra_config = load_config("infrastructure_trade")
    full_df = pd.DataFrame()
    for var in ['out']:
        rdf = pyam_df_from_rep(rep, scenario, var, infra_config.mapping)
        rdf = rdf.reset_index()
        rdf = rdf.drop_duplicates()
        #rdf = rdf[rdf['iamc_name'].str.split('|').str[4] != '']
        full_df = pd.concat([full_df, rdf])
    df = full_df.copy().reset_index()
    df = df.rename(columns = {0:'value'})
    df['model'] = scenario.model
    df['scenario'] = scenario.scenario
    df['exporter'] = df['nl']
    df['importer'] = df['iamc_name'].str.split('|').str[4]
    df['variable'] = df['iamc_name']
    df['year'] = df['ya']

    df['unit'] = 'km' # pipelines
    df['unit'] = np.where(df['variable'].str.contains('Shipping'), 'Mt-km', df['unit']) # tankers

    df = df[['model', 'scenario', 'exporter', 'importer', 'variable', 'unit', 'year', 'value']]
    df = df.drop_duplicates()

    return df 

# Call reporter
mp = ixmp.Platform()

model_scenarios = [('weu_security', 'SSP2_ppcost')]

infra_out = pd.DataFrame()
for mod, scen in model_scenarios:
    print(f"COMPILING {mod}/{scen}")
    print(f"--------------------------------")
    scenario = message_ix.Scenario(mp, model = mod, scenario = scen)
    rep = Reporter.from_scenario(scenario)

    # Collect all gas supply reporting
    infra_df = infrastructure_reporting(rep, scenario)
    infra_out = pd.concat([infra_out, infra_df])

    infra_out['infrastructure'] = "Pipeline"
    infra_out['infrastructure'] = np.where(infra_out['variable'].str.contains('Shipping'), "Shipping", infra_out['infrastructure'])

    infra_out.to_csv(package_data_path('weu_security', 'reporting', 'infrastructure_out.csv'))

mp.close_db()