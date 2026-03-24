"""Trade reporting workflow after bilateralization."""

import ixmp
import message_ix
import message_data
import pandas as pd
import os

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.trade_reporting import trade_reporting

mp = ixmp.Platform()

# Legacy reporting (requires message_data branch dev_bilateralize-reporting)
def run_legacy_reporting(scenario:message_ix.Scenario,
                         mp:ixmp.Platform,
                         legacy_config:str,
                         legacy_out_dir:str):
    legacy_report(mp=mp, scen=scenario,
                  run_config=legacy_config,
                  out_dir = legacy_out_dir)

# Full reporting
def report(scenario:message_ix.Scenario,
           mp: ixmp.Platform = ixmp.Platform(),
           add_legacy: bool = True,
           add_trade: bool = True,
           legacy_config:str = package_data_path('bilateralize', 'reporting','legacy', 'legacy_config.yaml'),
           legacy_out_dir:str = package_data_path("bilateralize", "reporting", "output", "legacy"),
           trade_out_dir:str = package_data_path("bilateralize", "reporting", "output", "trade"),
           report_out_dir:str = package_data_path("bilateralize", "reporting", "output")):
    
    if add_legacy:
        run_legacy_reporting(scenario, mp, legacy_config, legacy_out_dir)
        df_legacy = pd.read_excel(os.path.join(legacy_out_dir, scenario.model + '_' + scenario.scenario + '.xlsx'))
    if add_trade:
        trade_reporting(mp, scenario)
        df_trade = pd.read_csv(os.path.join(trade_out_dir, scenario.model + '_' + scenario.scenario + '.csv'))
    
    if add_legacy and add_trade:
        col = [str(c) for c in df_legacy.columns]
        df_legacy.columns = col
        df_trade = df_trade[col]
        df = pd.concat([df_legacy, df_trade])
    elif add_legacy:
        df = df_legacy
    elif add_trade:
        df = df_trade
    else:
        raise ValueError("No reporting to add")

    df.to_csv(os.path.join(report_out_dir, scenario.model + '_' + scenario.scenario + '.csv'))