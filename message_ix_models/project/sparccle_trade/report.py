"""Reporting workflow for SPARCCLE STS"""

from pathlib import Path

import ixmp
import message_ix
import message_data
import pandas as pd

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.tools.bilateralize.reporting.full_reporting import report as report_combine
from message_ix_models.util import package_data_path

def combined_legacy_trade_reporting():
    LEGACY_CONFIG  = package_data_path("sparccle_trade", "configs", "legacy_config.yaml")
    LEGACY_OUT_DIR = package_data_path("sparccle_trade", "reporting", "legacy")
    TRADE_OUT_DIR  = package_data_path("sparccle_trade", "reporting", "trade")
    REPORT_OUT_DIR = package_data_path("sparccle_trade", "reporting")

    # Call all scenarios for analysis
    mp = ixmp.Platform()
    for scen in [#'SSP3_NPiREF', 'SSP3_STS3',
                 'SSP3_NPiREF_frictions', 'SSP3_STS3_frictions']:
        report_combine(scenario    = message_ix.Scenario(mp, model="sparccle_trade", scenario=scen),
                    legacy_config  = LEGACY_CONFIG,
                    legacy_out_dir = LEGACY_OUT_DIR,
                    trade_out_dir  = TRADE_OUT_DIR,
                    report_out_dir = REPORT_OUT_DIR,
                    add_legacy = False)

combined_legacy_trade_reporting()