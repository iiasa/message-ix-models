"""Reporting workflow for WEU security scenarios"""

import ixmp
import message_ix
import message_data
import pandas as pd

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.full_reporting import report as report_combine

# Call all scenarios for analysis
mp = ixmp.Platform()
for scen in ["SSP_SSP2_v6.4", "SSP2"]: #"FSU2040", "FSU2100",
             #"SSP2_NAM10EJ", "FSU2040_NAM10EJ", "FSU2100_NAM10EJ",
             #"SSP2_NAM15EJ", "FSU2040_NAM15EJ", "FSU2100_NAM15EJ",
             #"SSP2_NAM20EJ", "FSU2040_NAM20EJ", "FSU2100_NAM20EJ",
             #"SSP2_NAM25EJ", "FSU2040_NAM25EJ", "FSU2100_NAM25EJ",
             #"SSP2_NAM30EJ", "FSU2040_NAM30EJ", "FSU2100_NAM30EJ",
             #"SSP2_MEACON_1.0", "FSU2040_MEACON_1.0", "FSU2100_MEACON_1.0",]:
             #"SSP2_MEACON_0.9", "FSU2040_MEACON_0.9", "FSU2100_MEACON_0.9",
             #"SSP2_MEACON_0.8", "FSU2040_MEACON_0.8", "FSU2100_MEACON_0.8",
             #"SSP2_MEACON_0.75", "FSU2040_MEACON_0.75", "FSU2100_MEACON_0.75",
             #"SSP2_MEACON_0.5", "FSU2040_MEACON_0.5", "FSU2100_MEACON_0.5",
             #"SSP2_MEACON_0.25", "FSU2040_MEACON_0.25", "FSU2100_MEACON_0.25"]:
    report_combine(scenario = message_ix.Scenario(mp, model = "weu_security", scenario = scen),
                   legacy_config = package_data_path('weu_security', 'reporting', 'legacy', 'legacy_config.yaml'),
                   legacy_out_dir = package_data_path('weu_security', 'reporting', 'output', "legacy"),
                   trade_out_dir = package_data_path('weu_security', 'reporting', 'output', 'trade'),
                   report_out_dir = package_data_path('weu_security', 'reporting', 'output'))