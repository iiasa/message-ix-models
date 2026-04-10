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
for scen in [#"SSP2", "FSU2040", "FSU2100",
             #"SSP2_NAM30EJ", "FSU2040_NAM30EJ", "FSU2100_NAM30EJ",
             #"SSP2_MEACON_1.0", "FSU2040_MEACON_1.0", "FSU2100_MEACON_1.0",
             "INDC2030", "INDC2030_FSU2040", "INDC2030_FSU2100",
             "INDC2030_NAM30EJ", "INDC2030_FSU2100_NAM30EJ",
             "INDC2030_MEACON_1.0", "INDC2030_FSU2100_MEACON_1.0"]:
    report_combine(scenario = message_ix.Scenario(mp, model = "weu_security", scenario = scen),
                   legacy_config = package_data_path('weu_security', 'reporting', 'legacy', 'legacy_config.yaml'),
                   legacy_out_dir = package_data_path('weu_security', 'reporting', 'output', "legacy"),
                   trade_out_dir = package_data_path('weu_security', 'reporting', 'output', 'trade'),
                   report_out_dir = package_data_path('weu_security', 'reporting', 'output'))