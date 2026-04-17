"""Reporting workflow for Chinese energy security scenarios"""

import ixmp
import message_ix
import message_data
import pandas as pd

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.full_reporting import report as report_combine

# Call all scenarios for analysis
mp = ixmp.Platform()
for scen in ["LED_Baseline"]: #"SSP2_Baseline", "SSP2_2C", "LED_Baseline", "LED_2C"]:
    #"SSP2_Baseline", "SSP2_2C", "SSP2_Commitments", "LED_Baseline", "LED_2C", "LED_Commitments"]:
    report_combine(scenario = message_ix.Scenario(mp, model = "china_security", scenario = scen),
                   legacy_config = package_data_path("china_security", 'reporting', 'legacy', 'legacy_config.yaml'),
                   legacy_out_dir = package_data_path("china_security", 'reporting', 'output', "legacy"),
                   trade_out_dir = package_data_path("china_security", 'reporting', 'output', 'trade'),
                   report_out_dir = package_data_path("china_security", 'reporting', 'output'))