"""Reporting workflow for Chinese energy security scenarios"""

import ixmp
import message_ix
import message_data
import pandas as pd

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.full_reporting import report as report_combine

# Call all scenarios for analysis
model_name = "weu_security"
project_name = "china_security"
mp = ixmp.Platform()
for scen in ["SSP2"]:
    #"SSP2_Baseline", "SSP2_2C", "SSP2_Commitments", "LED_Baseline", "LED_2C", "LED_Commitments"]:
    report_combine(scenario = message_ix.Scenario(mp, model = model_name, scenario = scen),
                   legacy_config = package_data_path(project_name, 'reporting', 'legacy', 'legacy_config.yaml'),
                   legacy_out_dir = package_data_path(project_name, 'reporting', 'output', "legacy"),
                   trade_out_dir = package_data_path(project_name, 'reporting', 'output', 'trade'),
                   report_out_dir = package_data_path(project_name, 'reporting', 'output'))