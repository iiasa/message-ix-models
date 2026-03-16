"""Reporting workflow for WEU security scenarios"""

import ixmp
import message_ix
import message_data
import pandas as pd

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.full_reporting import report as full_report

# Call all scenarios for analysis
mp = ixmp.Platform()
for scen in ["SSP2", "FSU2040", "FSU2100",
             "SSP2_NAMboost", "FSU2040_NAMboost", "FSU2100_NAMboost",
             "SSP2_MEACON", "FSU2040_MEACON", "FSU2100_MEACON"]:
    full_report(scenario = message_ix.Scenario(mp, model = "weu_security", scenario = scen))