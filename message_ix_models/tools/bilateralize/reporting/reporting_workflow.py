"""Trade reporting workflow after bilateralization."""

import ixmp
import message_ix

from message_ix_models.report.legacy.iamc_report_hackathon_trade import report as legacy_report
from message_ix_models.tools.bilateralize.reporting.new_reporting import trade_reporting

# Specify the scenario
model_name = "china_security"
scen_name = "SSP2_Baseline"
mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

# Run legacy reporting
legacy_report(mp=mp, scen=scen)

# Run new reporting
trade_reporting(scenario=scen)

mp.close_db()