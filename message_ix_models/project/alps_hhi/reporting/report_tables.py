"""Trade reporting workflow after bilateralization."""

import ixmp
import message_ix

from message_ix_models.report.legacy.iamc_report_hackathon_trade import report as legacy_report_trade
from message_ix_models.report.legacy.iamc_report_hackathon import report as legacy_report

from message_ix_models.tools.bilateralize.reporting.new_reporting import trade_reporting
from message_ix_models.util import package_data_path

mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])
# Specify the scenario
for model_name, scen_name in [("alps_hhi", "SSP2")]:

    scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

    # Run legacy reporting
    legacy_report_trade(mp=mp, scen=scen,
                        run_config=package_data_path('report', 'legacy', 'alps_hhi_config.yaml'),
                        out_dir = package_data_path("alps_hhi", "reporting", "legacy"))

    # Run new reporting
    #trade_reporting(scenario=scen,
    #                out_dir=package_data_path("alps_hhi", "reporting", "trade"))

mp.close_db()