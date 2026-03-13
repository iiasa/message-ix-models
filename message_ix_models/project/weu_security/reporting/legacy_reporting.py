"""Trade reporting workflow after bilateralization."""

import ixmp
import message_ix
import message_data

from message_data.tools.post_processing.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path

mp = ixmp.Platform()

# Specify the scenario
for model_name, scen_name in [("weu_security", "SSP2")]:

    scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

    # Run legacy reporting
    legacy_report(mp=mp, scen=scen,
                  run_config=package_data_path('weu_security', 'reporting','legacy', 'legacy_config.yaml'),
                  out_dir = package_data_path("weu_security", "reporting", "legacy"))

mp.close_db()