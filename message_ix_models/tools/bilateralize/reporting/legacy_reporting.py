import message_ix
import ixmp
from message_ix_models.report.legacy.iamc_report_hackathon_trade import report as legacy_report
from message_ix_models.util import package_data_path

# Specify the scenario
model_name = "china_security"
scen_name = "SSP2_Baseline"
mp = ixmp.Platform()
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

legacy_report(
    mp=mp, 
    scen=scen, 
) 

# Close the connection to the database
mp.close_db()