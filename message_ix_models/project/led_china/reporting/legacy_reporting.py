import message_ix
import ixmp
from message_ix_models.report.legacy.iamc_report_hackathon import report as legacy_report
from message_ix_models.util import package_data_path

# Specify the scenario
model_name = "china_security"
scen_name = "SSP2_Baseline"
mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

legacy_report(
    mp=mp, 
    scen=scen, 
    run_config="default_run_config.yaml",
    out_dir = package_data_path("led_china", "reporting", "legacy")
) 

# Close the connection to the database
mp.close_db()