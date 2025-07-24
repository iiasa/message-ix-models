import message_ix
import ixmp
from message_ix_models.model.material.report.run_reporting import (
    run as report_materials,
)
from message_data.tools.post_processing import iamc_report_hackathon


# Specify the scenario
model_name = "MixG_GEIDCO5_SSP2_v6.1"
scen_name = "Base_RCP7_int_noIBWT"
mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

# # Report the scenario
# scen.check_out(timeseries_only=True)
# df = report_materials(scen, region="R12_GLB", upload_ts=True)
# scen.commit("Add materials reporting") 
# # Require message_ix_models branch ssp-dev (which is where this branch is cloned from).
# # Frequent rebase may be required to keep up with the latest changes in the upstream branch. 

iamc_report_hackathon.report(
    mp=mp, 
    scen=scen, 
    merge_hist=True, 
    run_config="materials_daccs_run_config_gei.yaml"
)  # Require message_data branch project/geidco

# Close the connection to the database
mp.close_db()
