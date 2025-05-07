import message_ix
import ixmp
from message_ix_models.model.material.report.run_reporting import (
    run as report_materials,
)
from message_data.tools.post_processing import iamc_report_hackathon


# Specify the scenario
model_name = "MESSAGEix-GLOBIOM_GEI5"
scen_name = "baseline_uhv"
mp = ixmp.Platform()
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

# Report the scenario
# scen.check_out(timeseries_only=True)
# df = report_materials(scen, region="R12_GLB", upload_ts=True)
# scen.commit("Add materials reporting") # Require message_ix_models branch ssp-dev

iamc_report_hackathon.report(
    mp=mp, scen=scen, merge_hist=False, run_config="materials_daccs_run_config.yaml"
)  # Require message_data branch ssp-dev

# Close the connection to the database
mp.close_db()
