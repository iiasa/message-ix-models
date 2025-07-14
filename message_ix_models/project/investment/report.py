import ixmp 
import message_ix
mp = ixmp.Platform("ixmp_dev")
from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore
# requires the message_data branch ssp-dev
run_config = "materials_daccs_run_config.yaml"

def _report(scen):
    iamc_report_hackathon.report(
        mp=mp,
        scen=scen,
        merge_hist=True,
        run_config=run_config,
    )

# select scenario
model_orig = "MESSAGEix-GLOBIOM 2.0-M-R12 Investment" 
scen_orig = "baseline_ssp6.1_low_base"  # replace with your scenario name

# load scenario
print("Loading scenario...")
s = message_ix.Scenario(mp, model_orig, scen_orig)

_report(s)
