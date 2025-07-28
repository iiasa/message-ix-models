import os
import sys
import pandas as pd
import message_ix # type: ignore
import ixmp # type: ignore
import logging

mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])
from pathlib import Path
from message_ix_models.util import package_data_path

from message_ix_models.tools.inter_pipe.generate_inter_pipe import (
    inter_pipe_bare,
    inter_pipe_build,
)

from message_ix_models.model.material.report.run_reporting import ( # type: ignore
    run as report_materials,
)
from message_data.tools.post_processing import iamc_report_hackathon as legacy_report # type: ignore

# Generate bare sheets for pipe technologies and pipe supply technologies
# inter_pipe_bare()

# Building inter pipes on the scenario (scenario info set in Config)
scen = inter_pipe_build(config_name="config.yaml") # backup of /data/inter_pipe stored at IIASA sharepoint

# Add additional constraints on total capacity
bound_total_capacity_up = pd.read_csv(
    Path(package_data_path("inter_pipe")) / "bound_total_capacity_up.csv"
) # constraints from last phase GEI scenarios (maybe can be removed)
with scen.transact("Additional constraints added"):
    scen.add_par("bound_total_capacity_up", bound_total_capacity_up)

# Add additional constraints on the share of hydro, solar, and wind in pipe supply technologies
# Modeling the share of pipe supply techs joining DCUHV 
# Data selected from 88 GEIDCO planned UHV transmission projects (not open-source data)
# Adding addtional file here
# Path(package_data_path("inter_pipe")) / "relation_activity_share_pipe_supply.csv"
# It is collected by function inter_pipe_build().

# Add potential constraints of solar and wind (relation removed in the recent baselines)
bound_activity_up = pd.read_csv(
    Path(package_data_path("inter_pipe")) / "bound_activity_up.csv"
)  # the potential of solar and wind
bound_activity_up["year_act"] = bound_activity_up["year_act"].astype(int)
bound_activity_up["value"] = (
    bound_activity_up["value"] * 0.01
)  # 1% of them can be used for DCUHV
# TODO: add pot relation, pipe supply tech potential currently setting as 1% of the total potential,
with scen.transact("Additional constraints added"):
    scen.add_par("bound_activity_up", bound_activity_up)

# Specify cplex solver options
message_ix.models.DEFAULT_CPLEX_OPTIONS = {
    "advind": 0,
    "lpmethod": 4,
    "threads": 4,
    "epopt": 1e-6,
    "scaind": -1,
    # "predual": 1,
    "barcrossalg": 0,
}

# Specify solver
solver = "MESSAGE"  # solver = "MESSAGE-MACRO"

# Solve scenario
scen.solve(solver)

# Report the scenario
# scen.check_out(timeseries_only=True)
# df = report_materials(scen, region="R12_GLB", upload_ts=True)
# scen.commit("Add materials reporting") 
# # Require message_ix_models branch ssp-dev or any branch cloned from it. 
# # Frequent rebase may be required to keep up with the latest changes in the upstream repo. 

legacy_report.report(
    mp=mp, 
    scen=scen, 
    merge_hist=True, 
    run_config="materials_daccs_run_config_gei.yaml"
)  # Require message_data branch project/geidco

# Close the connection to the database
mp.close_db()
