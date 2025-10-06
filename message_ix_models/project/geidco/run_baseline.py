"""GEIDCO baseline scenario runner.

This script builds a baseline scenario with inter-pipe technologies under the GEIDCO
scenario assumptions.
It performs the following main steps:

1. Generate bare sheets for inter-pipe technologies
2. Clone the base scenario to create a new target scenario
3. Build inter-pipe technologies and constraints
4. Add additional constraints on total capacity and activity bounds if any
5. Solve the scenario and generate reports

One can replace other starting scenarios by changing the start_model and start_scen
variables.

Usage:
    python run_baseline.py

Configuration:
    The script uses config.yaml and various CSV files in the inter_pipe data directory
    to define technology parameters and constraints.
"""

from pathlib import Path

import ixmp  # type: ignore
import message_ix  # type: ignore
import pandas as pd
from message_data.tools.post_processing import (
    iamc_report_hackathon as legacy_report,  # type: ignore
)

from message_ix_models.tools.inter_pipe import build, generate_bare_sheets
from message_ix_models.util import package_data_path

mp = ixmp.Platform("ixmp_dev", jvmargs=["-Xmx32G"])

# Clarify the scenario information
start_model = "SSP_SSP2_v6.1"
start_scen = "baseline_DEFAULT"
target_model = "MixG_GEIDCO5_SSP2_v6.1"
target_scen = "Base_RCP7_int_noIBWT"

base_scen = message_ix.Scenario(mp, start_model, start_scen)

# Generate bare sheets for pipe technologies and pipe supply technologies
generate_bare_sheets(
    base_scen,
    config_name="config.yaml",
)

# Clone the scenario
tar_scen = base_scen.clone(target_model, target_scen, keep_solution=False)
tar_scen.set_as_default()

# Building inter pipes on the scenario (scenario info set in Config)
scen = build(
    tar_scen, config_name="config.yaml"
)  # backup of /data/inter_pipe stored at IIASA sharepoint

# Add additional constraints on total capacity
bound_total_capacity_up = pd.read_csv(
    Path(package_data_path("inter_pipe")) / "bound_total_capacity_up.csv"
)  # constraints from last phase GEI scenarios (maybe can be removed)
with scen.transact("Additional constraints added"):
    scen.add_par("bound_total_capacity_up", bound_total_capacity_up)

# Add additional constraints on the share of hydro, solar,
# and wind in pipe supply technologies
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
# TODO: add pot relation, pipe supply tech potential currently setting
# as 1% of the total potential,
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
# Require message_ix_models branch ssp-dev
# or any branch cloned from it.
# Frequent rebase may be required to keep up with the latest
# changes in the upstream repo.

legacy_report.report(
    mp=mp, scen=scen, merge_hist=True, run_config="materials_daccs_run_config_gei.yaml"
)  # Require message_data branch project/geidco

# Close the connection to the database
mp.close_db()
