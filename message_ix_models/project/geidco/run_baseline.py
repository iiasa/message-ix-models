import os
import sys
import pandas as pd
import message_ix
import ixmp
import logging

mp = ixmp.Platform()
from pathlib import Path
from message_ix_models.util import package_data_path

from message_ix_models.tools.inter_pipe.generate_inter_pipe import (
    inter_pipe_bare,
    inter_pipe_build,
)

# Generate bare sheets for pipe technologies and pipe supply technologies
inter_pipe_bare()

# Building inter pipes on the scenario (scenario info set in Config)
scen = inter_pipe_build(config_name=None)

# Add additional constraints
bound_total_capacity_up = pd.read_csv(
    Path(package_data_path("inter_pipe")) / "bound_total_capacity_up.csv"
)
with scen.transact("Additional constraints added"):
    scen.add_par("bound_total_capacity_up", bound_total_capacity_up)

# Additional steps for the specific baseline
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

# Close the connection to the database
mp.close_db()
