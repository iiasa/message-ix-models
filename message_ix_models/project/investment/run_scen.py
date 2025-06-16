import os
import pandas as pd
import message_ix
import ixmp
import logging
import sys

mp = ixmp.Platform()
from pathlib import Path
from message_ix_models.util import package_data_path

def get_logger(name: str):
    # Set the logging level to INFO (will show INFO and above messages)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Define the format of log messages:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

    # Apply the format to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    log.addHandler(handler)

    return log

log = get_logger(__name__)

par_list = [
    "inv_cost",
    # "bound_new_capacity_lo",
]

# All power generation technologies
# check here
# https://github.com/iiasa/message-ix-models/blob/main/message_ix_models/data/technology.yaml
tech_list = [
    "coal_ppl", "ucoal_ppl", "coal_adv", "coal_adv_ccs",
    "igcc", "igcc_ccs",
    "foil_ppl", "loil_ppl", "loil_cc",
    "gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs",
    "bio_ppl", "bio_istig", "bio_istig_ccs",
    "geo_ppl",
    "solar_res1", "solar_res2", "solar_res3", "solar_res4",
    "solar_res5", "solar_res6", "solar_res7", "solar_res8",
    "csp_sm1_res1", "csp_sm1_res2", "csp_sm1_res3", "csp_sm1_res4",
    "csp_sm1_res5", "csp_sm1_res6", "csp_sm1_res7",
    "wind_res1", "wind_res2", "wind_res3", "wind_res4",
    "wind_ref1", "wind_ref2", "wind_ref3", "wind_ref4", "wind_ref5",
    "nuc_lc", "nuc_hc", "nuc_fbr"
]

# Specify scenario
model_ori = "SSP_SSP2_v2.1"
scen_ori = "baseline_1000f"
model_tgt = "MESSAGEix-GLOBIOM 2.0-M-R12 Investment"
scen_tgt = "baseline_1000f_inv_base"

# Load scenario
base = message_ix.Scenario(mp, model=model_ori, scenario=scen_ori)
log.info("Scenario loaded.")

# Check the inv_cost of power technologies
inv_cost = base.par('inv_cost', filters={'technology': tech_list})
folder_path = package_data_path("investment")
inv_cost.to_csv(os.path.join(str(folder_path), "inv_cost_ori.csv"), index=False)

# Function that generate new inv_cost # Dummy
def imple_coc():
    try:
        inv_cost_ori = pd.read_csv(package_data_path("investment", "inv_cost_ori.csv"))
    except:
        log.info("Original inv_cost not found.")
        pass
    
    # Approach 1: a fixed share (e.g., A%) in the baseline year as financing cost, 
    # A%*inv_cost_0 following the long-term projection of financing cost, 
    # (1-A%)*inv_cost_0 following the tech progress rate (Meas tool)

    # Dummy A% as 10%
    A = 0.1

    inv_cost = inv_cost_ori[inv_cost_ori["year_vtg"] >= 2020].copy()
    mask_2020 = inv_cost["year_vtg"] == 2020
    inv_cost["coc_base"] = 0.0
    inv_cost["non_coc_base"] = 0.0
    inv_cost.loc[mask_2020, "coc_base"] = inv_cost.loc[mask_2020, "value"] * A
    coc_base_2020 = inv_cost.loc[mask_2020].set_index(["node_loc", "technology"])["coc_base"]
    inv_cost.loc[~mask_2020, "coc_base"] = inv_cost.loc[~mask_2020].set_index(["node_loc", "technology"]).index.map(coc_base_2020)
    
    inv_cost["non_coc_base"] = inv_cost["value"] - inv_cost["coc_base"]
    non_coc_base_2020 = (
        inv_cost[mask_2020]
        .set_index(["node_loc", "technology"])["non_coc_base"]
    )
    inv_cost["non_coc_base_2020"] = inv_cost.set_index(["node_loc", "technology"]).index.map(non_coc_base_2020)
    inv_cost["non_coc_base_growth"] = (
        inv_cost["non_coc_base"] / inv_cost["non_coc_base_2020"]
    )

    # Then try different approaches with coc and non_coc, and refill value

    # Output
    folder_path = package_data_path("investment")
    inv_cost.to_csv(os.path.join(str(folder_path), "inv_cost.csv"), index=False)

# Function that implements new CoC (read inv_cost)
def imple_coc(scen):
    scen.check_out()

    # Create data file list
    folder_path = package_data_path("investment")
    data_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    # Load data files
    dic_data = {}
    for file in data_files:
        file_path = os.path.join(folder_path, file)
        key_name = file.replace(".csv", "")  # Remove .csv extension
        df = pd.read_csv(file_path)
        dic_data[key_name] = df

    # Add par
    for i in par_list:
        # Find all keys in dic_data that exactly matching the parameter name
        matching_keys = [k for k in dic_data.keys() if k == i]
        if matching_keys:
            # Combine all matching DataFrames
            combined_df = pd.concat(
                [dic_data[k] for k in matching_keys], ignore_index=True
            )
            scen.add_par(i, combined_df)
            log.info(f"Parameter {i} from {matching_keys} added.")
        else:
            log.info("No new parameters found.")
            pass
    scen.commit("New CoC implemented.")

# Clone scenario
scen = base.clone(model_tgt, scen_tgt, keep_solution=False)
scen.set_as_default()
log.info("Scenario cloned.")

# Generate new parameters
# imple_coc()

# Apply scenario settings
imple_coc(scen)
log.info("Scenario settings added.")

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
log.info("Closing connection to the database.")
mp.close_db()
