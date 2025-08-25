import os
import pandas as pd
import message_ix # type: ignore
import ixmp # type: ignore
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

# Specify scenario
wacc_scenario, ssp = "cf_fair_f4", "SSP2"
model_ori = "SSP_SSP2_v6.1" # latest version "SSP_SSP2_v6.1"
scen_ori = "SSP2 - Low Emissions" # latest version "SSP2 - Low Emissions"
model_tgt = "MESSAGEix-GLOBIOM 2.0-M-R12 Investment"
scen_tgt = f"{wacc_scenario}_{ssp}_add"

# Load scenario
base = message_ix.Scenario(mp, model=model_ori, scenario=scen_ori)
log.info("Scenario loaded.")

# Check the inv_cost of power technologies
inv_cost = base.par('inv_cost')
folder_path = package_data_path("investment")
inv_cost.to_csv(os.path.join(str(folder_path), "inv_cost_ori.csv"), index=False)

unique_techs = inv_cost['technology'].unique()

# Function that generate new inv_cost # Dummy
def gene_coc_add(
    ssp: str = "SSP2",
    wacc_scenario: str = "ccf",
    baseline_year: int = 2020,
    wacc_csv_path: str = "message-ix-models/message_ix_models/project/investment/Predicted_WACC_All_SSPs_8.csv",
    wacc_excel_path: str = "message-ix-models/message_ix_models/project/investment/R12_region_WACC_all_years.xlsx",
    inv_cost_filename: str = "inv_cost_ori.csv",
    out_filename: str = "inv_cost.csv",
) -> None:
    """
    Combine WACC from CSV (solar/wind/bio/hydro) and Excel (coal/gas/nuclear),
    apply (1 + A) to inv_cost, and use each region's average WACC (all 7 techs) as default for unmatched techs.
    """

    def map_category(tech: str) -> str:
        """Classify technology based on MESSAGEix naming patterns."""
        if tech.startswith("solar_") or tech.startswith("csp_"):
            return "solar"
        elif tech.startswith("wind_"):
            return "wind"
        elif tech.startswith("bio_") or "biomass_" in tech or tech.startswith("eth_bio") or tech.startswith("meth_bio") or tech.startswith("liq_bio") or "charcoal" in tech:
            return "bio"
        elif tech.startswith("hydro_"):
            return "hydro"
        elif tech.startswith("coal_") or "furnace_coal" in tech or "c_ppl" in tech or tech.startswith("coal_i") or "lignite" in tech or "cokeoven" in tech:
            return "coal"
        elif tech.startswith("gas_") or "furnace_gas" in tech or "g_ppl" in tech or tech.startswith("gas_i") or "meth_ng" in tech or "dri_gas" in tech:
            return "gas"
        elif tech.startswith("nuc_") or "uran" in tech or "plutonium" in tech or "u5" in tech:
            return "nuclear"
        else:
            return "other"

    # === 1. Load inv_cost ===
    folder_path = package_data_path("investment")
    inv_cost = pd.read_csv(os.path.join(folder_path, inv_cost_filename))
    inv_cost = inv_cost[inv_cost["year_vtg"] >= baseline_year].copy()
    inv_cost = inv_cost.rename(columns={"technology": "technology_ori"})
    inv_cost["category"] = inv_cost["technology_ori"].apply(map_category)

    # === 2. Load CSV WACC (solar/wind/bio/hydro) ===
    wacc_csv = pd.read_csv(wacc_csv_path)
    wacc_csv = wacc_csv[(wacc_csv["Scenario"] == wacc_scenario) & (wacc_csv["SSP"] == ssp)].copy()
    wacc_csv = wacc_csv.rename(columns={
        "Region": "node_loc",
        "Year": "year_vtg",
        "Tech": "category",
        "WACC": "A"
    })

    # === 3. Load Excel WACC (coal/gas/nuclear, only 2020) ===
    wacc_xlsx_raw = pd.read_excel(wacc_excel_path)
    tech_columns = ["solar", "wind", "bio", "hydro", "coal", "gas", "nuclear"]

    # (a) Default A: region-level average over all 7 techs
    wacc_2020_all = wacc_xlsx_raw[wacc_xlsx_raw["year"] == 2020][["region"] + tech_columns].copy()
    region_default_A = wacc_2020_all.copy()
    region_default_A["A_default"] = region_default_A[tech_columns].mean(axis=1, skipna=True)
    region_default_A = region_default_A[["region", "A_default"]].rename(columns={"region": "node_loc"})

    # (b) Melt WACC to long format
    wacc_2020_long = wacc_xlsx_raw[wacc_xlsx_raw["year"] == 2020][["region"] + tech_columns]
    wacc_long = wacc_2020_long.melt(id_vars="region", var_name="category", value_name="A")
    wacc_long = wacc_long.rename(columns={"region": "node_loc"})

    # (c) Expand WACC to all years
    unique_years = inv_cost["year_vtg"].unique()
    wacc_excel_full = pd.DataFrame(
        [(r, y, c, a) for (r, c, a) in wacc_long.values for y in unique_years],
        columns=["node_loc", "year_vtg", "category", "A"]
    )

    # === 4. Combine all WACC sources ===
    wacc_csv["source"] = "csv"
    wacc_excel_full["source"] = "excel"
    wacc_all = pd.concat([wacc_csv, wacc_excel_full], ignore_index=True)

    # 去除重复项（优先保留 CSV）
    wacc_all = wacc_all.sort_values(by="source", ascending=True)
    wacc_all = wacc_all.drop_duplicates(subset=["node_loc", "year_vtg", "category"], keep="first")

    # === 5. Merge with inv_cost ===
    inv_cost = inv_cost.merge(wacc_all, on=["node_loc", "year_vtg", "category"], how="left")

    # === 6. Merge regional average A_default ===
    inv_cost = inv_cost.merge(region_default_A, on="node_loc", how="left")

    # === 7. Fill missing A with region default
    # inv_cost["A"] = inv_cost["A"].fillna(inv_cost["A_default"])
    inv_cost["A"] = inv_cost["A"].fillna(0.10)
    inv_cost.drop(columns=["A_default"], inplace=True)

    # === 8. Apply WACC to value
    inv_cost["value_original"] = inv_cost["value"]
    inv_cost["value"] = inv_cost["value_original"] * (1 + inv_cost["A"])

    # === 9. Save final result
    inv_cost["technology"] = inv_cost["technology_ori"]
    inv_cost = inv_cost.drop(columns=["technology_ori", "category"], errors="ignore")

    cols = ['node_loc', 'technology', 'year_vtg', 'value', 'value_original', 'A', 'unit']
    inv_cost[cols].to_csv(os.path.join(folder_path, out_filename), index=False)
    
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
gene_coc_add(ssp=ssp, wacc_scenario=wacc_scenario)

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
solver = "MESSAGE"  # after having some solved runs, try using solver = "MESSAGE-MACRO"

# Solve scenario
scen.solve(solver)

# Close the connection to the database
log.info("Closing connection to the database.")
mp.close_db()
