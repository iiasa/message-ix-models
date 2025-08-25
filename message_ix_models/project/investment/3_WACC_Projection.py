import pandas as pd
import numpy as np
from pathlib import Path
from message_ix_models.util import package_data_path

# === Step 0: Define parameters ===
INPUT_DIR = package_data_path("investment", "SSP_scenario")
REG_COEFF_PATH = package_data_path("investment", "Reg_coeff.csv")
OUTPUT_PATH = package_data_path("investment", "Predicted_WACC.csv")
SCENARIOS = {'ccf', 'cf_fair_f10',  'cf_his_f10'}
SSPS = [f"SSP{j}" for j in range(1, 6)]
TECHS = ["solar", "wind", "bio", "hydro"]

# === Step 1: Merge scenario and SSP files ===
def merge_scen_ssp_files(input_dir: str) -> pd.DataFrame:
    base_path = Path(input_dir)
    df_list = []

    for scen in SCENARIOS:
        for ssp in SSPS:
            file_name = f"{scen}_{ssp}_Indicators_2020_2100.xlsx"
            file_path = base_path / file_name
            if not file_path.exists():
                print(f"File not found, skipping: {file_path}")
                continue
            df_tmp = pd.read_excel(file_path)
            df_tmp['Scenario'] = scen
            df_tmp['SSP'] = ssp
            df_list.append(df_tmp)

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

# === Step 2: Load and prepare data ===
df = merge_scen_ssp_files(INPUT_DIR)
df["lnPGDP"] = np.log(df["PGDP"])
df["lnShareRE"] = np.log(df["ShareRE"])

# === Step 3: Extract 2020 baseline ===
for tech in TECHS:
    df[tech] = df[tech].replace(0, np.nan)
    df[f"ln_{tech}"] = np.log(df[tech])
    df.loc[df["Year"] == 2020, "ln_" + tech] = np.log(df.loc[df["Year"] == 2020, tech])

base = (
    df[df["Year"] == 2020]
    .drop_duplicates(subset=["Region", "SSP"])
    .loc[:, ["Region", "SSP", "Total_Fin", "lnPGDP", "lnShareRE", "Rating_based_Default_Spread", "R_D_expenditure", "FDI"] + [f"ln_{t}" for t in TECHS]]
    .rename(columns=lambda c: c + "_base" if c not in ["Region", "SSP"] else c)
)

# === Step 4: Merge baseline back to main table ===
df = df.merge(base, on=["Region", "SSP"], how="left")

# === Step 5: Calculate deltas ===
df["d_Total_Fin"] = df["Total_Fin"] - df["Total_Fin_base"]
df["d_lnPGDP"] = df["lnPGDP"] - df["lnPGDP_base"]
df["d_lnShareRE"] = df["lnShareRE"] - df["lnShareRE_base"]
df["d_Rating_based_Default_Spread"] = df["Rating_based_Default_Spread"] - df["Rating_based_Default_Spread_base"]
df["d_R_D_expenditure"] = df["R_D_expenditure"] - df["R_D_expenditure_base"]
df["d_FDI"] = df["FDI"] - df["FDI_base"]

# === Step 6: Load regression coefficients ===
coeff_df = pd.read_csv(REG_COEFF_PATH, index_col=0)

# Clean index and column names: strip whitespaces
coeff_df.index = coeff_df.index.astype(str).str.strip()
coeff_df.columns = coeff_df.columns.astype(str).str.strip()

# Treat invalid values (e.g., '鈥', '–', '-') as missing (NaN)
bad_values = {"鈥", "–", "-", "", "nan", "NaN", "None"}
coeff_df.replace(bad_values, np.nan, inplace=True)

# Force all entries to numeric; non-convertible values become NaN
for col in coeff_df.columns:
    coeff_df[col] = pd.to_numeric(coeff_df[col], errors='coerce')

# Remove unnecessary rows if present
if 'Observations' in coeff_df.index:
    coeff_df = coeff_df.drop(index='Observations')

# Build coefficient dictionary for each technology (drop NaNs)
coeffs = {
    tech.lower(): coeff_df[tech].dropna().to_dict()
    for tech in coeff_df.columns
}

# === Step 7: Predict ln(WACC) and convert to WACC ===
for tech, coef_dict in coeffs.items():
    base_ln_col = f"ln_{tech}_base"
    ln_pred_col = f"ln_pred_{tech}"
    pred_col = f"pred_{tech}"

    df[ln_pred_col] = df[base_ln_col] + coef_dict.get("Constant", 0)
    for var, beta in coef_dict.items():
        if var == "Constant":
            continue
        df[ln_pred_col] += beta * df.get("d_" + var, 0)

    df[pred_col] = np.exp(df[ln_pred_col])

# === Step 8: Fill predictions back into main tech columns ===
for tech in TECHS:
    pred_col = f"pred_{tech}"
    df.loc[df["Year"] != 2020, tech] = df.loc[df["Year"] != 2020, pred_col]

# === Step 9: Reshape final result ===
final_columns = ["Region", "Year", "Scenario", "SSP"] + TECHS
df_result = df[final_columns].copy()

df_result = df_result.melt(
    id_vars=['Region', 'Year', 'Scenario', 'SSP'],
    value_vars=TECHS,
    var_name='Tech',
    value_name='WACC'
)

# === Step 9.1: Add 'cwacc' scenario (fixed at 2020 values) ===
years = sorted(df["Year"].unique())
base_wacc = df.loc[df["Year"] == 2020, ["Region", "SSP"] + TECHS].copy()
cwacc_long = base_wacc.melt(id_vars=["Region", "SSP"], value_vars=TECHS, var_name="Tech", value_name="WACC")

years_df = pd.DataFrame({"Year": years})
cwacc_long["key"] = 1
years_df["key"] = 1
cwacc_expanded = cwacc_long.merge(years_df, on="key").drop(columns="key")

cwacc_expanded["Scenario"] = "cwacc"
cwacc_expanded = cwacc_expanded[["Region", "Year", "Scenario", "SSP", "Tech", "WACC"]]

df_result_all = pd.concat([df_result, cwacc_expanded], ignore_index=True)

# Filter regions with NaN
regions_with_nan = df_result_all[df_result_all["WACC"].isna()]["Region"].unique()
df_result_all = df_result_all[~df_result_all["Region"].isin(regions_with_nan)]

# === Step 10: Export result ===
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
df_result_all.to_csv(OUTPUT_PATH, index=False)
