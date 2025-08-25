import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
import statsmodels.api as sm
from pathlib import Path
from message_ix_models.util import package_data_path

# === Config ===
INPUT_XLSX = "Reg_data.xlsx"
SHEET = "Sheet1"
OUT_COEF_CSV = "Reg_coeff.csv"
SHOW_DASH_FOR_NA = True  # True: Replace NaN with "-" when exporting

# === Step 1: Load Excel data ===
df = pd.read_excel(package_data_path("investment", INPUT_XLSX), sheet_name=SHEET)

# === Step 2: Define safe log to handle zero/NaN ===
def safe_log(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = s.where(s > 0, np.nan)
    return np.log(s)

# Apply log transformations
df["lnPGDP"] = safe_log(df.get("PGDP"))
df["lnShareRE"] = safe_log(df.get("ShareRE"))
df["lnSpread"] = safe_log(df.get("Rating_based_Default_Spread"))

for col in ["GDP", "Total_Fin", "Cum_Total_Fin", "ShareRE_CAP", "HDI"]:
    if col in df.columns:
        df[f"ln{col}"] = safe_log(df[col])

for y in ["solar", "wind", "bio", "hydro", "coal", "gas", "nuclear", "WACC_RE"]:
    if y in df.columns:
        df[f"ln_{y}"] = safe_log(df[y])

# === Step 3: Set panel data index (Region × Year) ===
df = df.set_index(["Region_code", "Year"]).sort_index()

# === Step 4: Define regression models by technology ===
tech_models = {
    "Solar": {
        "dep": "ln_solar",
        "indep": ["Total_Fin", "Rating_based_Default_Spread", "R_D_expenditure", "lnShareRE"],
    },
    "Wind": {
        "dep": "ln_wind",
        "indep": ["Total_Fin", "Rating_based_Default_Spread", "R_D_expenditure", "lnShareRE"],
    },
    "Bio": {
        "dep": "ln_bio",
        "indep": ["Total_Fin", "Rating_based_Default_Spread", "R_D_expenditure", "FDI", "lnShareRE"],
    },
    "Hydro": {
        "dep": "ln_hydro",
        "indep": ["Total_Fin", "Rating_based_Default_Spread", "R_D_expenditure", "FDI", "lnShareRE"],
    },
}

# === Step 5: Run fixed effects regression (PanelOLS) ===
results = {}
coef_table = pd.DataFrame()
all_rows = set()
all_coefs = {}

for tech, info in tech_models.items():
    y = df[info["dep"]]
    X = df[info["indep"]].copy()

    # Ensure that all independent variables exist
    for col in info["indep"]:
        if col not in df.columns:
            df[col] = np.nan

    data = pd.concat([df[[info["dep"]]], df[info["indep"]]], axis=1).dropna(how="any")
    if data.empty:
        print(f"[WARN] {tech}: 有效样本为空，跳过。")
        continue

    y = data[info["dep"]]
    X = sm.add_constant(data[info["indep"]])

    model = PanelOLS(y, X, entity_effects=True, drop_absorbed=True)
    res = model.fit()
    results[tech] = res

    # —— Key point: Unify the names of cleaning parameters —— #
    coefs = res.params.copy()
    coefs.index = coefs.index.astype(str).str.strip()            
    coefs.index = coefs.index.where(coefs.index != "const", "Constant") 
    coefs

    all_coefs[tech] = coefs

coef_table = pd.DataFrame(all_coefs)
coef_table

coef_to_save = coef_table.copy()
if SHOW_DASH_FOR_NA:
    coef_to_save = coef_to_save.where(~coef_to_save.isna(), "–")

coef_to_save.to_csv(package_data_path("investment", OUT_COEF_CSV), float_format="%.6f")