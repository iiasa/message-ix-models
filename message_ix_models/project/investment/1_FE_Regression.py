"""Fixed Effects Regression Analysis for Investment Cost Modeling.

Authors: Shuting Fan, Yiyi Ju

This module performs panel regression analysis on historical data (2014 to 2022) to
understand the relationship between (cumulative) international climate finance (CF)
and weighted average cost of capital (WACC) of multi power generation technologies.
The analysis uses fixed effects regression to control for unobserved region-specific
factors and generates coefficients that are used for futureWACC projections.
"""

import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
from message_ix import Scenario

from message_ix_models.util import package_data_path, private_data_path

log = logging.getLogger(__name__)


def main(context, scenario: Scenario) -> Scenario:
    """Run fixed effects regression analysis on historical data.

    Parameters
    ----------
    context
        Workflow context
    scenario : Scenario
        MESSAGE scenario (not used in this function)

    Returns
    -------
    Scenario
        The input scenario (unchanged)
    """
    # === Config ===
    INPUT_XLSX = "Reg_data.xlsx"
    SHEET = "Sheet1"
    OUT_COEF_CSV = "Reg_coeff.csv"
    SHOW_DASH_FOR_NA = True  # True: Replace NaN with "-" when exporting

    log.info("Starting Fixed Effects Regression Analysis")

    # === Step 1: Load Excel data ===
    df = pd.read_excel(private_data_path("investment", INPUT_XLSX), sheet_name=SHEET)

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
            "indep": [
                "Total_Fin",
                "Rating_based_Default_Spread",
                "R_D_expenditure",
                "lnShareRE",
            ],
        },
        "Wind": {
            "dep": "ln_wind",
            "indep": [
                "Total_Fin",
                "Rating_based_Default_Spread",
                "R_D_expenditure",
                "lnShareRE",
            ],
        },
        "Bio": {
            "dep": "ln_bio",
            "indep": [
                "Total_Fin",
                "Rating_based_Default_Spread",
                "R_D_expenditure",
                "FDI",
                "lnShareRE",
            ],
        },
        "Hydro": {
            "dep": "ln_hydro",
            "indep": [
                "Total_Fin",
                "Rating_based_Default_Spread",
                "R_D_expenditure",
                "FDI",
                "lnShareRE",
            ],
        },
    }

    # === Step 5: Run fixed effects regression (PanelOLS) ===
    results = {}
    coef_table = pd.DataFrame()
    all_coefs = {}

    for tech, info in tech_models.items():
        y = df[info["dep"]]
        X = df[info["indep"]].copy()

        # Ensure that all independent variables exist
        for col in info["indep"]:
            if col not in df.columns:
                df[col] = np.nan

        data = pd.concat([df[[info["dep"]]], df[info["indep"]]], axis=1).dropna(
            how="any"
        )
        if data.empty:
            log.warning(f"{tech}: Data is empty, skipping.")
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

    # Save to the package data directory
    output_dir = package_data_path("investment")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUT_COEF_CSV
    coef_to_save.to_csv(output_path, float_format="%.6f")

    log.info(f"Regression coefficients will be saved to: {output_dir}")
    log.info(
        f"Fixed effects regression completed. Coefficients saved to: {output_path}"
    )

    return scenario
