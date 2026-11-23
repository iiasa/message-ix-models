"""WACC Projection for Investment Cost Modeling.

Authors: Shuting Fan, Yiyi Ju

This module projects Weighted Average Cost of Capital (WACC) for renewable energy
technologies (at least by 2050) using regression coefficients from fixed effects
analysis and scenario data from SSP projections. Power generation technologies
other than renewables are then treated as with no response to international climate
finance.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path, private_data_path

log = logging.getLogger(__name__)


def main():  # noqa: C901
    """Project WACC for renewable energy technologies.

    Returns
    -------
    None
    """
    # === Config ===
    INPUT_DIR = private_data_path("investment", "SSP_scenario")
    REG_COEFF_PATH = "Reg_coeff.csv"  # Will be in project/investment folder
    SCENARIOS = {"locf", "hicf_fair", "hicf_his", "rebase"}
    SSPS = [f"SSP{j}" for j in range(1, 6)]
    TECHS = ["solar", "wind", "bio", "hydro"]

    # === Step 1: Merge scenario and SSP files ===
    def merge_scen_ssp_files(input_dir: str) -> pd.DataFrame:
        base_path = Path(input_dir)
        df_list = []

        # Map new scenario names to the names of the files prepared by Shuting
        # TODO: these files should not be preperpared.
        scenario_file_mapping = {
            "locf": "Low_ICF_his",
            "hicf_his": "High_ICF_his",
            "hicf_fair": "High_ICF_coc",
            "rebase": "Baseline",
        }

        # TODO: this should not be hardcoded,
        # it is now manually replace parameters with SSP outputs
        # in cf scenario assumptions
        for scen in SCENARIOS:
            for ssp in SSPS:
                # Use original file name for reading
                original_scen_name = scenario_file_mapping[scen]
                file_name = f"{original_scen_name}_{ssp}_Indicators_2020_2100.xlsx"
                file_path = base_path / file_name
                if not file_path.exists():
                    log.warning(f"File not found, skipping: {file_path}")
                    continue
                df_tmp = pd.read_excel(file_path)
                # Use new scenario name in the data
                df_tmp["Scenario"] = scen
                df_tmp["SSP"] = ssp
                df_list.append(df_tmp)

        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

    # === Step 2: Load and prepare data ===
    log.info("Loading and merging files...")
    df = merge_scen_ssp_files(str(INPUT_DIR))
    df["lnPGDP"] = np.log(df["PGDP"])
    df["lnShareRE"] = np.log(df["ShareRE"])

    # === Step 3: Extract 2020 baseline ===
    log.info("Extracting 2020 baseline data...")
    for tech in TECHS:
        df[tech] = df[tech].replace(0, np.nan)
        df[f"ln_{tech}"] = np.log(df[tech])
        df.loc[df["Year"] == 2020, "ln_" + tech] = np.log(
            df.loc[df["Year"] == 2020, tech]
        )

    base = (
        df[df["Year"] == 2020]
        .drop_duplicates(subset=["Region", "SSP"])
        .loc[
            :,
            [
                "Region",
                "SSP",
                "Total_Fin",
                "lnPGDP",
                "lnShareRE",
                "Rating_based_Default_Spread",
                "R_D_expenditure",
                "FDI",
            ]
            + [f"ln_{t}" for t in TECHS],
        ]
        .rename(columns=lambda c: c + "_base" if c not in ["Region", "SSP"] else c)
    )

    # === Step 4: Merge baseline back to main table ===
    df = df.merge(base, on=["Region", "SSP"], how="left")

    # === Step 5: Calculate deltas ===
    log.info("Calculating deltas from baseline...")
    df["d_Total_Fin"] = df["Total_Fin"] - df["Total_Fin_base"]
    df["d_lnPGDP"] = df["lnPGDP"] - df["lnPGDP_base"]
    df["d_lnShareRE"] = df["lnShareRE"] - df["lnShareRE_base"]
    df["d_Rating_based_Default_Spread"] = (
        df["Rating_based_Default_Spread"] - df["Rating_based_Default_Spread_base"]
    )
    df["d_R_D_expenditure"] = df["R_D_expenditure"] - df["R_D_expenditure_base"]
    df["d_FDI"] = df["FDI"] - df["FDI_base"]

    # === Step 6: Load regression coefficients ===
    log.info("Loading regression coefficients...")
    coeff_dir = package_data_path("investment")
    coeff_path = coeff_dir / REG_COEFF_PATH
    coeff_df = pd.read_csv(coeff_path, index_col=0)

    # Clean index and column names: strip whitespaces
    coeff_df.index = coeff_df.index.astype(str).str.strip()
    coeff_df.columns = coeff_df.columns.astype(str).str.strip()

    # Treat invalid values (e.g., '鈥', '–', '-') as missing (NaN)
    bad_values = {"鈥", "–", "-", "", "nan", "NaN", "None"}
    coeff_df.replace(bad_values, np.nan, inplace=True)

    # Force all entries to numeric; non-convertible values become NaN
    for col in coeff_df.columns:
        coeff_df[col] = pd.to_numeric(coeff_df[col], errors="coerce")

    # Remove unnecessary rows if present
    if "Observations" in coeff_df.index:
        coeff_df = coeff_df.drop(index="Observations")

    # Build coefficient dictionary for each technology (drop NaNs)
    coeffs = {
        tech.lower(): coeff_df[tech].dropna().to_dict() for tech in coeff_df.columns
    }

    # === Step 7: Predict ln(WACC) and convert to WACC ===
    log.info("Predicting WACC using regression coefficients...")
    for tech, coef_dict in coeffs.items():
        base_ln_col = f"ln_{tech}_base"
        ln_pred_col = f"ln_pred_{tech}"
        pred_col = f"pred_{tech}"

        df[ln_pred_col] = df[base_ln_col]
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
    log.info("Reshaping final results...")
    final_columns = ["Region", "Year", "Scenario", "SSP"] + TECHS
    df_result = df[final_columns].copy()

    df_result = df_result.melt(
        id_vars=["Region", "Year", "Scenario", "SSP"],
        value_vars=TECHS,
        var_name="Tech",
        value_name="WACC",
    )

    # === Step 9.1: Add 'cwacc' scenario assumptions(fixed at 2020 values) ===
    years = sorted(df["Year"].unique())
    base_wacc = df.loc[df["Year"] == 2020, ["Region", "SSP"] + TECHS].copy()
    cwacc_long = base_wacc.melt(
        id_vars=["Region", "SSP"], value_vars=TECHS, var_name="Tech", value_name="WACC"
    )

    years_df = pd.DataFrame({"Year": years})
    cwacc_long["key"] = 1
    years_df["key"] = 1
    cwacc_expanded = cwacc_long.merge(years_df, on="key").drop(columns="key")

    cwacc_expanded["Scenario"] = "cwacc"
    cwacc_expanded = cwacc_expanded[
        ["Region", "Year", "Scenario", "SSP", "Tech", "WACC"]
    ]

    df_result_all = pd.concat([df_result, cwacc_expanded], ignore_index=True)

    # Filter regions with NaN
    regions_with_nan = df_result_all[df_result_all["WACC"].isna()]["Region"].unique()
    df_result_all = df_result_all[~df_result_all["Region"].isin(regions_with_nan)]

    # === Step 10: Export result ===
    log.info("Saving WACC projections...")

    # Create output directory if it doesn't exist
    output_dir = package_data_path("investment")
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"WACC files will be saved to: {output_dir}")

    # Save individual files for each SSP and scenario combination
    for ssp in SSPS:
        for scen in SCENARIOS:
            # Filter data for this specific SSP and scenario combination
            ssp_scenario_data = df_result_all[
                (df_result_all["SSP"] == ssp) & (df_result_all["Scenario"] == scen)
            ]
            if not ssp_scenario_data.empty:
                output_path = output_dir / f"predicted_wacc_{ssp.lower()}_{scen}.csv"
                ssp_scenario_data.to_csv(output_path, index=False)
                log.info(f"Saved WACC projections for {ssp} {scen} to {output_path}")

    # Also save the combined file for backward compatibility
    combined_output_path = output_dir / "predicted_wacc.csv"
    df_result_all.to_csv(combined_output_path, index=False)
    log.info(f"Saved combined WACC projections to {combined_output_path}")

    log.info("WACC projection completed.")

    return None
