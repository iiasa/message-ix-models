"""Climate Finance Scenario Generation for Investment Cost Modeling.

Authors: Shuting Fan, Yiyi Ju

This module generates three different climate finance scenarios for future projections:
- locf: Constant financing (no growth)
- hicf_his: Path dependency (linear growth preserving 2022 shares)
- hicf_fair: Fairness weighted (redistributed based on per-capita CO2 emissions)

TODO: properly rename the scenarios to the final scenario matrix
"""

import logging
from pathlib import Path

import pandas as pd

from message_ix_models.util import package_data_path, private_data_path

log = logging.getLogger(__name__)


def main():
    """Generate climate finance scenarios for future projections.

    Returns
    -------
    None
    """
    # === Config ===
    ICF_HIS_XLSX = "ICF_R12.xlsx"
    CO2_XLSX = "CarbonEmission.xlsx"
    POP_XLSX = "Population_WB.xlsx"
    growth_factor = 10  # You can freely change this (e.g., 5, 8, 15)

    # === Load input data ===
    log.info("Loading historical climate finance and CO2 emission data...")

    df_hist = pd.read_excel(
        private_data_path("investment", ICF_HIS_XLSX)
    )  # Historical climate finance (2000–2022)
    co2_region = pd.read_excel(
        private_data_path("investment", CO2_XLSX)
    )  # Historical cumulative CO2 emissions by region

    # Load population data and extract 2022 total population by country
    # (World Bank format)
    pop_df = pd.read_excel(private_data_path("investment", POP_XLSX), sheet_name=0)
    pop_df = (
        pop_df[pop_df["Series Code"] == "SP.POP.TOTL"]
        .rename(columns={"Country Code": "iso"})[["iso", "2022 [YR2022]"]]
        .rename(columns={"2022 [YR2022]": "pop2022"})
    )

    # === Define future global financing target for 2050 ===
    log.info(f"Setting up global financing target with growth factor: {growth_factor}")

    global_fin = df_hist.groupby("Year")["Total_Fin"].sum()
    gf2022 = global_fin.loc[2022]
    gf2050 = gf2022 * growth_factor

    # Compute 2022 share for path dependency scenario
    fin2022 = df_hist[df_hist["Year"] == 2022].set_index("Region")["Total_Fin"]
    share2020 = fin2022 / fin2022.sum()

    # Path dependency: compute fund_s1_2050
    co2_region["share_s1"] = co2_region["Region"].map(share2020)
    co2_region["fund_s1_2050"] = gf2050 * co2_region["share_s1"]

    # Generate CF series
    log.info("Generating climate finance series...")

    scenario0_df = generate_constant_financing_scenario(df_hist)
    scenario1_df = generate_path_dependency_scenario(df_hist, co2_region)
    scenario2_df = generate_fairness_weighted_scenario(df_hist, co2_region, pop_df)

    # Save outputs
    log.info("Saving climate finance scenario series...")

    # Save to the package data directory
    output_dir = package_data_path("investment")
    output_dir.mkdir(parents=True, exist_ok=True)

    scenario0_df.to_csv(output_dir / "locf.csv", index=False)
    scenario1_df.to_csv(output_dir / "hicf_his.csv", index=False)
    scenario2_df.to_csv(output_dir / "hicf_fair.csv", index=False)

    log.info("Climate finance series generation completed.")

    return None


# -------------------------------
# Scenario assumption generation functions
# -------------------------------


def generate_constant_financing_scenario(df_hist):
    """
    Scenario 0:
    Assumes that each region continues to receive the same absolute financing as in
    2022. No growth or redistribution.
    """
    records = []
    last_cum = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Cum_Total_Fin"].to_dict()
    )
    fin2022 = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Total_Fin"].to_dict()
    )

    for _, row in df_hist.iterrows():
        records.append(
            {
                "Region": row["Region"],
                "Year": row["Year"],
                "Total_Fin": row["Total_Fin"],
                "Cum_Total_Fin": row["Cum_Total_Fin"],
            }
        )

    for year in range(2023, 2101):
        for region in df_hist["Region"].unique():
            fin = fin2022.get(region, 0.0)
            last_cum[region] += fin
            records.append(
                {
                    "Region": region,
                    "Year": year,
                    "Total_Fin": fin,
                    "Cum_Total_Fin": last_cum[region],
                }
            )

    return pd.DataFrame(records).sort_values(["Region", "Year"])


def generate_path_dependency_scenario(df_hist, co2_region):
    """
    Scenario 1:
    Financing grows linearly from 2022 levels to 2050 targets while preserving each
    region's 2022 share.
    """
    records = []
    last_cum = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Cum_Total_Fin"].to_dict()
    )
    fin2022 = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Total_Fin"].to_dict()
    )

    for _, row in df_hist.iterrows():
        records.append(
            {
                "Region": row["Region"],
                "Year": row["Year"],
                "Total_Fin": row["Total_Fin"],
                "Cum_Total_Fin": row["Cum_Total_Fin"],
            }
        )

    for year in range(2023, 2101):
        t, span = year - 2022, 2050 - 2022
        for _, r in co2_region.iterrows():
            region = r["Region"]
            f2022 = fin2022.get(region, 0.0)
            if pd.isna(r["fund_s1_2050"]):
                continue
            if year < 2050:
                fin = f2022 + (r["fund_s1_2050"] - f2022) * (t / span)
            else:
                fin = r["fund_s1_2050"]
            last_cum[region] += fin
            records.append(
                {
                    "Region": region,
                    "Year": year,
                    "Total_Fin": fin,
                    "Cum_Total_Fin": last_cum[region],
                }
            )

    return pd.DataFrame(records).sort_values(["Region", "Year"])


def generate_fairness_weighted_scenario(
    df_hist, co2_region, pop_df, gf2050=None, eps=1e-3, p=5
):
    """
    Scenario 2:
    Financing is redistributed based on inverse per-capita CO2 emissions since 1850.
    Regions with lower historical per-capita emissions receive more support.

    Parameters:
        df_hist (pd.DataFrame): Historical finance records (with Region, Year,
            Total_Fin, Cum_Total_Fin)
        co2_region (pd.DataFrame): Region-level CO2 since 1850
        pop_df (pd.DataFrame): ISO-level 2022 population data
        gf2050 (float): Global financing target for 2050 (optional;
            auto-computed if None)
        eps (float): Small value to avoid divide-by-zero in per-capita CO2
        p (int): Power exponent to control fairness weighting strength

    Returns:
        pd.DataFrame: Scenario dataframe (Region, Year, Total_Fin, Cum_Total_Fin)
    """
    # Import get_region function locally to avoid relative import issues
    import importlib.util

    current_dir = Path(__file__).parent
    region_utils_path = current_dir / "Region_utils.py"
    spec = importlib.util.spec_from_file_location("region_utils", region_utils_path)
    region_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(region_utils)
    get_region = region_utils.get_region

    # --- Step 1: Calculate fairness weights ---
    pop_df = pop_df.copy()
    pop_df["Region"] = pop_df["iso"].apply(get_region)
    pop_region = pop_df.groupby("Region", as_index=False)["pop2022"].sum()
    pop_region_dict = dict(zip(pop_region["Region"], pop_region["pop2022"]))

    co2_region = co2_region.copy()
    co2_region["pop2022"] = co2_region["Region"].map(pop_region_dict)
    co2_region["co2_percap"] = co2_region["co2_since_1850"] / co2_region["pop2022"]
    co2_region["inv_pc"] = 1.0 / (co2_region["co2_percap"] + eps) ** p
    co2_region["share_pc"] = co2_region["inv_pc"] / co2_region["inv_pc"].sum()

    # --- Step 2: Determine 2050 financing target ---
    if gf2050 is None:
        gf2022 = df_hist.groupby("Year")["Total_Fin"].sum().loc[2022]
        gf2050 = gf2022 * 10

    co2_region["fund_s2_pc_2050"] = gf2050 * co2_region["share_pc"]

    # --- Step 3: Build financing trajectory (2000–2100) ---
    records = []
    last_cum = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Cum_Total_Fin"].to_dict()
    )
    fin2022 = (
        df_hist[df_hist["Year"] == 2022].set_index("Region")["Total_Fin"].to_dict()
    )

    # Add historical records
    for _, row in df_hist.iterrows():
        records.append(
            {
                "Region": row["Region"],
                "Year": row["Year"],
                "Total_Fin": row["Total_Fin"],
                "Cum_Total_Fin": row["Cum_Total_Fin"],
            }
        )

    # Add future projections
    for year in range(2023, 2101):
        t, span = year - 2022, 2050 - 2022
        for _, r in co2_region.iterrows():
            region = r["Region"]
            if region not in last_cum or pd.isna(r["fund_s2_pc_2050"]):
                continue  # 🔐 Skip if no initial financing or no target
            f2022 = fin2022.get(region, 0.0)
            if pd.isna(r["fund_s2_pc_2050"]):
                continue
            if year < 2050:
                fin = f2022 + (r["fund_s2_pc_2050"] - f2022) * (t / span)
            else:
                fin = r["fund_s2_pc_2050"]
            last_cum[region] += fin
            records.append(
                {
                    "Region": region,
                    "Year": year,
                    "Total_Fin": fin,
                    "Cum_Total_Fin": last_cum[region],
                }
            )

    return pd.DataFrame(records).sort_values(["Region", "Year"])
