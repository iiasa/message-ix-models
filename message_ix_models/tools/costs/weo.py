"""
Code for handling IEA WEO data
"""

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path


def get_weo_data():
    """
    Read in raw WEO investment/capital costs and O&M costs data (for all technologies and for STEPS scenario only).
    Convert to long format

    Returns DataFrame of processed data
    """

    # Read in raw data file
    file_path = package_data_path(
        "iea", "WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb"
    )

    # Dict of all of the technologies, their respective sheet in the Excel file, and the start row
    tech_rows = {
        "steam_coal_subcritical": ["Coal", 5],
        "steam_coal_supercritical": ["Coal", 15],
        "steam_coal_ultrasupercritical": ["Coal", 25],
        "igcc": ["Coal", 35],
        "ccgt": ["Gas", 5],
        "gas_turbine": ["Gas", 15],
        "ccgt_chp": ["Gas", 25],
        "fuel_cell": ["Gas", 35],
        "pulverized_coal_ccs": ["Fossil fuels equipped with CCUS", 5],
        "igcc_ccs": ["Fossil fuels equipped with CCUS", 15],
        "ccgt_ccs": ["Fossil fuels equipped with CCUS", 25],
        "nuclear": ["Nuclear", 5],
        "solarpv_large": ["Renewables", 5],
        "solarpv_buildings": ["Renewables", 15],
        "wind_onshore": ["Renewables", 25],
        "wind_offshore": ["Renewables", 35],
        "hydropower_large": ["Renewables", 45],
        "hydropower_small": ["Renewables", 55],
        "bioenergy_large": ["Renewables", 65],
        "bioenergy_cofiring": ["Renewables", 75],
        "bioenergy_medium_chp": ["Renewables", 85],
        "bioenergy_ccus": ["Renewables", 95],
        "csp": ["Renewables", 105],
        "geothermal": ["Renewables", 115],
        "marine": ["Renewables", 125],
    }

    # Specify cost types to read in and the required columns
    cost_cols = {"capital_costs": "A,B:D", "annual_om_costs": "A,F:H"}

    # Loop through each technology and cost type
    # Read in data and convert to long format
    dfs_cost = []
    for tech_key in tech_rows:
        for cost_key in cost_cols:
            df = pd.read_excel(
                file_path,
                sheet_name=tech_rows[tech_key][0],
                header=None,
                skiprows=tech_rows[tech_key][1],
                nrows=8,
                usecols=cost_cols[cost_key],
            )

            df.columns = ["region", "2021", "2030", "2050"]
            df_long = pd.melt(
                df, id_vars=["region"], var_name="year", value_name="value"
            )

            df_long["scenario"] = "stated_policies"
            df_long["technology"] = tech_key
            df_long["cost_type"] = cost_key
            df_long["units"] = "usd_per_kw"

            # Reorganize columns
            df_long = df_long[
                [
                    "scenario",
                    "technology",
                    "region",
                    "year",
                    "cost_type",
                    "units",
                    "value",
                ]
            ]

            dfs_cost.append(df_long)

    all_cost_df = pd.concat(dfs_cost)

    return all_cost_df


"""
Match each R11 region with a WEO region
"""
dict_weo_r11 = {
    "NAM": "United States",
    "LAM": "Brazil",
    "WEU": "Europe",
    "EEU": "Russia",
    "FSU": "Russia",
    "AFR": "Africa",
    "MEA": "Middle East",
    "SAS": "India",
    "CPA": "China",
    "PAS": "India",
    "PAO": "Japan",
}


def get_cost_ratios(dict_reg):
    """
    Returns DataFrame of cost ratios (investment cost and O&M cost) for each R11 region, for each technology

    Only returns values for the earliest year in the dataset (which, as of writing, is 2021)
    """

    # Get processed WEO data
    df_weo = get_weo_data()

    # Replace "n.a." strings with NaNs
    df_weo["value"] = df_weo["value"].replace("n.a.", np.nan)

    # Filter for only United States data (this is the NAM region)
    df_us = df_weo.loc[df_weo.region == "United States"].copy()

    # Rename the `value` column in the US dataframe to `us_value`
    df_us.rename(columns={"value": "us_value"}, inplace=True)

    # Drop `region`` and `units`` columns
    df_us.drop(columns={"region", "units"}, inplace=True)

    # Merge complete WEO data with only US data
    df_merged = pd.merge(
        df_weo, df_us, on=["scenario", "technology", "year", "cost_type"]
    )

    # Calculate cost ratio (region-specific cost divided by US value)
    df_merged["cost_ratio"] = df_merged["value"] / df_merged["us_value"]

    l_cost_ratio = []
    for m, w in dict_reg.items():
        df_sel = df_merged.loc[df_merged.year == min(df_merged.year)]
        df_sel = df_sel.loc[df_sel.region == w].copy()
        df_sel.rename(columns={"region": "weo_region"}, inplace=True)
        df_sel["r11_region"] = m

        df_sel = df_sel[
            [
                "scenario",
                "technology",
                "r11_region",
                "weo_region",
                "year",
                "cost_type",
                "cost_ratio",
            ]
        ]

        # df_sel = df_sel.loc[df_sel.year == min(df_sel.year)]

        l_cost_ratio.append(df_sel)

    df_cost_ratio = pd.concat(l_cost_ratio)
    df_cost_ratio.loc[df_cost_ratio.cost_ratio.isnull()]

    # Replace NaN cost ratios with assumptions
    # Assumption 1: For CSP in EEU and FSU, make cost ratio == 0
    df_cost_ratio.loc[
        (df_cost_ratio.technology == "csp")
        & (df_cost_ratio.r11_region.isin(["EEU", "FSU"])),
        "cost_ratio",
    ] = 0

    # Assumption 2: For pulverized coal with CCS and IGCC with CCS in MEA,
    # make cost ratio the same as in the FSU region
    # TODO: this method to replace the values seems a little prone to errors, so probably best to change later
    df_cost_ratio.loc[
        (df_cost_ratio.cost_ratio.isnull()) & (df_cost_ratio.r11_region == "MEA"),
        "cost_ratio",
    ] = df_cost_ratio.loc[
        (df_cost_ratio.r11_region == "FSU")
        & (df_cost_ratio.technology.isin(["pulverized_coal_ccs", "igcc_ccs"]))
    ].cost_ratio.values

    # Assumption 3: For CSP in PAO, assume the same as NAM region (cost ratio == 1)
    df_cost_ratio.loc[
        (df_cost_ratio.technology == "csp") & (df_cost_ratio.r11_region.isin(["PAO"])),
        "cost_ratio",
    ] = 1

    return df_cost_ratio


"""
Match each MESSAGEix technology with a WEO technology
"""
dict_weo_technologies = {
    "coal_ppl": "steam_coal_subcritical",
    "gas_ppl": "gas_turbine",
    "gas_ct": "gas_turbine",
    "gas_cc": "ccgt",
    "bio_ppl": "bioenergy_large",
    "coal_adv": "steam_coal_supercritical",
    "igcc": "igcc",
    "bio_istig": "igcc",
    "coal_adv_ccs": "pulverized_coal_ccs",
    "igcc_ccs": "igcc_ccs",
    "gas_cc_ccs": "ccgt_ccs",
    "bio_istig_ccs": "igcc_ccs",
    "syn_liq": "igcc",
    "meth_coal": "igcc",
    "syn_liq_ccs": "igcc_ccs",
    "meth_coal_ccs": "igcc_ccs",
    "h2_coal": "igcc",
    "h2_smr": "igcc",
    "h2_bio": "igcc",
    "h2_coal_ccs": "igcc_ccs",
    "h2_smr_ccs": "igcc_ccs",
    "h2_bio_ccs": "igcc_ccs",
    "eth_bio": "igcc",
    "eth_bio_ccs": "igcc_ccs",
    "c_ppl_co2scr": "pulverized_coal_ccs",
    "g_ppl_co2scr": "ccgt_ccs",
    "bio_ppl_co2scr": "igcc_ccs",
    "wind_ppl": "wind_onshore",
    "wind_ppf": "wind_offshore",
    "solar_th_ppl": "csp",
    "solar_pv_I": "solarpv_buildings",
    "solar_pv_RC": "solarpv_buildings",
    "solar_pv_ppl": "solarpv_large",
    "geo_ppl": "geothermal",
    "hydro_lc": "hydropower_large",
    "hydro_hc": "hydropower_small",
    "meth_ng": "igcc",
    "meth_ng_ccs": "igcc_ccs",
    "coal_ppl_u": "steam_coal_subcritical",
    "stor_ppl": "",
    "h2_elec": "",
    "liq_bio": "igcc",
    "liq_bio_ccs": "igcc_ccs",
    "coal_i": "ccgt_chp",
    "foil_i": "ccgt_chp",
    "loil_i": "ccgt_chp",
    "gas_i": "ccgt_chp",
    "biomass_i": "bioenergy_medium_chp",
    "eth_i": "bioenergy_medium_chp",
    "meth_i": "bioenergy_medium_chp",
    "elec_i": "ccgt_chp",
    "h2_i": "ccgt_chp",
    "hp_el_i": "ccgt_chp",
    "hp_gas_i": "ccgt_chp",
    "solar_i": "solarpv_buildings",
    "heat_i": "ccgt_chp",
    "geo_hpl": "geothermal",
    "nuc_lc": "nuclear",
    "nuc_hc": "nuclear",
    "csp_sm1_ppl": "csp",
    "csp_sm3_ppl": "csp",
}
