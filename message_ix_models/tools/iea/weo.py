"""
Code for handling IEA WEO data
"""

import pandas as pd

from message_ix_models.util import package_data_path


def get_weo_data():
    """
    Read in raw WEO investment/capital costs and O&M costs data (for all technologies and for STEPS scenario only).
    Convert to long format
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
