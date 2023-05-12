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
        "coal_ccs": ["Fossil fuels equipped with CCUS", 5],
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
