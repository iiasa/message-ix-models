"""
Code for handling IEA WEO data
"""

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path


def get_weo_data():
    """
    Read in raw WEO investment/capital costs and O&M costs data
    (for all technologies and for STEPS scenario only).
    Convert to long format

    Returns DataFrame of processed data
    """

    # Read in raw data file
    file_path = package_data_path(
        "iea", "WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb"
    )

    # Dict of all of the technologies, their respective sheet in the Excel file,
    # and the start row
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
    "WEU": "European Union",
    "EEU": "Russia",
    "FSU": "Russia",
    "AFR": "Africa",
    "MEA": "Middle East",
    "SAS": "India",
    "CPA": "China",
    "PAS": "India",
    "PAO": "Japan",
}


def calculate_cost_ratios(weo_df, dict_reg):
    """
    Returns DataFrame of cost ratios (investment cost and O&M cost) for each R11 region,
    for each technology

    Only returns values for the earliest year in the dataset
    (which, as of writing, is 2021)
    """

    # Replace "n.a." strings with NaNs
    weo_df["value"] = weo_df["value"].replace("n.a.", np.nan)

    # Filter for only United States data (this is the NAM region)
    df_us = weo_df.loc[weo_df.region == "United States"].copy()

    # Rename the `value` column in the US dataframe to `us_value`
    df_us.rename(columns={"value": "us_value"}, inplace=True)

    # Drop `region`` and `units`` columns
    df_us.drop(columns={"region", "units"}, inplace=True)

    # Merge complete WEO data with only US data
    df_merged = pd.merge(
        weo_df, df_us, on=["scenario", "technology", "year", "cost_type"]
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
    # TODO: this method to replace the values seems a little prone to errors,
    # so probably best to change later
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

first_model_year = 2020
conversion_2017_to_2005_usd = 83.416 / 103.015


def get_cost_assumption_data():
    # Read in raw data files
    inv_file_path = package_data_path("costs", "eric-investment-costs.csv")
    fom_file_path = package_data_path("costs", "eric-fom-costs.csv")

    df_inv = pd.read_csv(inv_file_path, header=8)
    df_fom = pd.read_csv(fom_file_path, header=8)

    # Rename columns
    df_inv.rename(
        columns={"investment_cost_nam_original_message": "cost_NAM_original_message"},
        inplace=True,
    )
    df_fom.rename(
        columns={"fom_cost_nam_original_message": "cost_NAM_original_message"},
        inplace=True,
    )

    # Add cost type column
    df_inv["cost_type"] = "capital_costs"
    df_fom["cost_type"] = "annual_om_costs"

    # Concatenate dataframes
    df_costs = pd.concat([df_inv, df_fom]).reset_index()
    df_costs = df_costs[
        [
            "message_technology",
            "cost_type",
            "cost_NAM_original_message",
        ]
    ]

    return df_costs


def compare_original_and_weo_nam_costs(
    weo_df, eric_df, dict_weo_tech, dict_weo_regions
):
    df_assumptions = eric_df.copy()
    df_assumptions["technology"] = df_assumptions.message_technology.map(dict_weo_tech)

    df_nam = weo_df.loc[
        (weo_df.region == dict_weo_regions["NAM"]) & (weo_df.year == min(weo_df.year))
    ].copy()

    df_nam_assumptions = pd.merge(
        df_assumptions, df_nam, on=["technology", "cost_type"], how="left"
    )
    df_nam_assumptions.drop(
        columns={"year", "region", "units", "scenario"}, inplace=True
    )
    df_nam_assumptions.rename(
        columns={"value": "cost_NAM_weo_2021", "technology": "weo_technology"},
        inplace=True,
    )
    df_nam_assumptions = df_nam_assumptions[
        [
            "message_technology",
            "weo_technology",
            "cost_type",
            "cost_NAM_original_message",
            "cost_NAM_weo_2021",
        ]
    ]

    return df_nam_assumptions


# Type 1: WEO * conversion rate
def adj_nam_cost_conversion(df_costs, conv_rate):
    df_costs["cost_NAM_adjusted"] = df_costs["cost_NAM_weo_2021"] * conv_rate


# Type 2: Same as NAM original MESSAGE
tech_same_orig_message_inv = [
    "c_ppl_co2scr",
    "g_ppl_co2scr",
    "bio_ppl_co2scr",
    "stor_ppl",
    "coal_i",
    "foil_i",
    "loil_i",
    "gas_i",
    "biomass_i",
    "eth_i",
    "meth_i",
    "elec_i",
    "h2_i",
    "hp_el_i",
    "hp_gas_i",
    "heat_i",
    "geo_hpl",
    "nuc_lc",
    "nuc_hc",
    "csp_sm1_ppl",
    "csp_sm3_ppl",
]

tech_same_orig_message_fom = [
    "stor_ppl",
    "coal_i",
    "foil_i",
    "loil_i",
    "gas_i",
    "biomass_i",
    "eth_i",
    "meth_i",
    "elec_i",
    "h2_i",
    "hp_el_i",
    "hp_gas_i",
    "heat_i",
]


def adj_nam_cost_message(df_costs, list_tech_inv, list_tech_fom):
    df_costs.loc[
        (df_costs.message_technology.isin(list_tech_inv))
        & (df_costs.cost_type == "capital_costs"),
        "cost_NAM_adjusted",
    ] = df_costs.loc[
        (df_costs.message_technology.isin(list_tech_inv))
        & (df_costs.cost_type == "capital_costs"),
        "cost_NAM_original_message",
    ]

    df_costs.loc[
        (df_costs.message_technology.isin(list_tech_fom))
        & (df_costs.cost_type == "annual_om_costs"),
        "cost_NAM_adjusted",
    ] = df_costs.loc[
        (df_costs.message_technology.isin(list_tech_fom))
        & (df_costs.cost_type == "annual_om_costs"),
        "cost_NAM_original_message",
    ]


# Type 3: Manually assigned values
dict_manual_nam_costs_inv = {
    "bio_istig": 4064,
    "bio_istig_ccs": 5883,
    "syn_liq": 3224,  # US EIA
    "h2_coal": 2127,  # IEA Future H2
    "h2_smr": 725,  # IEA Future H2
    "h2_coal_ccs": 2215,
    "h2_smr_ccs": 1339,
    "wind_ppl": 1181,
    "wind_ppf": 1771,
    "solar_pv_ppl": 1189,
    "geo_ppl": 3030,
    "h2_elec": 1120,
    "liq_bio": 4264,
}

dict_manual_nam_costs_fom = {
    "bio_istig": 163,
    "bio_istig_ccs": 235,
    "syn_liq": 203,
    "h2_coal": 106,
    "h2_smr": 34,
    "h2_coal_ccs": 111,
    "h2_smr_ccs": 40,
    "wind_ppl": 27,
    "wind_ppf": 48,
    "h2_elec": 17,
    "liq_bio": 171,
    "liq_bio_ccs": 174,
}


def adj_nam_cost_manual(df_costs, dict_inv, dict_fom):
    for k in dict_inv:
        df_costs.loc[
            (df_costs.message_technology == k)
            & (df_costs.cost_type == "capital_costs"),
            "cost_NAM_adjusted",
        ] = dict_inv[k]

    for f in dict_fom:
        df_costs.loc[
            (df_costs.message_technology == f)
            & (df_costs.cost_type == "annual_om_costs"),
            "cost_NAM_adjusted",
        ] = dict_fom[f]


# Type 4: function of another cost value (using ratio)
def calc_nam_cost_ratio(
    df_costs, desired_tech, desired_cost_type, reference_tech, reference_cost_type
):
    c_adj_ref = df_costs.loc[
        (df_costs.message_technology == reference_tech)
        & (df_costs.cost_type == reference_cost_type),
        "cost_NAM_adjusted",
    ].values[0]

    orig_des = df_costs.loc[
        (df_costs.message_technology == desired_tech)
        & (df_costs.cost_type == desired_cost_type),
        "cost_NAM_original_message",
    ].values[0]

    orig_ref = df_costs.loc[
        (df_costs.message_technology == reference_tech)
        & (df_costs.cost_type == reference_cost_type),
        "cost_NAM_original_message",
    ].values[0]

    c_adj_des = c_adj_ref * (orig_des / orig_ref)

    df_costs.loc[
        (df_costs.message_technology == desired_tech)
        & (df_costs.cost_type == desired_cost_type),
        "cost_NAM_adjusted",
    ] = c_adj_des

    # return c_adj_des


dict_tech_ref_inv = {
    "gas_ppl": {"reference_tech": "gas_cc", "reference_cost_type": "capital_costs"},
    "meth_coal": {"reference_tech": "syn_liq", "reference_cost_type": "capital_costs"},
    "syn_liq_ccs": {
        "reference_tech": "syn_liq",
        "reference_cost_type": "capital_costs",
    },
    "meth_coal_ccs": {
        "reference_tech": "meth_coal",
        "reference_cost_type": "capital_costs",
    },
    "h2_bio": {"reference_tech": "h2_coal", "reference_cost_type": "capital_costs"},
    "h2_bio_ccs": {"reference_tech": "h2_bio", "reference_cost_type": "capital_costs"},
    "eth_bio": {"reference_tech": "liq_bio", "reference_cost_type": "capital_costs"},
    "eth_bio_ccs": {
        "reference_tech": "eth_bio",
        "reference_cost_type": "capital_costs",
    },
    "solar_th_ppl": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "capital_costs",
    },
    "solar_pv_I": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "capital_costs",
    },
    "solar_pv_RC": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "capital_costs",
    },
    "meth_ng": {"reference_tech": "syn_liq", "reference_cost_type": "capital_costs"},
    "meth_ng_ccs": {
        "reference_tech": "meth_ng",
        "reference_cost_type": "capital_costs",
    },
    "coal_ppl_u": {
        "reference_tech": "coal_ppl",
        "reference_cost_type": "capital_costs",
    },
    "liq_bio_ccs": {
        "reference_tech": "liq_bio",
        "reference_cost_type": "capital_costs",
    },
    "solar_i": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "capital_costs",
    },
}

dict_tech_ref_fom = {
    "gas_ppl": {"reference_tech": "gas_cc", "reference_cost_type": "annual_om_costs"},
    "meth_coal": {
        "reference_tech": "syn_liq",
        "reference_cost_type": "annual_om_costs",
    },
    "syn_liq_ccs": {
        "reference_tech": "syn_liq",
        "reference_cost_type": "annual_om_costs",
    },
    "meth_coal_ccs": {
        "reference_tech": "meth_coal",
        "reference_cost_type": "annual_om_costs",
    },
    "h2_bio": {"reference_tech": "h2_coal", "reference_cost_type": "annual_om_costs"},
    "h2_bio_ccs": {
        "reference_tech": "h2_bio",
        "reference_cost_type": "annual_om_costs",
    },
    "eth_bio": {"reference_tech": "liq_bio", "reference_cost_type": "annual_om_costs"},
    "eth_bio_ccs": {
        "reference_tech": "eth_bio",
        "reference_cost_type": "annual_om_costs",
    },
    "solar_th_ppl": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "annual_om_costs",
    },
    "solar_pv_I": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "annual_om_costs",
    },
    "solar_pv_RC": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "annual_om_costs",
    },
    "meth_ng": {"reference_tech": "syn_liq", "reference_cost_type": "annual_om_costs"},
    "meth_ng_ccs": {
        "reference_tech": "meth_ng",
        "reference_cost_type": "annual_om_costs",
    },
    "coal_ppl_u": {
        "reference_tech": "coal_ppl",
        "reference_cost_type": "annual_om_costs",
    },
    "liq_bio_ccs": {
        "reference_tech": "liq_bio",
        "reference_cost_type": "annual_om_costs",
    },
    "solar_i": {
        "reference_tech": "solar_pv_ppl",
        "reference_cost_type": "annual_om_costs",
    },
}


def adj_nam_cost_reference(df_costs, dict_inv, dict_fom):
    for m in dict_inv:
        calc_nam_cost_ratio(
            df_costs,
            m,
            "capital_costs",
            dict_inv[m]["reference_tech"],
            dict_inv[m]["reference_cost_type"],
        )

    for n in dict_fom:
        calc_nam_cost_ratio(
            df_costs,
            n,
            "annual_om_costs",
            dict_fom[n]["reference_tech"],
            dict_fom[n]["reference_cost_type"],
        )


def get_region_differentiated_costs():
    df_weo = get_weo_data()
    df_eric = get_cost_assumption_data()
    df_nam_costs = compare_original_and_weo_nam_costs(
        df_weo, df_eric, dict_weo_technologies, dict_weo_r11
    )

    adj_nam_cost_conversion(df_nam_costs, conversion_2017_to_2005_usd)
    adj_nam_cost_message(
        df_nam_costs, tech_same_orig_message_inv, tech_same_orig_message_fom
    )
    adj_nam_cost_manual(
        df_nam_costs, dict_manual_nam_costs_inv, dict_manual_nam_costs_fom
    )
    adj_nam_cost_reference(df_nam_costs, dict_tech_ref_inv, dict_tech_ref_fom)

    df_nam_adj_costs_only = df_nam_costs[
        ["message_technology", "weo_technology", "cost_type", "cost_NAM_adjusted"]
    ]

    # assign fake WEO technology for stor_ppl and h2_elec so that dfs can be merged
    df_nam_adj_costs_only.loc[
        df_nam_adj_costs_only.message_technology.isin(["stor_ppl", "h2_elec"]),
        "weo_technology",
    ] = "marine"

    # get ratios
    df_ratios = calculate_cost_ratios(df_weo, dict_weo_r11)
    df_ratios.rename(columns={"technology": "weo_technology"}, inplace=True)
    df_ratios.drop(columns={"scenario", "year"}, inplace=True)

    # merge costs
    df_regiondiff = pd.merge(
        df_ratios, df_nam_adj_costs_only, on=["weo_technology", "cost_type"]
    )

    # for stor_ppl and h2_elec, make ratios = 1 (all regions have the same cost)
    df_regiondiff.loc[
        df_regiondiff.message_technology.isin(["stor_ppl", "h2_elec"]), "cost_ratio"
    ] = 1.0

    # calculate region-specific costs
    df_regiondiff["cost_region"] = (
        df_regiondiff["cost_NAM_adjusted"] * df_regiondiff["cost_ratio"]
    )

    return df_regiondiff


get_region_differentiated_costs()
