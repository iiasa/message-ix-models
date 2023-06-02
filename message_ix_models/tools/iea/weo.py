"""Code for handling IEA WEO data"""

from itertools import product
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

# Conversion rate from 2017 USD to 2005 USD
# Taken from https://www.officialdata.org/us/inflation/2017?endYear=2005&amount=1
conversion_2017_to_2005_usd = 0.8

# Dict of all of the technologies,
# their respective sheet in the Excel file,
# and the start row
DICT_TECH_ROWS = {
    "bioenergy_ccus": ["Renewables", 95],
    "bioenergy_cofiring": ["Renewables", 75],
    "bioenergy_large": ["Renewables", 65],
    "bioenergy_medium_chp": ["Renewables", 85],
    "ccgt": ["Gas", 5],
    "ccgt_ccs": ["Fossil fuels equipped with CCUS", 25],
    "ccgt_chp": ["Gas", 25],
    "csp": ["Renewables", 105],
    "fuel_cell": ["Gas", 35],
    "gas_turbine": ["Gas", 15],
    "geothermal": ["Renewables", 115],
    "hydropower_large": ["Renewables", 45],
    "hydropower_small": ["Renewables", 55],
    "igcc": ["Coal", 35],
    "igcc_ccs": ["Fossil fuels equipped with CCUS", 15],
    "marine": ["Renewables", 125],
    "nuclear": ["Nuclear", 5],
    "pulverized_coal_ccs": ["Fossil fuels equipped with CCUS", 5],
    "solarpv_buildings": ["Renewables", 15],
    "solarpv_large": ["Renewables", 5],
    "steam_coal_subcritical": ["Coal", 5],
    "steam_coal_supercritical": ["Coal", 15],
    "steam_coal_ultrasupercritical": ["Coal", 25],
    "wind_offshore": ["Renewables", 35],
    "wind_onshore": ["Renewables", 25],
}

# Dict of cost types to read in and the required columns
DICT_COST_COLS = {"capital_costs": "A,B:D", "annual_om_costs": "A,F:H"}

# Dict of each R11 region matched with a WEO region
DICT_WEO_R11 = {
    "AFR": "Africa",
    "CPA": "China",
    "EEU": "Russia",
    "FSU": "Russia",
    "LAM": "Brazil",
    "MEA": "Middle East",
    "NAM": "United States",
    "PAO": "Japan",
    "PAS": "India",
    "SAS": "India",
    "WEU": "European Union",
}

# Dict of WEO technologies and the corresponding MESSAGE technologies
DICT_WEO_TECH = {
    "bio_istig": "igcc",
    "bio_istig_ccs": "igcc_ccs",
    "bio_ppl": "bioenergy_large",
    "bio_ppl_co2scr": "igcc_ccs",
    "biomass_i": "bioenergy_medium_chp",
    "c_ppl_co2scr": "pulverized_coal_ccs",
    "coal_adv": "steam_coal_supercritical",
    "coal_adv_ccs": "pulverized_coal_ccs",
    "coal_i": "ccgt_chp",
    "coal_ppl": "steam_coal_subcritical",
    "coal_ppl_u": "steam_coal_subcritical",
    "csp_sm1_ppl": "csp",
    "csp_sm3_ppl": "csp",
    "elec_i": "ccgt_chp",
    "eth_bio": "igcc",
    "eth_bio_ccs": "igcc_ccs",
    "eth_i": "bioenergy_medium_chp",
    "foil_i": "ccgt_chp",
    "g_ppl_co2scr": "ccgt_ccs",
    "gas_cc": "ccgt",
    "gas_cc_ccs": "ccgt_ccs",
    "gas_ct": "gas_turbine",
    "gas_i": "ccgt_chp",
    "gas_ppl": "gas_turbine",
    "geo_hpl": "geothermal",
    "geo_ppl": "geothermal",
    "h2_bio": "igcc",
    "h2_bio_ccs": "igcc_ccs",
    "h2_coal": "igcc",
    "h2_coal_ccs": "igcc_ccs",
    "h2_elec": "",
    "h2_i": "ccgt_chp",
    "h2_smr": "igcc",
    "h2_smr_ccs": "igcc_ccs",
    "heat_i": "ccgt_chp",
    "hp_el_i": "ccgt_chp",
    "hp_gas_i": "ccgt_chp",
    "hydro_hc": "hydropower_small",
    "hydro_lc": "hydropower_large",
    "igcc": "igcc",
    "igcc_ccs": "igcc_ccs",
    "liq_bio": "igcc",
    "liq_bio_ccs": "igcc_ccs",
    "loil_i": "ccgt_chp",
    "meth_coal": "igcc",
    "meth_coal_ccs": "igcc_ccs",
    "meth_i": "bioenergy_medium_chp",
    "meth_ng": "igcc",
    "meth_ng_ccs": "igcc_ccs",
    "nuc_hc": "nuclear",
    "nuc_lc": "nuclear",
    "solar_i": "solarpv_buildings",
    "solar_pv_I": "solarpv_buildings",
    "solar_pv_RC": "solarpv_buildings",
    "solar_pv_ppl": "solarpv_large",
    "solar_th_ppl": "csp",
    "stor_ppl": "",
    "syn_liq": "igcc",
    "syn_liq_ccs": "igcc_ccs",
    "wind_ppf": "wind_offshore",
    "wind_ppl": "wind_onshore",
}

# Dict of technologies whose NAM investment costs are the same as in MESSAGE
DICT_TECH_SAME_ORIG_MESSAGE_INV = [
    "bio_ppl_co2scr",
    "biomass_i",
    "c_ppl_co2scr",
    "coal_i",
    "csp_sm1_ppl",
    "csp_sm3_ppl",
    "elec_i",
    "eth_i",
    "foil_i",
    "g_ppl_co2scr",
    "gas_i",
    "geo_hpl",
    "h2_i",
    "heat_i",
    "hp_el_i",
    "hp_gas_i",
    "loil_i",
    "meth_i",
    "nuc_hc",
    "nuc_lc",
    "stor_ppl",
]

# Dict of technologies whose NAM FO&M costs are the same as in MESSAGE
DICT_TECH_SAME_ORIG_MESSAGE_FOM = [
    "biomass_i",
    "coal_i",
    "elec_i",
    "eth_i",
    "foil_i",
    "gas_i",
    "h2_i",
    "heat_i",
    "hp_el_i",
    "hp_gas_i",
    "loil_i",
    "meth_i",
    "stor_ppl",
]

# Dict of technologies whose investment costs are manually specified
# Values are taken directly from the "RegionDiff" sheet
# in p:/ene.model/MESSAGE-technology-costs/costs-spreadsheets/SSP1_techinput.xlsx
DICT_MANUAL_NAM_COSTS_INV = {
    "bio_istig": 4064,
    "bio_istig_ccs": 5883,
    "geo_ppl": 3030,
    "h2_coal": 2127,
    "h2_coal_ccs": 2215,
    "h2_elec": 1120,
    "h2_smr": 725,
    "h2_smr_ccs": 1339,
    "liq_bio": 4264,
    "solar_pv_ppl": 1189,
    "syn_liq": 3224,
    "wind_ppf": 1771,
    "wind_ppl": 1181,
}

# Dict of technologies whose FO&M costs are manually specified
# Values are taken directly from the "RegionDiff" sheet
# in p:/ene.model/MESSAGE-technology-costs/costs-spreadsheets/SSP1_techinput.xlsx
DICT_MANUAL_NAM_COSTS_FOM = {
    "bio_istig": 163,
    "bio_istig_ccs": 235,
    "h2_coal": 106,
    "h2_coal_ccs": 111,
    "h2_elec": 17,
    "h2_smr": 34,
    "h2_smr_ccs": 40,
    "liq_bio": 171,
    "liq_bio_ccs": 174,
    "syn_liq": 203,
    "wind_ppf": 48,
    "wind_ppl": 27,
}

# Dict of the technologies whose investment costs are in reference to
# other technologies.
# Within the key, the `tech` refers to the reference tech,
# and the `cost_type` refers to the reference cost type (either investment or FO&M cost)
DICT_TECH_REF_INV = {
    "coal_ppl_u": {
        "tech": "coal_ppl",
        "cost_type": "capital_costs",
    },
    "eth_bio": {"tech": "liq_bio", "cost_type": "capital_costs"},
    "eth_bio_ccs": {
        "tech": "eth_bio",
        "cost_type": "capital_costs",
    },
    "gas_ppl": {"tech": "gas_cc", "cost_type": "capital_costs"},
    "h2_bio": {"tech": "h2_coal", "cost_type": "capital_costs"},
    "h2_bio_ccs": {"tech": "h2_bio", "cost_type": "capital_costs"},
    "liq_bio_ccs": {
        "tech": "liq_bio",
        "cost_type": "capital_costs",
    },
    "meth_coal": {"tech": "syn_liq", "cost_type": "capital_costs"},
    "meth_coal_ccs": {
        "tech": "meth_coal",
        "cost_type": "capital_costs",
    },
    "meth_ng": {"tech": "syn_liq", "cost_type": "capital_costs"},
    "meth_ng_ccs": {
        "tech": "meth_ng",
        "cost_type": "capital_costs",
    },
    "solar_i": {
        "tech": "solar_pv_ppl",
        "cost_type": "capital_costs",
    },
    "solar_pv_I": {
        "tech": "solar_pv_ppl",
        "cost_type": "capital_costs",
    },
    "solar_pv_RC": {
        "tech": "solar_pv_ppl",
        "cost_type": "capital_costs",
    },
    "solar_th_ppl": {
        "tech": "solar_pv_ppl",
        "cost_type": "capital_costs",
    },
    "syn_liq_ccs": {
        "tech": "syn_liq",
        "cost_type": "capital_costs",
    },
}

# Dict of the technologies whose FO&M costs are in reference to other technologies.
# Within the key, the `tech` refers to the reference tech,
# and the `cost_type` refers to the reference cost type (either investment or FO&M cost)
DICT_TECH_REF_FOM = {
    "coal_ppl_u": {
        "tech": "coal_ppl",
        "cost_type": "annual_om_costs",
    },
    "eth_bio": {"tech": "liq_bio", "cost_type": "annual_om_costs"},
    "eth_bio_ccs": {
        "tech": "eth_bio",
        "cost_type": "annual_om_costs",
    },
    "gas_ppl": {"tech": "gas_cc", "cost_type": "annual_om_costs"},
    "h2_bio": {"tech": "h2_coal", "cost_type": "annual_om_costs"},
    "h2_bio_ccs": {
        "tech": "h2_bio",
        "cost_type": "annual_om_costs",
    },
    "liq_bio_ccs": {
        "tech": "liq_bio",
        "cost_type": "annual_om_costs",
    },
    "meth_coal": {
        "tech": "syn_liq",
        "cost_type": "annual_om_costs",
    },
    "meth_coal_ccs": {
        "tech": "meth_coal",
        "cost_type": "annual_om_costs",
    },
    "meth_ng": {"tech": "syn_liq", "cost_type": "annual_om_costs"},
    "meth_ng_ccs": {
        "tech": "meth_ng",
        "cost_type": "annual_om_costs",
    },
    "solar_i": {
        "tech": "solar_pv_ppl",
        "cost_type": "annual_om_costs",
    },
    "solar_pv_I": {
        "tech": "solar_pv_ppl",
        "cost_type": "annual_om_costs",
    },
    "solar_pv_RC": {
        "tech": "solar_pv_ppl",
        "cost_type": "annual_om_costs",
    },
    "solar_th_ppl": {
        "tech": "solar_pv_ppl",
        "cost_type": "annual_om_costs",
    },
    "syn_liq_ccs": {
        "tech": "syn_liq",
        "cost_type": "annual_om_costs",
    },
}


def get_weo_data(
    dict_tech_rows: Dict[str, Tuple[str, int]],
    dict_cols: Dict[str, str],
) -> pd.DataFrame:
    """Read in raw WEO investment/capital costs and O&M costs data.

    Data are read for all technologies and for STEPS scenario only from the file
    :file:`data/iea/WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb`.

    Parameters
    ----------
    dict_tech_rows : str -> tuple of (str, int)
        Keys are the IDs of the technologies for which data are read.
        Values give the sheet name, and the start row.

    Returns
    -------
    pandas.DataFrame
        with columns:

        - year: values from 2021 to 2050, as appearing in the file.
    """
    # Could possibly use the global directly instead of accepting it as an argument
    # dict_tech_rows = DICT_TECH_ROWS

    # Read in raw data file
    file_path = package_data_path(
        "iea", "WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb"
    )

    # Loop through each technology and cost type
    # Read in data and convert to long format
    dfs_cost = []
    for tech_key, cost_key in product(dict_tech_rows, dict_cols):
        df = (
            pd.read_excel(
                file_path,
                sheet_name=dict_tech_rows[tech_key][0],
                header=None,
                skiprows=dict_tech_rows[tech_key][1],
                nrows=8,
                usecols=dict_cols[cost_key],
            )
            .set_axis(["region", "2021", "2030", "2050"], axis=1)
            .melt(id_vars=["region"], var_name="year", value_name="value")
            .assign(
                scenario="stated_policies",
                technology=tech_key,
                cost_type=cost_key,
                units="usd_per_kw",
            )
            .reindex(
                [
                    "scenario",
                    "technology",
                    "region",
                    "year",
                    "cost_type",
                    "units",
                    "value",
                ],
                axis=1,
            )
            .replace({"value": "n.a."}, np.nan)
        )

        dfs_cost.append(df)
    all_cost_df = pd.concat(dfs_cost)

    return all_cost_df


def calculate_region_cost_ratios(weo_df, dict_reg):
    """Return DataFrame of cost ratios (investment cost and O&M cost)
    for each R11 region, for each technology

    Only return values for the earliest year in the dataset
    (which, as of writing, is 2021)
    """

    df = (
        weo_df.loc[weo_df.region == "United States"]
        .copy()
        .rename(columns={"value": "us_value"})
        .drop(columns={"region", "units"})
        .merge(weo_df, on=["scenario", "technology", "year", "cost_type"])
        .assign(cost_ratio=lambda x: x.value / x.us_value)
    )

    l_cost_ratio = []
    for m, w in dict_reg.items():
        df_sel = (
            df.loc[(df.year == min(df.year)) & (df.region == w)]
            .copy()
            .rename(columns={"region": "weo_region"})
            .assign(r11_region=m)
            .reindex(
                [
                    "scenario",
                    "technology",
                    "r11_region",
                    "weo_region",
                    "year",
                    "cost_type",
                    "cost_ratio",
                ],
                axis=1,
            )
        )

        l_cost_ratio.append(df_sel)

    df_cost_ratio = pd.concat(l_cost_ratio)

    # Replace NaN cost ratios with assumptions
    # Assumption 1: For CSP in EEU and FSU, make cost ratio == 0
    df_cost_ratio.loc[
        (df_cost_ratio.technology == "csp")
        & (df_cost_ratio.r11_region.isin(["EEU", "FSU"])),
        "cost_ratio",
    ] = 0

    # Assumption 2: For CSP in PAO, assume the same as NAM region (cost ratio == 1)
    df_cost_ratio.loc[
        (df_cost_ratio.technology == "csp") & (df_cost_ratio.r11_region.isin(["PAO"])),
        "cost_ratio",
    ] = 1

    # Assumption 3: For pulverized coal with CCS and IGCC with CCS in MEA,
    # make cost ratio the same as in the FSU region
    sub_mea = df_cost_ratio[
        (df_cost_ratio.cost_ratio.isnull()) & (df_cost_ratio.r11_region == "MEA")
    ].drop(columns={"cost_ratio"})

    sub_fsu = df_cost_ratio.loc[
        (df_cost_ratio.r11_region == "FSU")
        & (df_cost_ratio.technology.isin(["pulverized_coal_ccs", "igcc_ccs"]))
    ].drop(columns={"weo_region", "r11_region"})

    sub_merge = sub_mea.merge(
        sub_fsu, on=["scenario", "technology", "year", "cost_type"]
    )

    df_cost_ratio_fix = pd.concat(
        [
            df_cost_ratio[
                ~(
                    (df_cost_ratio.cost_ratio.isnull())
                    & (df_cost_ratio.r11_region == "MEA")
                )
            ],
            sub_merge,
        ]
    ).reset_index(drop=1)

    return df_cost_ratio_fix


def get_cost_assumption_data():
    # Read in raw data files
    inv_file_path = package_data_path("costs", "investment_costs-0.csv")
    fom_file_path = package_data_path("costs", "fixed_om_costs-0.csv")

    df_inv = (
        pd.read_csv(inv_file_path, header=9)
        .rename(
            columns={
                "investment_cost_nam_original_message": "cost_NAM_original_message"
            }
        )
        .assign(cost_type="capital_costs")
    )

    df_fom = (
        pd.read_csv(fom_file_path, header=9)
        .rename(columns={"fom_cost_nam_original_message": "cost_NAM_original_message"})
        .assign(cost_type="annual_om_costs")
    )

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
    df_assumptions = (
        eric_df.copy()
        .assign(technology=lambda x: x.message_technology.map(dict_weo_tech))
        .merge(
            weo_df.loc[
                (weo_df.region == dict_weo_regions["NAM"])
                & (weo_df.year == min(weo_df.year))
            ].copy(),
            on=["technology", "cost_type"],
            how="left",
        )
        .drop(columns={"year", "region", "units", "scenario"})
        .rename(columns={"value": "cost_NAM_weo_2021", "technology": "weo_technology"})
        .reindex(
            [
                "message_technology",
                "weo_technology",
                "cost_type",
                "cost_NAM_original_message",
                "cost_NAM_weo_2021",
            ],
            axis=1,
        )
    )

    return df_assumptions


# Type 1: WEO * conversion rate
def adj_nam_cost_conversion(df_costs, conv_rate):
    df_costs["cost_NAM_adjusted"] = df_costs["cost_NAM_weo_2021"] * conv_rate


# Type 2: Same as NAM original MESSAGE
def adj_nam_cost_message(df_costs, list_tech_inv, list_tech_fom):
    mask = (df_costs.message_technology.isin(list_tech_inv)) & (
        df_costs.cost_type == "capital_costs"
    )
    df_costs.loc[mask, "cost_NAM_adjusted"] = df_costs.loc[
        mask, "cost_NAM_original_message"
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


def adj_nam_cost_reference(df_costs, dict_inv, dict_fom):
    for m in dict_inv:
        calc_nam_cost_ratio(
            df_costs,
            m,
            "capital_costs",
            dict_inv[m]["tech"],
            dict_inv[m]["cost_type"],
        )

    for n in dict_fom:
        calc_nam_cost_ratio(
            df_costs,
            n,
            "annual_om_costs",
            dict_fom[n]["tech"],
            dict_fom[n]["cost_type"],
        )


def get_region_differentiated_costs() -> pd.DataFrame:
    """Perform all calculations needed to get regionally-differentiated costs.

    The algorithm is roughly:

    1. Retrieve data with :func:`.get_weo_data` and assumptions with
       :func:`.get_cost_assumption_data`.
    2. Adjust costs for the NAM region with reference to older MESSAGE data.
    3. Compute cost ratios across regions, relative to ``*_NAM``, based on (1).
    4. Apply the ratios from (3) to the adjusted data (2).

    Returns
    -------
    pandas.DataFrame
        with columns:

        - cost_type: either "capital_costs" or "annual_om_costs".
        - region
        - technology
        - value
        - unit

    """
    # Get WEO data
    df_weo = get_weo_data(DICT_TECH_ROWS, DICT_COST_COLS)

    # Get manual Eric data
    df_eric = get_cost_assumption_data()

    # Get comparison of original and WEO NAM costs
    df_nam_costs = compare_original_and_weo_nam_costs(
        df_weo, df_eric, DICT_WEO_TECH, DICT_WEO_R11
    )

    # Adjust NAM costs
    adj_nam_cost_conversion(df_nam_costs, conversion_2017_to_2005_usd)
    adj_nam_cost_message(
        df_nam_costs, DICT_TECH_SAME_ORIG_MESSAGE_INV, DICT_TECH_SAME_ORIG_MESSAGE_FOM
    )
    adj_nam_cost_manual(
        df_nam_costs, DICT_MANUAL_NAM_COSTS_INV, DICT_MANUAL_NAM_COSTS_FOM
    )
    adj_nam_cost_reference(df_nam_costs, DICT_TECH_REF_INV, DICT_TECH_REF_FOM)

    df_nam_adj_costs_only = df_nam_costs[
        ["message_technology", "weo_technology", "cost_type", "cost_NAM_adjusted"]
    ]

    # Assign fake WEO technology for stor_ppl and h2_elec so that dfs can be merged
    df_nam_adj_costs_only.loc[
        df_nam_adj_costs_only.message_technology.isin(["stor_ppl", "h2_elec"]),
        "weo_technology",
    ] = "marine"

    # Get ratios
    df_ratios = (
        calculate_region_cost_ratios(df_weo, DICT_WEO_R11)
        .rename(columns={"technology": "weo_technology"})
        .drop(columns={"scenario", "year"})
    )

    # Merge costs
    df_regiondiff = pd.merge(
        df_ratios, df_nam_adj_costs_only, on=["weo_technology", "cost_type"]
    )

    # For stor_ppl and h2_elec, make ratios = 1 (all regions have the same cost)
    df_regiondiff.loc[
        df_regiondiff.message_technology.isin(["stor_ppl", "h2_elec"]), "cost_ratio"
    ] = 1.0

    # Calculate region-specific costs
    df_regiondiff["cost_region"] = (
        df_regiondiff["cost_NAM_adjusted"] * df_regiondiff["cost_ratio"]
    )

    return df_regiondiff


df = get_region_differentiated_costs()
