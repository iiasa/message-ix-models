from itertools import product
from typing import Dict

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
DICT_COST_COLS = {"inv_cost": "A,B:D", "fix_cost": "A,F:H"}

# Dict of each R11 region matched with a WEO region
DICT_WEO_R11 = {
    "R11_AFR": "Africa",
    "R11_CPA": "China",
    "R11_EEU": "Russia",
    "R11_FSU": "Russia",
    "R11_LAM": "Brazil",
    "R11_MEA": "Middle East",
    "R11_NAM": "United States",
    "R11_PAO": "Japan",
    "R11_PAS": "India",
    "R11_SAS": "India",
    "R11_WEU": "European Union",
}

DICT_WEO_R12 = {
    "R12_AFR": "Africa",
    "R12_RCPA": "China",
    "R12_CHN": "China",
    "R12_EEU": "Russia",
    "R12_FSU": "Russia",
    "R12_LAM": "Brazil",
    "R12_MEA": "Middle East",
    "R12_NAM": "United States",
    "R12_PAO": "Japan",
    "R12_PAS": "India",
    "R12_SAS": "India",
    "R12_WEU": "European Union",
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
        "cost_type": "inv_cost",
    },
    "eth_bio": {"tech": "liq_bio", "cost_type": "inv_cost"},
    "eth_bio_ccs": {
        "tech": "eth_bio",
        "cost_type": "inv_cost",
    },
    "gas_ppl": {"tech": "gas_cc", "cost_type": "inv_cost"},
    "h2_bio": {"tech": "h2_coal", "cost_type": "inv_cost"},
    "h2_bio_ccs": {"tech": "h2_bio", "cost_type": "inv_cost"},
    "liq_bio_ccs": {
        "tech": "liq_bio",
        "cost_type": "inv_cost",
    },
    "meth_coal": {"tech": "syn_liq", "cost_type": "inv_cost"},
    "meth_coal_ccs": {
        "tech": "meth_coal",
        "cost_type": "inv_cost",
    },
    "meth_ng": {"tech": "syn_liq", "cost_type": "inv_cost"},
    "meth_ng_ccs": {
        "tech": "meth_ng",
        "cost_type": "inv_cost",
    },
    "solar_i": {
        "tech": "solar_pv_ppl",
        "cost_type": "inv_cost",
    },
    "solar_pv_I": {
        "tech": "solar_pv_ppl",
        "cost_type": "inv_cost",
    },
    "solar_pv_RC": {
        "tech": "solar_pv_ppl",
        "cost_type": "inv_cost",
    },
    "solar_th_ppl": {
        "tech": "solar_pv_ppl",
        "cost_type": "inv_cost",
    },
    "syn_liq_ccs": {
        "tech": "syn_liq",
        "cost_type": "inv_cost",
    },
}

# Dict of the technologies whose FO&M costs are in reference to other technologies.
# Within the key, the `tech` refers to the reference tech,
# and the `cost_type` refers to the reference cost type (either investment or FO&M cost)
DICT_TECH_REF_FOM = {
    "coal_ppl_u": {
        "tech": "coal_ppl",
        "cost_type": "fix_cost",
    },
    "eth_bio": {"tech": "liq_bio", "cost_type": "fix_cost"},
    "eth_bio_ccs": {
        "tech": "eth_bio",
        "cost_type": "fix_cost",
    },
    "gas_ppl": {"tech": "gas_cc", "cost_type": "fix_cost"},
    "h2_bio": {"tech": "h2_coal", "cost_type": "fix_cost"},
    "h2_bio_ccs": {
        "tech": "h2_bio",
        "cost_type": "fix_cost",
    },
    "liq_bio_ccs": {
        "tech": "liq_bio",
        "cost_type": "fix_cost",
    },
    "meth_coal": {
        "tech": "syn_liq",
        "cost_type": "fix_cost",
    },
    "meth_coal_ccs": {
        "tech": "meth_coal",
        "cost_type": "fix_cost",
    },
    "meth_ng": {"tech": "syn_liq", "cost_type": "fix_cost"},
    "meth_ng_ccs": {
        "tech": "meth_ng",
        "cost_type": "fix_cost",
    },
    "solar_i": {
        "tech": "solar_pv_ppl",
        "cost_type": "fix_cost",
    },
    "solar_pv_I": {
        "tech": "solar_pv_ppl",
        "cost_type": "fix_cost",
    },
    "solar_pv_RC": {
        "tech": "solar_pv_ppl",
        "cost_type": "fix_cost",
    },
    "solar_th_ppl": {
        "tech": "solar_pv_ppl",
        "cost_type": "fix_cost",
    },
    "syn_liq_ccs": {
        "tech": "syn_liq",
        "cost_type": "fix_cost",
    },
}


def get_weo_data() -> pd.DataFrame:
    """Read in raw WEO investment/capital costs and O&M costs data.

    Data are read for all technologies and for STEPS scenario only from the file
    :file:`data/iea/WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - technology: WEO technologies, with shorthands as defined in `DICT_WEO_TECH`
        - region: WEO regions
        - year: values from 2021 to 2050, as appearing in the file
        - cost type: either “inv_cost” or “fix_cost”
        - units: "usd_per_kw"
        - value: the cost value
    """

    dict_rows = DICT_TECH_ROWS
    dict_cols = DICT_COST_COLS

    # Read in raw data file
    file_path = package_data_path(
        "iea", "WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb"
    )

    # Loop through each technology and cost type
    # Read in data and convert to long format
    dfs_cost = []
    for tech_key, cost_key in product(dict_rows, dict_cols):
        df = (
            pd.read_excel(
                file_path,
                sheet_name=dict_rows[tech_key][0],
                header=None,
                skiprows=dict_rows[tech_key][1],
                nrows=9,
                usecols=dict_cols[cost_key],
            )
            .set_axis(["region", "2021", "2030", "2050"], axis=1)
            .melt(id_vars=["region"], var_name="year", value_name="value")
            .assign(
                technology=tech_key,
                cost_type=cost_key,
                units="usd_per_kw",
            )
            .reindex(
                [
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

    # nonull_df = all_cost_df.loc[
    #     ~all_cost_df.value.isnull()
    # ]  # filter out NaN cost values

    return all_cost_df


def calculate_region_cost_ratios(
    weo_df: pd.DataFrame, sel_node: str = "r12"
) -> pd.DataFrame:
    """Calculate regional cost ratios (relative to NAM) using the WEO data

    Some assumptions are made as well:
        - For CSP in EEU and FSU, make cost ratio == 0.
        - For CSP in PAO, assume the same as NAM region (cost ratio == 1).
        - For pulverized coal with CCS and IGCC with CCS in MEA, \
          make cost ratio the same as in the FSU region.

    Parameters
    ----------
    weo_df : pandas.DataFrame
        Created using :func:`.get_weo_data`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - technology: WEO technologies, with shorthands as defined in `DICT_WEO_TECH`
        - region: MESSAGE R11 regions
        - weo_region: the WEO region corresponding to the R11 region, \
            as mapped in `DICT_WEO_R11`
        - year: the latest year of data, in this case 2021
        - cost_type: either “inv_cost” or “fix_cost”
        - cost_ratio: value between 0-1; \
          the cost ratio of each technology-region's cost \
          relative to the NAM region's cost

    """

    if sel_node.upper() == "R11":
        dict_regions = DICT_WEO_R11
    else:
        dict_regions = DICT_WEO_R12

    df = (
        weo_df.loc[weo_df.region == "United States"]
        .copy()
        .rename(columns={"value": "us_value"})
        .drop(columns={"region", "units"})
        .merge(weo_df, on=["technology", "year", "cost_type"])
        .assign(cost_ratio=lambda x: x.value / x.us_value)
    )

    l_cost_ratio = []
    for m, w in dict_regions.items():
        df_sel = (
            df.loc[(df.year == min(df.year)) & (df.region == w)]
            .copy()
            .rename(columns={"region": "weo_region"})
            .assign(region=m)
            .reindex(
                [
                    "technology",
                    "region",
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
        & (df_cost_ratio.region.isin(["R11_EEU", "R11_FSU", "R12_EEU", "R12_FSU"])),
        "cost_ratio",
    ] = 0

    # Assumption 2: For CSP in PAO, assume the same as NAM region (cost ratio == 1)
    df_cost_ratio.loc[
        (df_cost_ratio.technology == "csp")
        & (df_cost_ratio.region.isin(["R11_PAO", "R12_PAO"])),
        "cost_ratio",
    ] = 1

    # Assumption 3: For pulverized coal with CCS and IGCC with CCS in MEA,
    # make cost ratio the same as in the FSU region
    sub_mea = df_cost_ratio[
        (df_cost_ratio.cost_ratio.isnull())
        & (df_cost_ratio.region.isin(["R11_MEA", "R12_MEA"]))
    ].drop(columns={"cost_ratio"})

    sub_fsu = df_cost_ratio.loc[
        (df_cost_ratio.region.isin(["R11_FSU", "R12_FSU"]))
        & (df_cost_ratio.technology.isin(["pulverized_coal_ccs", "igcc_ccs"]))
    ].drop(columns={"weo_region", "region"})

    sub_merge_mea = sub_mea.merge(sub_fsu, on=["technology", "year", "cost_type"])

    # Asusumption 4: for all missing LAM data (ratios), replace with AFR data (ratios)
    sub_lam = df_cost_ratio.loc[
        (df_cost_ratio.cost_ratio.isnull()) & (df_cost_ratio.region.str.contains("LAM"))
    ].drop(columns={"cost_ratio"})

    sub_afr = df_cost_ratio.loc[
        (df_cost_ratio.region.str.contains("AFR"))
        & (df_cost_ratio.technology.isin(sub_lam.technology.unique()))
    ].drop(columns={"weo_region", "region"})

    sub_merge_lam = sub_lam.merge(sub_afr, on=["technology", "year", "cost_type"])

    # Create completed dataframe
    df_cost_ratio_fix = (
        pd.concat(
            [
                df_cost_ratio[
                    ~(
                        (df_cost_ratio.cost_ratio.isnull())
                        & (
                            (df_cost_ratio.region.str.contains("MEA"))
                            | (df_cost_ratio.region.str.contains("LAM"))
                        )
                    )
                ],
                sub_merge_mea,
                sub_merge_lam,
            ]
        )
        .reset_index(drop=1)
        .rename(columns={"technology": "weo_technology"})
        .drop(columns={"year"})
    )

    return df_cost_ratio_fix


def get_cost_assumption_data() -> pd.DataFrame:
    """Read in raw data on investment and fixed O&M costs in NAM region
    from older MESSAGE data.

    Data for investment costs and fixed O&M costs are read from the files
    :file:`data/costs/investment_costs-0.csv` and
    :file:`data/costs/fixed_om_costs-0.csv`, respectively.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: technologies included in MESSAGE
        - cost_type: either “inv_cost” or “fix_cost”
        - cost_NAM_original_message: costs for each technology given \
            in units of USD per kW
    """
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
        .assign(cost_type="inv_cost")
    )

    df_fom = (
        pd.read_csv(fom_file_path, header=9)
        .rename(columns={"fom_cost_nam_original_message": "cost_NAM_original_message"})
        .assign(cost_type="fix_cost")
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
    weo_df: pd.DataFrame,
    orig_message_df: pd.DataFrame,
    dict_weo_tech: Dict[str, str],
    dict_weo_regions: Dict[str, str],
) -> pd.DataFrame:
    """Compare NAM costs in older MESSAGE data with NAM costs in WEO data

    Merges the two NAM costs sources together.

    The function only keeps the latest year from the WEO.

    Parameters
    ----------
    weo_df : pandas.DataFrame
        Output of :func:`.get_weo_data`.
    orig_message_df : pandas.DataFrame
        Output of :func:`.get_cost_assumption_data`.
    dict_weo_tech : str -> tuple of (str, str)
        Keys are MESSAGE technologies
        Values are WEO technologies.
    dict_weo_regions : str -> tuple of (str, str)
        Keys are MESSAGE R11 regions.
        Values are WEO region assigned to each R11 region.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology:
        - weo_technology: WEO technologies, with shorthands \
        as defined in `DICT_WEO_TECH`
        - region: MESSAGE R11 regions
        - cost_type: either “inv_cost” or “fix_cost”
        - cost_NAM_original_message: costs for each technology from old MESSAGE data \
            given in units of USD per kW
        - cost_NAM_weo_2021: costs for each technology from 2021 WEO given in \
            units of USD per kW

    """

    df_assumptions = (
        orig_message_df.copy()
        .assign(technology=lambda x: x.message_technology.map(dict_weo_tech))
        .merge(
            weo_df.loc[
                (weo_df.region == dict_weo_regions["R11_NAM"])
                & (weo_df.year == min(weo_df.year))
            ].copy(),
            on=["technology", "cost_type"],
            how="left",
        )
        .drop(columns={"year", "region", "units"})
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


def adj_nam_cost_conversion(df_costs: pd.DataFrame, conv_rate: float):
    """Convert NAM technology costs from 2017 USD to 2005 USD

    Adjust values in-place

    Parameters
    ----------
    df_costs : pandas.DataFrame
        Output of `compare_original_and_weo_nam_costs`
    conv_rate : float
        Conversion rate from 2017 USD to 2006 USD
    """

    df_costs["cost_NAM_adjusted"] = df_costs["cost_NAM_weo_2021"] * conv_rate


def adj_nam_cost_message(
    df_costs: pd.DataFrame, list_tech_inv: list, list_tech_fom: list
):
    """Set specified technologies to have same NAM costs as older MESSAGE data

    Adjust values in place

    Parameters
    ----------
    df_costs : pandas.DataFrame
        Output of `compare_original_and_weo_nam_costs`
    list_tech_inv :
        List of technologies whose investment costs should be
        set to be the same as in older MESSAGE data
    list_tech_fom:
        List of technologies whose fixed O&M costs should be
        set to be the same as in older MESSAGE data

    """
    mask = (df_costs.message_technology.isin(list_tech_inv)) & (
        df_costs.cost_type == "inv_cost"
    )
    df_costs.loc[mask, "cost_NAM_adjusted"] = df_costs.loc[
        mask, "cost_NAM_original_message"
    ]

    df_costs.loc[
        (df_costs.message_technology.isin(list_tech_fom))
        & (df_costs.cost_type == "fix_cost"),
        "cost_NAM_adjusted",
    ] = df_costs.loc[
        (df_costs.message_technology.isin(list_tech_fom))
        & (df_costs.cost_type == "fix_cost"),
        "cost_NAM_original_message",
    ]


def adj_nam_cost_manual(
    df_costs: pd.DataFrame,
    dict_manual_inv: Dict[str, int],
    dict_manual_fom: Dict[str, int],
):
    """Assign manually-specified technology cost values to certain technologies

    Adjust values in place

    Parameters
    ----------
    df_costs : pandas.DataFrame
        Output of :func:`.compare_original_and_weo_nam_costs`
    dict_manual_inv : str -> tuple of (str, int)
        Keys are the MESSAGE technologies whose investment costs in NAM region
        should be manually set. Values are investment costs in units of USD per kW.
    dict_manual_fom: str -> tuple of (str, int)
        Keys are the MESSAGE technologies whose fixed O&M costs in NAM region
        should be manually set. Values are investment costs in units of USD per kW.
    """
    for k in dict_manual_inv:
        df_costs.loc[
            (df_costs.message_technology == k) & (df_costs.cost_type == "inv_cost"),
            "cost_NAM_adjusted",
        ] = dict_manual_inv[k]

    for f in dict_manual_fom:
        df_costs.loc[
            (df_costs.message_technology == f) & (df_costs.cost_type == "fix_cost"),
            "cost_NAM_adjusted",
        ] = dict_manual_fom[f]


def calc_nam_cost_ratio(
    df_costs: pd.DataFrame,
    desired_tech: str,
    desired_cost_type: str,
    reference_tech: str,
    reference_cost_type: str,
):
    """Calculate the cost of a desired technology based on a reference technology

    This function calculates the ratio of investment or fixed O&M costs
    (from older MESSAGE data) and uses this ratio to calculate an adjusted cost for
    a desired technology.

    Parameters
    ----------
    df_costs : pandas.DataFrame
        Output of `compare_original_and_weo_nam_costs`
    desired_tech : str
        The MESSAGE technology whose costs need to be adjusted.
    desired_cost_type: str
        The cost type of the MESSAGE technology that is being changed.
    desired_tech : str
        The reference technology whose cost the desired technology is based off of.
    desired_cost_type: str
        The cost type of the reference technology that should be used
        for the calculation.

    """

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


def adj_nam_cost_reference(
    df_costs: pd.DataFrame,
    dict_reference_inv: Dict,
    dict_reference_fom: Dict,
):
    """Assign technology costs for using other technologies as references

    The function :func:`.calc_nam_cost_ratio` is used to calculate the adjusted cost,
    based on provided reference technology and cost type.

    Since some technologies are similar to others, this function modifies the costs
    of some technologies to be based off the costs other technologies. In a few cases,
    the fixed O&M costs of a technology is based on the investment cost of
    another technology, hence why the reference cost type is also specified.

    Adjust values in place

    Parameters
    ----------
    df_costs : pandas.DataFrame
        Output of `compare_original_and_weo_nam_costs`
    dict_reference_inv : str
        Keys are the MESSAGE technologies whose investment costs in NAM region
        should be changed. Values describe the reference technology and the
        reference cost type that should be used for the calculation..
    dict_reference_fom: str
        Keys are the MESSAGE technologies whose fixed O&M costs in NAM region
        should be changed. Values describe the reference technology and the
        reference cost type that should be used for the calculation.
    """
    for m in dict_reference_inv:
        calc_nam_cost_ratio(
            df_costs,
            m,
            "inv_cost",
            dict_reference_inv[m]["tech"],
            dict_reference_inv[m]["cost_type"],
        )

    for n in dict_reference_fom:
        calc_nam_cost_ratio(
            df_costs,
            n,
            "fix_cost",
            dict_reference_fom[n]["tech"],
            dict_reference_fom[n]["cost_type"],
        )


def get_region_differentiated_costs(
    df_weo, df_orig_message, df_cost_ratios
) -> pd.DataFrame:
    """Perform all calculations needed to get regionally-differentiated costs.

    The algorithm is roughly:

    1. Retrieve data with :func:`.get_weo_data` and assumptions with
       :func:`.get_cost_assumption_data`.
    2. Adjust costs for the NAM region with reference to older MESSAGE data.
    3. Compute cost ratios across regions, relative to ``*_NAM``, based on (1).
    4. Apply the ratios from (3) to the adjusted data (2).

    Parameters
    ----------
    df_weo : pandas.DataFrame
        Output of `get_weo_data`
    df_orig_message : pandas.DataFrame
        Output of `get_cost_assumption_data`
    df_cost_ratios : pandas.DataFrame
        Output of `calculate_region_cost_ratios`

    Returns
    -------
    pandas.DataFrame
        with columns:

        - cost_type: either "inv_cost" or "fix_cost".
        - region
        - technology
        - value
        - unit

    """
    # Get comparison of original and WEO NAM costs
    df_nam_costs = compare_original_and_weo_nam_costs(
        df_weo, df_orig_message, DICT_WEO_TECH, DICT_WEO_R11
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

    # Merge costs
    df_regiondiff = pd.merge(
        df_cost_ratios, df_nam_adj_costs_only, on=["weo_technology", "cost_type"]
    )

    # For stor_ppl and h2_elec, make ratios = 1 (all regions have the same cost)
    df_regiondiff.loc[
        df_regiondiff.message_technology.isin(["stor_ppl", "h2_elec"]), "cost_ratio"
    ] = 1.0

    # Calculate region-specific costs
    df_regiondiff["cost_region_2021"] = (
        df_regiondiff["cost_NAM_adjusted"] * df_regiondiff["cost_ratio"]
    )

    return df_regiondiff


def calculate_fom_to_inv_cost_ratios(input_df_weo):
    df_inv = (
        input_df_weo.loc[
            (input_df_weo.cost_type == "inv_cost")
            & (input_df_weo.year == min(input_df_weo.year))
        ]
        .rename(columns={"value": "inv_cost"})
        .drop(columns=["year", "cost_type", "units"])
    )

    df_fom = (
        input_df_weo.loc[
            (input_df_weo.cost_type == "fix_cost")
            & (input_df_weo.year == min(input_df_weo.year))
        ]
        .rename(columns={"value": "fom_cost"})
        .drop(columns=["year", "cost_type", "units"])
    )

    df_ratio = (
        df_inv.merge(df_fom, on=["technology", "region"])
        .assign(fom_to_inv_cost_ratio=lambda x: x.fom_cost / x.inv_cost)
        .drop(columns=["inv_cost", "fom_cost"])
    )

    msg_tech = list(DICT_WEO_TECH.keys())
    r11_reg = list(DICT_WEO_R11.keys())

    tech_reg = (
        pd.DataFrame(
            list(product(msg_tech, r11_reg)),
            columns=["message_technology", "region"],
        )
        .assign(technology=lambda x: x.message_technology.map(DICT_WEO_TECH))
        .assign(region=lambda x: x.region.map(DICT_WEO_R11))
        .merge(df_ratio, on=["technology", "region"])
        .drop(columns=["technology", "region"])
    )

    return tech_reg
