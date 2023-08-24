from itertools import product

import numpy as np
import pandas as pd

from message_ix_models.tools.costs.config import CONVERSION_2021_TO_2005_USD
from message_ix_models.util import package_data_path

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

DICT_WEO_R20 = {
    "R20_AFR": "Africa",
    "R20_CHN": "China",
    "R20_PRK": "Russia",
    "R20_MNG": "Russia",
    "R20_MSA": "India",
    "R20_JPN": "Japan",
    "R20_AUNZ": "Japan",
    "R20_KOR": "China",
    "R20_SEA": "India",
    "R20_RUBY": "Russia",
    "R20_UMBA": "Russia",
    "R20_CAS": "Russia",
    "R20_SCST": "European Union",
    "R20_EEU27": "European Union",
    "R20_LAM": "Brazil",
    "R20_MEA": "Middle East",
    "R20_NAM": "United States",
    "R20_SAS": "India",
    "R20_WEU27": "European Union",
    "R20_UKEFT": "European Union",
}


# Function to read in raw IEA WEO data
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

    # Set file path for raw IEA WEO cost data
    file_path = package_data_path(
        "iea", "WEO_2022_PG_Assumptions_STEPSandNZE_Scenario.xlsb"
    )

    # Loop through Excel sheets to read in data and process:
    # - Convert to long format
    # - Only keep investment costs
    # - Replace "n.a." with NaN
    # - Convert units from 2021 USD to 2005 USD
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
            .set_axis(["weo_region", "2021", "2030", "2050"], axis=1)
            .melt(id_vars=["weo_region"], var_name="year", value_name="value")
            .assign(
                weo_technology=tech_key,
                cost_type=cost_key,
                units="usd_per_kw",
            )
            .reindex(
                [
                    "cost_type",
                    "weo_technology",
                    "weo_region",
                    "year",
                    "units",
                    "value",
                ],
                axis=1,
            )
            .replace({"value": "n.a."}, np.nan)
            .assign(value=lambda x: x.value * CONVERSION_2021_TO_2005_USD)
        )

        dfs_cost.append(df)

    all_cost_df = pd.concat(dfs_cost)

    # Substitute NaN values
    # If value is missing, then replace with median across regions for that technology

    # Calculate median values for each technology
    df_median = (
        all_cost_df.groupby(["weo_technology"])
        .agg(median_value=("value", "median"))
        .reset_index()
    )

    # Merge full dataframe with median dataframe
    # Replace null values with median values
    df_merged = (
        all_cost_df.merge(df_median, on=["weo_technology"], how="left")
        .assign(adj_value=lambda x: np.where(x.value.isnull(), x.median_value, x.value))
        .drop(columns={"value", "median_value"})
        .rename(columns={"adj_value": "value"})
    )

    return df_merged


# Function to read in technology mapping file
def get_technology_mapping() -> pd.DataFrame:
    """Read in technology mapping file

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - map_source: data source to map MESSAGEix technology to (e.g., WEO)
        - map_technology: technology name in the data source
        - base_year_reference_region_cost: manually specified base year cost of the \
            technology in the reference region (in 2005 USD)
    """

    file_path = package_data_path("costs", "technology_weo_map.csv")
    df_tech_map = pd.read_csv(file_path)

    return df_tech_map


# Function to get WEO-based regional differentiation
def get_weo_region_differentiated_costs(
    input_node,
    input_ref_region,
    input_base_year,
) -> pd.DataFrame:
    """Calculate regionally differentiated costs and fixed-to-investment cost ratios

    Parameters
    ----------
    input_node : str, optional
        MESSAGEix node, by default "r12"
    input_ref_region : str, optional
        Reference region, by default "r12_nam"
    input_base_year : int, optional
        Base year, by default BASE_YEAR

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - reg_cost_ratio: regional cost ratio relative to reference region
        - reg_cost_base_year: regional cost in base year
        - fix_to_inv_cost_ratio: fixed-to-investment cost ratio
    """

    # Set default values for input arguments
    # If specified node is R11, then use R11_NAM as the reference region
    # If specified node is R12, then use R12_NAM as the reference region
    # If specified node is R20, then use R20_NAM as the reference region
    # However, if a reference region is specified, then use that instead
    if input_ref_region is None:
        if input_node.upper() == "R11":
            input_ref_region = "R11_NAM"
        if input_node.upper() == "R12":
            input_ref_region = "R12_NAM"
        if input_node.upper() == "R20":
            input_ref_region = "R20_NAM"
    else:
        input_ref_region = input_ref_region

    if input_node.upper() == "R11":
        dict_regions = DICT_WEO_R11
    if input_node.upper() == "R12":
        dict_regions = DICT_WEO_R12
    if input_node.upper() == "R20":
        dict_regions = DICT_WEO_R20

    # Grab WEO data and keep only investment costs
    df_weo = get_weo_data()

    # Grab technology mapping data
    df_tech_map = get_technology_mapping()

    # If base year does not exist in WEO data, then use earliest year and give warning
    base_year = str(input_base_year)
    if base_year not in df_weo.year.unique():
        base_year = str(min(df_weo.year.unique()))
        print(
            f"Base year {input_base_year} not found in WEO data. \
                Using {base_year} instead."
        )

    # Map WEO data to MESSAGEix regions
    # Keep only base year data
    l_sel_weo = []
    for m, w in dict_regions.items():
        df_sel = (
            df_weo.query("year == @base_year & weo_region == @w")
            .assign(region=m)
            .rename(columns={"value": "weo_cost"})
            .reindex(
                [
                    "cost_type",
                    "weo_technology",
                    "weo_region",
                    "region",
                    "year",
                    "weo_cost",
                ],
                axis=1,
            )
        )

        l_sel_weo.append(df_sel)

    df_sel_weo = pd.concat(l_sel_weo)

    # If specified reference region is not in WEO data, then give error
    ref_region = input_ref_region.upper()
    if ref_region not in df_sel_weo.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. \
                Please specify a different reference region. \
                    Available regions are: {df_sel_weo.region.unique()}"
        )

    # Calculate regional investment cost ratio relative to reference region
    df_reg_ratios = (
        df_sel_weo.query("region == @ref_region and cost_type == 'inv_cost'")
        .rename(columns={"weo_cost": "weo_ref_cost"})
        .drop(columns={"weo_region", "region"})
        .merge(
            df_sel_weo.query("cost_type == 'inv_cost'"), on=["weo_technology", "year"]
        )
        .assign(reg_cost_ratio=lambda x: x.weo_cost / x.weo_ref_cost)
        .reindex(
            [
                "region",
                "weo_region",
                "weo_technology",
                "year",
                "weo_cost",
                "weo_ref_cost",
                "reg_cost_ratio",
            ],
            axis=1,
        )
    )

    # Calculate fixed O&M cost ratio relative to investment cost
    # Get investment costs
    df_inv = (
        df_sel_weo.query("cost_type == 'inv_cost' and year == @base_year")
        .rename(columns={"weo_cost": "inv_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Get fixed O&M costs
    df_fix = (
        df_sel_weo.query("cost_type == 'fix_cost' and year == @base_year")
        .rename(columns={"weo_cost": "fix_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Merge investment and fixed O&M costs
    # Calculate ratio of fixed O&M costs to investment costs
    df_fom_inv = (
        df_inv.merge(df_fix, on=["weo_technology", "weo_region", "region"])
        .assign(fix_to_inv_cost_ratio=lambda x: x.fix_cost / x.inv_cost)
        .drop(columns=["inv_cost", "fix_cost"])
    )

    # Combine cost ratios (regional and fix-to-investment) together
    df_cost_ratios = df_reg_ratios.merge(
        df_fom_inv, on=["weo_technology", "weo_region", "region"]
    )

    # Merge WEO costs and cost ratio data with technology mapping data
    # If no base year cost in reference region is specified, then use the WEO cost
    # Calculate regional costs using base year reference region cost and cost ratios
    df_reg_diff = (
        df_tech_map.merge(
            df_cost_ratios,
            left_on="map_technology",
            right_on="weo_technology",
            how="left",
        )
        .assign(
            base_year_reference_region_cost_final=lambda x: np.where(
                x.base_year_reference_region_cost.isnull(),
                x.weo_ref_cost,  # WEO cost in reference region
                x.base_year_reference_region_cost,  # specified base year cost
            ),
            reg_cost_base_year=lambda x: x.base_year_reference_region_cost_final
            * x.reg_cost_ratio,
        )
        .reindex(
            [
                "message_technology",
                "region",
                "reg_cost_ratio",
                "reg_cost_base_year",
                "fix_to_inv_cost_ratio",
            ],
            axis=1,
        )
    )

    return df_reg_diff
