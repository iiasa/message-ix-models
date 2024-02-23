from itertools import product

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

from .config import BASE_YEAR, CONVERSION_2021_TO_2005_USD

# Dict of each R11 region matched with a WEO region
DICT_WEO_R11 = {
    "R11_AFR": "Africa",
    "R11_CPA": "China",
    "R11_EEU": "European Union",
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
    "R12_EEU": "European Union",
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

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - cost_type: investment or fixed O&M cost
        - weo_technology: WEO technology name
        - weo_region: WEO region
        - year: year
        - value: cost value
    """

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
    for tech_key, cost_key in product(DICT_TECH_ROWS, DICT_COST_COLS):
        df = (
            pd.read_excel(
                file_path,
                sheet_name=DICT_TECH_ROWS[tech_key][0],
                header=None,
                skiprows=DICT_TECH_ROWS[tech_key][1],
                nrows=9,
                usecols=DICT_COST_COLS[cost_key],
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
    # If value is missing, then replace with median across regions for that
    # technology

    # Calculate median values for each technology
    df_median = (
        all_cost_df.groupby(["weo_technology", "cost_type"])
        .agg(median_value=("value", "median"))
        .reset_index()
    )

    # Merge full dataframe with median dataframe
    # Replace null values with median values
    df_merged = (
        all_cost_df.merge(df_median, on=["weo_technology", "cost_type"], how="left")
        .assign(adj_value=lambda x: np.where(x.value.isnull(), x.median_value, x.value))
        .drop(columns={"value", "median_value"})
        .rename(columns={"adj_value": "value"})
    )

    return df_merged


# Function to read in intratec data
def get_intratec_data() -> pd.DataFrame:
    """Read in raw Intratec data

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - intratec_tech: Intratec technology name
        - intratec_region: Intratec region
        - intratec_index: Intratec index value
    """

    # Set file path for raw Intratec data
    file = package_data_path("intratec", "intratec_data.xlsx")

    # Read in data
    df = (
        pd.read_excel(file, sheet_name="comparison", header=0)
        .rename(columns={"Unnamed: 0": "type"})
        .query("type.notnull()")
    )

    # Convert to long format
    df_long = (
        (
            df.melt(
                id_vars=["type"], var_name="region", value_name="value"
            ).reset_index(drop=True)
        )
        .query("type == 'Intratec index'")
        .rename(columns={"value": "intratec_index", "region": "intratec_region"})
        .assign(intratec_tech="all")
        .drop(columns={"type"})
        .reset_index(drop=True)
    )

    return df_long


# Function get raw technology mapping
def get_raw_technology_mapping(module) -> pd.DataFrame:
    """Create technology mapping for each module

    Parameters
    ----------
    module : str
        Model module

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - reg_diff_source: data source to map MESSAGEix technology to (e.g., WEO)
        - reg_diff_technology: technology name in the data source
        - base_year_reference_region_cost: manually specified base year cost
        of the technology in the reference region (in 2005 USD)
    """

    if module == "energy":
        energy_file = package_data_path("costs", "energy", "tech_map_energy.csv")
        raw_map_energy = pd.read_csv(energy_file, skiprows=2)

        return raw_map_energy

    elif module == "materials":
        materials_file = package_data_path(
            "costs", "materials", "tech_map_materials.csv"
        )
        raw_map_materials = pd.read_csv(materials_file)

        return raw_map_materials


# Function to subset materials mapping for only
# technologies that have sufficient data
def subset_materials_map(raw_map):
    """Subset materials mapping for only technologies that have sufficient data

    Parameters
    ----------
    raw_map : pandas.DataFrame
        Output of :func:`get_raw_technology_mapping`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - reg_diff_source: data source to map MESSAGEix technology to (e.g., WEO)
        - reg_diff_technology: technology name in the data source
        - base_year_reference_region_cost: manually specified base year cost
        of the technology in the reference region (in 2005 USD)

    """
    # - Remove materials technologies that are missing both a reg_diff_source and a
    # base_year_reference_region_cost
    # - Round base_year_reference_region_cost to nearest integer
    sub_map = (
        raw_map.query(
            "reg_diff_source.notnull() or base_year_reference_region_cost.notnull()"
        )
        .rename(columns={"base_year_reference_region_cost": "base_cost"})
        .assign(base_year_reference_region_cost=lambda x: x.base_cost.round())
        .drop(columns={"base_cost"})
    )

    return sub_map


# Function to adjust technology mapping
def adjust_technology_mapping(module) -> pd.DataFrame:
    """Adjust technology mapping based on sources and assumptions

    Parameters
    ----------
    module : str
        Model module

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - reg_diff_source: data source to map MESSAGEix technology to \
            (e.g., WEO, Intratec)
        - reg_diff_technology: technology name in the data source
        - base_year_reference_region_cost: manually specified base year cost
        of the technology in the reference region (in 2005 USD)
    """

    if module == "energy":
        raw_map_energy = get_raw_technology_mapping("energy")
        return raw_map_energy

    elif module == "materials":
        raw_map_energy = get_raw_technology_mapping("energy")
        raw_map_materials = get_raw_technology_mapping("materials")
        sub_map_materials = subset_materials_map(raw_map_materials)

        # If message_technology in sub_map_materials is in raw_map_energy
        # and base_year_reference_region_cost is not null/empty,
        # then replace base_year_reference_region_cost in raw_map_energy
        # with base_year_reference_region_cost in sub_map_materials
        materials_replace = (
            sub_map_materials.query(
                "message_technology in @raw_map_energy.message_technology"
            )
            .rename(
                columns={
                    "message_technology": "material_message_technology",
                    "base_year_reference_region_cost": "material_base_cost",
                }
            )
            .drop(columns=["reg_diff_source", "reg_diff_technology"])
            .merge(
                raw_map_energy,
                how="right",
                left_on="material_message_technology",
                right_on="message_technology",
            )
            .assign(
                base_year_reference_region_cost=lambda x: np.where(
                    x.material_base_cost.notnull(),
                    x.material_base_cost,
                    x.base_year_reference_region_cost,
                )
            )
            .reindex(
                [
                    "message_technology",
                    "reg_diff_source",
                    "reg_diff_technology",
                    "base_year_reference_region_cost",
                ],
                axis=1,
            )
        )

        # Subset to only rows where reg_diff_source is "energy"
        # Merge with raw_map_energy on reg_diff_technology
        # If the "base_year_reference_region_cost" is not
        # null/empty in raw_materials_map,
        # then use that.
        # If the base_year_reference_region_cost is null/empty in raw_materials_map,
        # then use the base_year_reference_region_cost from the mapped energy technology
        materials_map_energy = (
            sub_map_materials.query("reg_diff_source == 'energy'")
            .drop(columns=["reg_diff_source"])
            .rename(
                columns={
                    "reg_diff_technology": "reg_diff_technology_energy",
                    "base_year_reference_region_cost": "material_base_cost",
                }
            )
            .merge(
                raw_map_energy.rename(
                    columns={
                        "message_technology": "message_technology_base",
                    }
                ),
                left_on="reg_diff_technology_energy",
                right_on="message_technology_base",
                how="left",
            )
            .assign(
                base_year_reference_region_cost=lambda x: np.where(
                    x.material_base_cost.isnull(),
                    x.base_year_reference_region_cost,
                    x.material_base_cost,
                )
            )
            .reindex(
                [
                    "message_technology",
                    "reg_diff_source",
                    "reg_diff_technology",
                    "base_year_reference_region_cost",
                ],
                axis=1,
            )
        )

        # Get technologies that are mapped to Intratec AND have a base year cost
        # Assign map_techonology as "all"
        materials_map_intratec = sub_map_materials.query(
            "reg_diff_source == 'intratec' and \
                base_year_reference_region_cost.notnull()"
        ).assign(reg_diff_technology="all")

        # Get technologies that don't have a map source but do have a base year cost
        # For these technologies, assume no regional differentiation
        # So use the reference region base year cost as the base year cost
        # across all regions
        materials_map_noregdiff = sub_map_materials.query(
            "reg_diff_source.isnull() and base_year_reference_region_cost.notnull()"
        )

        # Concatenate materials_replace and materials_map_energy
        # Drop duplicates
        materials_all = (
            pd.concat(
                [
                    materials_replace,
                    materials_map_energy,
                    materials_map_intratec,
                    materials_map_noregdiff,
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Get list of technologies in raw_map_materials that are not in materials_all
        missing_tech = raw_map_materials.query(
            "message_technology not in @materials_all.message_technology"
        ).message_technology.unique()

        print(
            "The following technologies are not projected due to insufficient data:"
            + "\n"
            + "\n".join(missing_tech)
        )

        return materials_all


# Function to get WEO regional differentiation
# Inputs: node, ref_region
# The function should take the WEO data, map it to MESSAGEix regions
# using the node and ref_region,
# and then calculate cost ratios for each region relative to the reference region
def get_weo_regional_differentiation(node, ref_region) -> pd.DataFrame:
    """Apply WEO regional differentiation

    Parameters
    ----------
    node : str
        MESSAGEix node
    ref_region : str
        Reference region

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - weo_ref_region_cost: WEO cost in reference region
        - reg_cost_ratio: regional cost ratio relative to reference region
    """

    if node.upper() == "R11":
        dict_regions = DICT_WEO_R11
    if node.upper() == "R12":
        dict_regions = DICT_WEO_R12
    if node.upper() == "R20":
        dict_regions = DICT_WEO_R20

    # Grab WEO data and keep only investment costs
    df_weo = get_weo_data()

    # Get list of years in WEO data and select year closest to base year
    l_years = df_weo.year.unique()
    sel_year = min(l_years, key=lambda x: abs(int(x) - BASE_YEAR))
    print("......(Using year " + str(sel_year) + " data from WEO.)")

    # Map WEO data to MESSAGEix regions
    # Keep only base year data
    l_sel_weo = []
    for m, w in dict_regions.items():
        df_sel = (
            df_weo.query("year == @sel_year & weo_region == @w")
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
    ref_region = ref_region.upper()
    if ref_region not in df_sel_weo.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. \
                Please specify a different reference region. \
                    Available regions are: {df_sel_weo.region.unique()}"
        )

    # Calculate regional investment cost ratio relative to reference region
    df_reg_ratios = (
        df_sel_weo.query("region == @ref_region and cost_type == 'inv_cost'")
        .rename(columns={"weo_cost": "weo_ref_region_cost"})
        .drop(columns={"weo_region", "region"})
        .merge(
            df_sel_weo.query("cost_type == 'inv_cost'"), on=["weo_technology", "year"]
        )
        .assign(reg_cost_ratio=lambda x: x.weo_cost / x.weo_ref_region_cost)
        .reindex(
            [
                "weo_technology",
                "region",
                "weo_ref_region_cost",
                "reg_cost_ratio",
            ],
            axis=1,
        )
    )

    # Calculate fixed O&M cost ratio relative to investment cost
    # Get investment costs
    df_inv = (
        df_sel_weo.query("cost_type == 'inv_cost' and year == @sel_year")
        .rename(columns={"weo_cost": "inv_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Get fixed O&M costs
    df_fix = (
        df_sel_weo.query("cost_type == 'fix_cost' and year == @sel_year")
        .rename(columns={"weo_cost": "fix_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Merge investment and fixed O&M costs
    # Calculate ratio of fixed O&M costs to investment costs
    df_fom_inv = (
        df_inv.merge(df_fix, on=["weo_technology", "weo_region", "region"])
        .assign(weo_fix_ratio=lambda x: x.fix_cost / x.inv_cost)
        .drop(columns=["inv_cost", "fix_cost", "weo_region"])
    )

    # Combine cost ratios (regional and fix-to-investment) together
    df_cost_ratios = df_reg_ratios.merge(df_fom_inv, on=["weo_technology", "region"])

    return df_cost_ratios


# Function to get Intratec regional differentiation
# Inputs: node, ref_region
# The function should take the Intratec data, map it to MESSAGEix regions using
# the node and ref_region,
# and then calculate cost ratios for each region relative to the reference region
def get_intratec_regional_differentiation(node, ref_region) -> pd.DataFrame:
    """Apply Intratec regional differentiation

    Parameters
    ----------
    node : str
        MESSAGEix node
    ref_region : str
        Reference region

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - intratec_ref_region_cost: Intratec cost in reference region
        - reg_cost_ratio: regional cost ratio relative to reference region
    """

    df_intratec = get_intratec_data()

    # Map Intratec regions to MESSAGEix regions
    # If node == "R11", add "R11_" to the beginning of each region
    # If node == "R11", then rename "CHN" to "CPA" and remove "CHN" and "RCPA"
    # If node == "R12", add "R12_" to the beginning of each region
    if node.upper() == "R11":
        df_intratec_map = (
            df_intratec.assign(region=lambda x: "R11_" + x.intratec_region)
            .replace({"region": {"R11_CHN": "R11_CPA", "R11_RCPA": np.nan}})
            .dropna()
        )
    if node.upper() == "R12":
        df_intratec_map = df_intratec.assign(
            region=lambda x: "R12_" + x.intratec_region
        )

    # If specified reference region is not in data, then give error
    ref_region = ref_region.upper()
    if ref_region not in df_intratec_map.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. \
                Please specify a different reference region. \
                    Available regions are: {df_intratec_map.region.unique()}"
        )

    # Calculate regional investment cost ratio relative to reference region
    df_reg_ratios = (
        df_intratec_map.query("region == @ref_region")
        .rename(columns={"intratec_index": "intratec_ref_region_cost"})
        .drop(columns={"intratec_region", "region"})
        .merge(df_intratec_map, on=["intratec_tech"])
        .assign(reg_cost_ratio=lambda x: x.intratec_index / x.intratec_ref_region_cost)
        .reindex(
            [
                "intratec_tech",
                "region",
                "intratec_ref_region_cost",
                "reg_cost_ratio",
            ],
            axis=1,
        )
    )

    return df_reg_ratios


# Function to get regional differentiation
# Inputs: module, node, ref_region
# If reg_diff_source is "energy" or "weo", then use WEO data
# If reg_diff_source is "intratec", then use Intratec data
# If reg_diff_source is "none", then assume no regional differentiation
# and use the reference region cost as the cost across all regions
def apply_regional_differentiation(module, node, ref_region) -> pd.DataFrame:
    """Apply regional differentiation depending on mapping source

    Parameters
    ----------
    module : str
        Model module
    node : str
        MESSAGEix node
    ref_region : str
        Reference region

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: MESSAGEix technology name
        - reg_diff_source: data source to map MESSAGEix technology to \
            (e.g., WEO, Intratec)
        - reg_diff_technology: technology name in the data source
        - region: MESSAGEix region
        - base_year_reference_region_cost: manually specified base year cost
        of the technology in the reference region (in 2005 USD)
        - reg_cost_ratio: regional cost ratio relative to reference region
        - fix_ratio: ratio of fixed O&M costs to investment costs
    """

    # Set default values for input arguments
    # If specified node is R11, then use R11_NAM as the reference region
    # If specified node is R12, then use R12_NAM as the reference region
    # If specified node is R20, then use R20_NAM as the reference region
    # However, if a reference region is specified, then use that instead
    if ref_region is None:
        if node.upper() == "R11":
            ref_region = "R11_NAM"
        if node.upper() == "R12":
            ref_region = "R12_NAM"
        if node.upper() == "R20":
            ref_region = "R20_NAM"
    else:
        ref_region = ref_region

    df_map = adjust_technology_mapping(module)
    df_weo = get_weo_regional_differentiation(node, ref_region)
    df_intratec = get_intratec_regional_differentiation(node, ref_region)

    # Filter for reg_diff_source == "energy" or "weo"
    # Then merge with output of get_weo_regional_differentiation
    # If the base_year_reference_region_cost is empty, then use the weo_ref_region_cost
    # If the fix_ratio is empty, then use weo_fix_ratio
    filt_weo = (
        df_map.query("reg_diff_source == 'energy' or reg_diff_source == 'weo'")
        .merge(
            df_weo, left_on="reg_diff_technology", right_on="weo_technology", how="left"
        )
        .assign(
            base_year_reference_region_cost=lambda x: np.where(
                x.base_year_reference_region_cost.isnull(),
                x.weo_ref_region_cost,
                x.base_year_reference_region_cost,
            ),
            fix_ratio=lambda x: np.where(
                x.fix_ratio.isnull(), x.weo_fix_ratio, x.fix_ratio
            ),
        )
        .reindex(
            [
                "message_technology",
                "reg_diff_source",
                "reg_diff_technology",
                "region",
                "base_year_reference_region_cost",
                "reg_cost_ratio",
                "fix_ratio",
            ],
            axis=1,
        )
    )

    filt_weo.query("message_technology == 'coal_ppl'")

    # Filter for reg_diff_source == "intratec"
    # Then merge with output of get_intratec_regional_differentiation
    # If the base_year_reference_region_cost is empty,
    # then use the intratec_ref_region_cost
    # If the fix_ratio is empty, then use 0
    filt_intratec = (
        df_map.query("reg_diff_source == 'intratec'")
        .merge(
            df_intratec,
            left_on="reg_diff_technology",
            right_on="intratec_tech",
            how="left",
        )
        .assign(
            base_year_reference_region_cost=lambda x: np.where(
                x.base_year_reference_region_cost.isnull(),
                x.intratec_ref_region_cost,
                x.base_year_reference_region_cost,
            ),
            fix_ratio=lambda x: np.where(x.fix_ratio.isnull(), 0, x.fix_ratio),
        )
        .reindex(
            [
                "message_technology",
                "reg_diff_source",
                "reg_diff_technology",
                "region",
                "base_year_reference_region_cost",
                "reg_cost_ratio",
                "fix_ratio",
            ],
            axis=1,
        )
    )

    # Filter for reg_diff_source == NaN
    # Create dataframe of all regions and merge with map data
    # Assume reg_cost_ratio = 1 for all regions
    # If the fix_ratio is empty, then use 0
    un_reg = pd.DataFrame(
        {"region": filt_intratec.region.unique(), "reg_cost_ratio": 1, "key": "z"}
    )

    filt_none = (
        df_map.query("reg_diff_source.isnull()")
        .assign(key="z")
        .merge(un_reg, on="key", how="left")
        .assign(fix_ratio=lambda x: np.where(x.fix_ratio.isnull(), 0, x.fix_ratio))
        .reindex(
            [
                "message_technology",
                "reg_diff_source",
                "reg_diff_technology",
                "region",
                "base_year_reference_region_cost",
                "reg_cost_ratio",
                "fix_ratio",
            ],
            axis=1,
        )
    )

    all_tech = (
        pd.concat([filt_weo, filt_intratec, filt_none])
        .reset_index(drop=True)
        .assign(
            reg_cost_base_year=lambda x: x.base_year_reference_region_cost
            * x.reg_cost_ratio
        )
    )

    return all_tech
