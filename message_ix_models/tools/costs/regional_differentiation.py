import logging
from collections.abc import Mapping
from functools import lru_cache
from itertools import product
from typing import Literal

import numpy as np
import pandas as pd
from iam_units import registry

from message_ix_models.util import package_data_path
from message_ix_models.util.node import adapt_R11_R12

from .config import Config

log = logging.getLogger(__name__)


@lru_cache
def get_weo_region_map(regions: str) -> Mapping[str, str]:
    """Return a mapping from MESSAGE node IDs to WEO region names.

    The mapping is constructed from the ``iea-weo-region`` annotations on the
    :doc:`/pkg-data/node`.
    """
    from message_ix_models.model.structure import get_codelist

    # Retrieve the appropriate node codelist; the "World" code; and its children
    nodes = get_codelist(f"node/{regions}")["World"].child
    # Map from the child's (node's) ID to the value of the "iea-weo-region" annotation
    return {n.id: str(n.get_annotation(id="iea-weo-region").text) for n in nodes}


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
        "bioenergy_ccus": ["Renewables", 99],
        "bioenergy_cofiring": ["Renewables", 79],
        "bioenergy_large": ["Renewables", 69],
        "bioenergy_medium_chp": ["Renewables", 89],
        "ccgt": ["Gas", 9],
        "ccgt_ccs": ["Fossil fuels equipped with CCUS", 29],
        "ccgt_chp": ["Gas", 29],
        "csp": ["Renewables", 109],
        "fuel_cell": ["Gas", 39],
        "gas_turbine": ["Gas", 19],
        "geothermal": ["Renewables", 119],
        "hydropower_large": ["Renewables", 49],
        "hydropower_small": ["Renewables", 59],
        "igcc": ["Coal", 39],
        "igcc_ccs": ["Fossil fuels equipped with CCUS", 19],
        "marine": ["Renewables", 129],
        "nuclear": ["Nuclear", 9],
        "pulverized_coal_ccs": ["Fossil fuels equipped with CCUS", 9],
        "solarpv_buildings": ["Renewables", 19],
        "solarpv_large": ["Renewables", 9],
        "steam_coal_subcritical": ["Coal", 9],
        "steam_coal_supercritical": ["Coal", 19],
        "steam_coal_ultrasupercritical": ["Coal", 29],
        "wind_offshore": ["Renewables", 39],
        "wind_onshore": ["Renewables", 29],
    }

    # Dict of cost types to read in and the required columns
    DICT_COST_COLS = {"inv_cost": "A,B:D", "fix_cost": "A,F:H"}

    # Set file path for raw IEA WEO cost data
    file_path = package_data_path(
        "iea", "WEO_2023_PG_Assumptions_STEPSandNZE_Scenario.xlsx"
    )

    # Retrieve conversion factor
    conversion_factor = registry("1.0 USD_2022").to("USD_2005").magnitude

    # Loop through Excel sheets to read in data and process:
    # - Convert to long format
    # - Only keep investment costs
    # - Replace "n.a." with NaN
    # - Convert units from 2022 USD to 2005 USD
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
            .set_axis(["weo_region", "2022", "2030", "2050"], axis=1)
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
            .assign(value=lambda x: x.value * conversion_factor)
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


def get_intratec_data() -> pd.DataFrame:
    """Read in raw Intratec data.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - node: Intratec region
        - value: Intratec index value
    """

    # Set file path for raw Intratec data
    file = package_data_path("intratec", "R11", "indices.csv")

    return pd.read_csv(file, comment="#", skipinitialspace=True)


def get_raw_technology_mapping(
    module: Literal["energy", "materials", "cooling"],
) -> pd.DataFrame:
    """Retrieve a technology mapping for `module`.

    The data are read from a CSV file at :file:`data/{module}/tech_map.csv`.
    The file must have the following columns:

    - ``message_technology``: MESSAGEix-GLOBIOM technology code
    - ``reg_diff_source``: data source to map MESSAGEix technology to. A string like
      "weo", "energy", or possibly others.
    - ``reg_diff_technology``: technology code in the source data.
    - ``base_year_reference_region_cost``: manually specified base year cost of the
      technology in the reference region (in 2005 USD).
    - ``fix_ratio``: manually specified of fixed O&M costs to investment costs.

    Parameters
    ----------
    module : str
        See :attr:`.Config.module`.

    Returns
    -------
    pandas.DataFrame
    """

    path = package_data_path("costs", module, "tech_map.csv")
    return pd.read_csv(path, comment="#")


def subset_module_map(raw_map):
    """Subset non-energy module mapping for only technologies that have sufficient data.

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
    # - Remove module technologies that are missing both a reg_diff_source and a
    # base_year_reference_region_cost
    sub_map = (
        raw_map.query(
            "reg_diff_source.notnull() or base_year_reference_region_cost.notnull()"
        )
        .rename(columns={"base_year_reference_region_cost": "base_cost"})
        .assign(base_year_reference_region_cost=lambda x: x.base_cost)
        .drop(columns={"base_cost"})
    )

    return sub_map


def adjust_technology_mapping(
    module: Literal["energy", "materials", "cooling"],
) -> pd.DataFrame:
    """Adjust technology mapping based on sources and assumptions.

    Parameters
    ----------
    module : str
        See :attr:`.Config.module`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: MESSAGEix technology name.
        - reg_diff_source: data source to map MESSAGEix technology to (e.g., WEO,
          Intratec).
        - reg_diff_technology: technology name in the data source.
        - base_year_reference_region_cost: manually specified base year cost
          of the technology in the reference region (in 2005 USD).
    """

    raw_map_energy = get_raw_technology_mapping("energy")

    if module == "energy":
        return raw_map_energy

    else:
        raw_map_module = get_raw_technology_mapping(module)
        sub_map_module = subset_module_map(raw_map_module)

        # If message_technology in sub_map_module is in raw_map_energy and
        # base_year_reference_region_cost is not null/empty, then replace
        # base_year_reference_region_cost in raw_map_energy with
        # base_year_reference_region_cost in sub_map_module
        module_replace = (
            sub_map_module.query(
                "message_technology in @raw_map_energy.message_technology"
            )
            .rename(
                columns={
                    "message_technology": "material_message_technology",
                    "base_year_reference_region_cost": "module_base_cost",
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
                    x.module_base_cost.notnull(),
                    x.module_base_cost,
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
        # null/empty in raw_module_map,
        # then use that.
        # If the base_year_reference_region_cost is null/empty in raw_module_map,
        # then use the base_year_reference_region_cost from the mapped energy technology
        module_map_energy = (
            sub_map_module.query("reg_diff_source == 'energy'")
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

        # Get technologies that don't have a map source but do have a base year cost
        # For these technologies, assume no regional differentiation
        # So use the reference region base year cost as the base year cost
        # across all regions
        module_map_noregdiff = sub_map_module.query(
            "reg_diff_source.isnull() and base_year_reference_region_cost.notnull()"
        )

        # Concatenate module_replace, module_map_energy, and module_map_noregdiff
        # Drop duplicates
        module_all = (
            pd.concat(
                [
                    module_replace,
                    module_map_energy,
                    module_map_noregdiff,
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # If module == "materials", then get materials_map_intratec
        # and concatenate with module_all
        if module == "materials":
            # Get technologies that are mapped to Intratec AND have a base year cost
            # Assign map_techonology as "all"
            materials_map_intratec = sub_map_module.query(
                "reg_diff_source == 'intratec' and "
                "base_year_reference_region_cost.notnull()"
            ).assign(reg_diff_technology="all")

            # Concatenate materials_map_intratec and module_all
            # Drop duplicates
            module_all = (
                pd.concat(
                    [
                        module_all,
                        materials_map_intratec,
                    ]
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )

        # Get full list of technologies in module_all
        # If a custom fix_ratio exists in raw_map_energy, then use that
        # If a custom fix_ratio exists in sub_map_module, then use that
        # (including replacing one in raw_map_energy)
        # Otherwise, keep the fix_ratio as null
        module_all = (
            module_all.merge(
                raw_map_energy.query("fix_ratio.notnull()")[
                    ["message_technology", "fix_ratio"]
                ].rename(columns={"fix_ratio": "fix_ratio_energy"}),
                how="left",
                on="message_technology",
            )
            .merge(
                sub_map_module.query("fix_ratio.notnull()")[
                    ["message_technology", "fix_ratio"]
                ].rename(columns={"fix_ratio": "fix_ratio_module"}),
                how="left",
                on="message_technology",
            )
            .assign(
                fix_ratio=lambda x: np.where(
                    x.fix_ratio_energy.notnull(),
                    x.fix_ratio_energy,
                    x.fix_ratio,
                )
            )
            .assign(
                fix_ratio=lambda x: np.where(
                    x.fix_ratio_module.notnull(),
                    x.fix_ratio_module,
                    x.fix_ratio,
                )
            )
            .drop(columns=["fix_ratio_energy", "fix_ratio_module"])
        )

        # Get list of technologies in raw_map_module that are not in module_all
        missing_tech = raw_map_module.query(
            "message_technology not in @module_all.message_technology"
        ).message_technology.unique()

        log.info(
            "The following technologies are not projected due to insufficient data:"
            + "\n"
            + "\n".join(missing_tech)
        )

        return module_all


def get_weo_regional_differentiation(config: "Config") -> pd.DataFrame:
    """Apply WEO regional differentiation.

    1. Retrieve WEO data using :func:`.get_weo_data`.
    2. Map data to MESSAGEix-GLOBIOM regions according to the :attr:`.Config.node`.
    3. Calculate cost ratios for each region relative to the
       :attr:`~.Config.ref_region`.

    Parameters
    ----------
    config : .Config
        The function responds to the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.node`, and
        :attr:`~.Config.ref_region`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - weo_ref_region_cost: WEO cost in reference region
        - reg_cost_ratio: regional cost ratio relative to reference region
    """

    # Grab WEO data and keep only investment costs
    df_weo = get_weo_data()

    # Even if config.base_year is greater than 2022, use 2022 WEO values
    sel_year = str(2022)
    log.info("…using year " + str(sel_year) + " data from WEO")

    # - Retrieve a map from MESSAGEix node IDs to WEO region names.
    # - Map WEO data to MESSAGEix regions.
    # - Keep only base year data.
    l_sel_weo = []
    for message_node, weo_region in get_weo_region_map(config.node).items():
        df_sel = (
            df_weo.query("year == @sel_year & weo_region == @weo_region")
            .assign(region=message_node)
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
    assert config.ref_region is not None
    ref_region = config.ref_region.upper()
    if ref_region not in df_sel_weo.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. "
            "Please specify a different reference region. "
            f"Available regions are: {df_sel_weo.region.unique()}"
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


def get_intratec_regional_differentiation(node: str, ref_region: str) -> pd.DataFrame:
    """Apply Intratec regional differentiation.

    1. Retrieve Intratec data using :func:`.get_intratec_data`.
    2. Map data to MESSAGEix-GLOBIOM regions according to the :attr:`.Config.node`.
    3. Calculate cost ratios for each region relative to the
       :attr:`~.Config.ref_region`.

    Parameters
    ----------
    node : str
        See :attr`.Config.node`.
    ref_region : str
        See :attr`.Config.ref_region`.

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
    # If node is R11, then map directly
    # If node is R12, then adapt R11 regions to R12 regions
    if node.upper() == "R11":
        df_intratec_map = df_intratec.rename(
            columns={"node": "region", "value": "intratec_index"}
        ).assign(intratec_tech="all")
    elif node.upper() == "R12":
        df_intratec_map = (
            adapt_R11_R12(df_intratec)
            .rename(columns={"node": "region", "value": "intratec_index"})
            .assign(intratec_tech="all")
            .drop(columns=["unit"])
        )
    elif node.upper() == "R20":
        raise NotImplementedError

    # If specified reference region is not in data, then give error
    ref_region = ref_region.upper()
    if ref_region not in df_intratec_map.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. "
            "Please specify a different reference region. "
            f"Available regions are: {df_intratec_map.region.unique()}"
        )

    # Calculate regional investment cost ratio relative to reference region
    df_reg_ratios = (
        df_intratec_map.query("region == @ref_region")
        .rename(columns={"intratec_index": "intratec_ref_region_cost"})
        .drop(columns={"region"})
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


def apply_regional_differentiation(config: "Config") -> pd.DataFrame:
    """Apply regional differentiation depending on mapping source.

    1. Retrieve an adjusted technology mapping from :func:`.adjust_technology_mapping`.
    2. Based on the value in the ``reg_diff_source`` column:

       - "energy" or "weo": use WEO data via :func:`.get_weo_regional_differentiation`.
       - "intratec": use Intratec data via
         :func:`.get_intratec_regional_differentiation`.
       - "none": assume no regional differentiation; use the :attr:`~.Config.ref_region`
         cost as the cost for all regions.

    Parameters
    ----------
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.module`,
        :attr:`~.Config.node`, and
        :attr:`~.Config.ref_region`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: MESSAGEix technology name
        - reg_diff_source: data source to map MESSAGEix technology to (e.g., WEO,
          Intratec)
        - reg_diff_technology: technology name in the data source
        - region: MESSAGEix region
        - base_year_reference_region_cost: manually specified base year cost
          of the technology in the reference region (in 2005 USD)
        - reg_cost_ratio: regional cost ratio relative to reference region
        - fix_ratio: ratio of fixed O&M costs to investment costs
    """
    df_map = adjust_technology_mapping(config.module)
    assert config.ref_region is not None
    df_weo = get_weo_regional_differentiation(config)
    df_intratec = get_intratec_regional_differentiation(config.node, config.ref_region)

    # Get mapping of technologies
    # Then merge with output of get_weo_regional_differentiation
    # If the base_year_reference_region_cost is empty, then use the weo_ref_region_cost
    # If the fix_ratio is empty, then use weo_fix_ratio
    filt_weo = (
        df_map.merge(
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

    # TODO: Change from using intratec source as list of regions
    un_reg = pd.DataFrame(
        {"region": filt_intratec.region.unique(), "reg_cost_ratio": 1, "key": "z"}
    )

    # Filter for reg_diff_source == NaN
    # Create dataframe of all regions and merge with map data
    # Assume reg_cost_ratio = 1 for all regions
    # If the fix_ratio is empty, then use 0
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
            reg_cost_ratio=lambda x: np.where(
                x.reg_diff_source.isna() & x.reg_diff_technology.isna(),
                1,
                x.reg_cost_ratio,
            )
        )
        .assign(
            reg_cost_base_year=lambda x: x.base_year_reference_region_cost
            * x.reg_cost_ratio
        )
        .dropna(subset=["region"])
        .reset_index(drop=True)
    )

    return all_tech
