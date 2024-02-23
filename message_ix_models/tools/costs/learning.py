import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

from .config import (
    FIRST_MODEL_YEAR,
    LAST_MODEL_YEAR,
    PRE_LAST_YEAR_RATE,
    TIME_STEPS,
    Config,
)
from .regional_differentiation import get_raw_technology_mapping, subset_materials_map


# Function to get GEA based cost reduction data
def get_cost_reduction_data(module) -> pd.DataFrame:
    """Get cost reduction data

    Raw data on cost reduction in 2100 for technologies are read from
    :file:`data/[module]/cost_reduction_[module].csv`.

    Parameters
    ----------
    module : str
        Model module

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: name of technology in MESSAGEix
        - learning_rate: the learning rate (either very_low, low, medium, high, or
          very_high)
        - cost_reduction: cost reduction in 2100 (%)
    """

    # Get full list of technologies from mapping
    if module == "energy":
        tech_map = get_raw_technology_mapping("energy")

    if module == "materials":
        energy_map = get_raw_technology_mapping("energy")
        materials_map = get_raw_technology_mapping("materials")
        materials_sub = subset_materials_map(materials_map)

        # Remove energy technologies that exist in materials mapping
        energy_map = energy_map.query(
            "message_technology not in @materials_sub.message_technology"
        )

        tech_map = pd.concat([energy_map, materials_sub], ignore_index=True)

    # Read in raw data
    gea_file_path = package_data_path("costs", "energy", "cost_reduction_energy.csv")
    energy_rates = (
        pd.read_csv(gea_file_path, header=8)
        .melt(
            id_vars=["message_technology", "technology_type"],
            var_name="learning_rate",
            value_name="cost_reduction",
        )
        .assign(
            technology_type=lambda x: x.technology_type.fillna("NA"),
            cost_reduction=lambda x: x.cost_reduction.fillna(0),
        )
        .drop_duplicates()
        .reset_index(drop=1)
    ).reindex(["message_technology", "learning_rate", "cost_reduction"], axis=1)

    # For materials technologies with map_tech == energy, map to base technologies
    # and use cost reduction data
    materials_rates_energy = (
        tech_map.query("reg_diff_source == 'energy'")
        .drop(columns=["reg_diff_source", "base_year_reference_region_cost"])
        .merge(
            energy_rates.rename(
                columns={"message_technology": "base_message_technology"}
            ),
            how="inner",
            left_on="reg_diff_technology",
            right_on="base_message_technology",
        )
        .drop(columns=["base_message_technology", "reg_diff_technology"])
        .drop_duplicates()
        .reset_index(drop=1)
    ).reindex(["message_technology", "learning_rate", "cost_reduction"], axis=1)

    # Combine technologies that have cost reduction rates
    df_reduction_techs = pd.concat(
        [energy_rates, materials_rates_energy], ignore_index=True
    )
    df_reduction_techs = df_reduction_techs.drop_duplicates().reset_index(drop=1)

    # Create unique dataframe of learning rates and make all cost_reduction values 0
    un_rates = pd.DataFrame(
        {
            "learning_rate": ["none"],
            "cost_reduction": [0],
            "key": "z",
        }
    )

    # For remaining materials technologies that are not mapped to energy technologies,
    # assume no cost reduction
    materials_rates_nolearning = (
        tech_map.query(
            "message_technology not in @df_reduction_techs.message_technology"
        )
        .assign(key="z")
        .merge(un_rates, on="key")
        .drop(columns=["key"])
    ).reindex(["message_technology", "learning_rate", "cost_reduction"], axis=1)

    # Concatenate base and materials rates
    all_rates = pd.concat(
        [energy_rates, materials_rates_energy, materials_rates_nolearning],
        ignore_index=True,
    ).reset_index(drop=1)

    return all_rates


# Function to get technology learning scenarios data
def get_technology_learning_scenarios_data(base_year, module) -> pd.DataFrame:
    """Read in technology first year and cost reduction scenarios

    Raw data on technology first year and learning scenarios are read from
    :file:`data/costs/[module]/first_year_[module]`. The first year the technology is
    available in MESSAGEix is adjusted to be the base year if the original first year is
    before the base year.

    Raw data on cost reduction scenarios are read from
    :file:`data/costs/[module]/scenarios_reduction_[module].csv`.

    Assumptions are made for the materials module for technologies' cost reduction
    scenarios that are not given.

    Parameters
    ----------
    base_year : int, optional
        The base year, by default set to global BASE_YEAR
    module : str
        Model module

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: name of technology in MESSAGEix
        - scenario: learning scenario (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - first_technology_year: first year the technology is available in MESSAGEix.
        - learning_rate: the learning rate (either very_low, low, medium, high, or
          very_high)
    """

    if module == "energy":
        energy_first_year_file = package_data_path(
            "costs", "energy", "first_year_energy.csv"
        )
        df_first_year = pd.read_csv(energy_first_year_file, skiprows=3)

    if module == "materials":
        energy_first_year_file = package_data_path(
            "costs", "energy", "first_year_energy.csv"
        )
        energy_first_year = pd.read_csv(energy_first_year_file, skiprows=3)

        materials_first_year_file = package_data_path(
            "costs", "materials", "first_year_materials.csv"
        )
        materials_first_year = pd.read_csv(materials_first_year_file)
        df_first_year = pd.concat(
            [energy_first_year, materials_first_year], ignore_index=True
        ).drop_duplicates()

    if module == "energy":
        tech_map = (
            get_raw_technology_mapping("energy")
            .reindex(
                ["message_technology", "reg_diff_source", "reg_diff_technology"], axis=1
            )
            .drop_duplicates()
        )
    if module == "materials":
        tech_energy = get_raw_technology_mapping("energy")
        tech_materials = subset_materials_map(get_raw_technology_mapping("materials"))
        tech_energy = tech_energy.query(
            "message_technology not in @tech_materials.message_technology"
        )
        tech_map = (
            pd.concat([tech_energy, tech_materials], ignore_index=True)
            .reindex(
                ["message_technology", "reg_diff_source", "reg_diff_technology"], axis=1
            )
            .drop_duplicates()
        )

    # Adjust first year:
    # - if first year is missing, set to base year
    # - if first year is after base year, then keep assigned first year
    all_first_year = (
        pd.merge(tech_map, df_first_year, on="message_technology", how="left")
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original.isnull(),
                base_year,
                x.first_year_original,
            )
        )
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original > base_year, x.first_year_original, base_year
            )
        )
        .drop(columns=["first_year_original"])
    )

    # Create new column for scenario_technology
    # - if reg_diff_source == weo, then scenario_technology = message_technology
    # - if reg_diff_source == energy, then scenario_technology = reg_diff_technology
    # - otherwise, scenario_technology = message_technology
    adj_first_year = (
        all_first_year.assign(
            scenario_technology=lambda x: np.where(
                x.reg_diff_source == "weo",
                x.message_technology,
                np.where(
                    x.reg_diff_source == "energy",
                    x.reg_diff_technology,
                    x.message_technology,
                ),
            )
        )
        .drop(columns=["reg_diff_source", "reg_diff_technology"])
        .drop_duplicates()
        .reset_index(drop=1)
    )

    # Merge with energy technologies that have given scenarios
    energy_scen_file = package_data_path(
        "costs", "energy", "scenarios_reduction_energy.csv"
    )
    df_energy_scen = pd.read_csv(energy_scen_file).rename(
        columns={"message_technology": "scenario_technology"}
    )

    existing_scens = (
        pd.merge(
            adj_first_year,
            df_energy_scen,
            on=["scenario_technology"],
            how="inner",
        )
        .drop(columns=["scenario_technology"])
        .melt(
            id_vars=[
                "message_technology",
                "first_technology_year",
            ],
            var_name="scenario",
            value_name="learning_rate",
        )
    )

    # Create dataframe of SSP1-SSP5 and LED scenarios with "none" learning rate
    un_scens = pd.DataFrame(
        {
            "scenario": ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"],
            "learning_rate": "none",
            "key": "z",
        }
    )

    # Get remaining technologies that do not have given scenarios
    remaining_scens = (
        adj_first_year.query(
            "message_technology not in @existing_scens.message_technology.unique()"
        )
        .assign(key="z")
        .merge(un_scens, on="key")
        .drop(columns=["key", "scenario_technology"])
    )

    # Concatenate all technologies
    all_scens = (
        pd.concat([existing_scens, remaining_scens], ignore_index=True)
        .sort_values(by=["message_technology", "scenario"])
        .reset_index(drop=1)
    )

    return all_scens


# Function to project reference region investment cost using learning rates
def project_ref_region_inv_costs_using_learning_rates(
    regional_diff_df: pd.DataFrame, config: Config
) -> pd.DataFrame:
    """Project investment costs using learning rates for reference region

    This function uses the learning rates for each technology under each scenario to
    project the capital costs for each technology in the reference region.

    Parameters
    ----------
    regional_diff_df : pandas.DataFrame
        Dataframe output from :func:`get_weo_region_differentiated_costs`
    ref_region : str, optional
        The reference region, by default None (defaults set in function)
    base_year : int, optional
        The base year, by default set to global BASE_YEAR
    module : str
        Model module

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: name of technology in MESSAGEix
        - scenario: learning scenario (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - reference_region: reference region
        - first_technology_year: first year the technology is available in MESSAGEix.
        - year: year
        - inv_cost_ref_region_learning: investment cost in reference region in year.
    """

    # Get cost reduction data
    df_cost_reduction = get_cost_reduction_data(config.module)

    # Get learning rates data
    df_learning = get_technology_learning_scenarios_data(
        config.base_year, config.module
    )

    # Merge cost reduction data with learning rates data
    df_learning_reduction = df_learning.merge(
        df_cost_reduction, on=["message_technology", "learning_rate"], how="left"
    )

    # Filter for reference region, then merge with learning scenarios and discount rates
    # Calculate cost in reference region in 2100
    df_ref = (
        regional_diff_df.query("region == @config.ref_region")
        .merge(df_learning_reduction, on="message_technology")
        .assign(
            cost_region_2100=lambda x: x.reg_cost_base_year
            - (x.reg_cost_base_year * x.cost_reduction),
            b=lambda x: (1 - PRE_LAST_YEAR_RATE) * x.cost_region_2100,
            r=lambda x: (1 / (LAST_MODEL_YEAR - config.base_year))
            * np.log((x.cost_region_2100 - x.b) / (x.reg_cost_base_year - x.b)),
            reference_region=config.ref_region,
        )
    )

    seq_years = list(range(FIRST_MODEL_YEAR, LAST_MODEL_YEAR + TIME_STEPS, TIME_STEPS))

    for y in seq_years:
        df_ref = df_ref.assign(
            ycur=lambda x: np.where(
                y <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                (x.reg_cost_base_year - x.b)
                * np.exp(x.r * (y - x.first_technology_year))
                + x.b,
            )
        ).rename(columns={"ycur": y})

    df_inv_ref = (
        df_ref.drop(
            columns=[
                "b",
                "r",
                "reg_diff_source",
                "reg_diff_technology",
                "region",
                "base_year_reference_region_cost",
                "reg_cost_ratio",
                "reg_cost_base_year",
                "fix_ratio",
                "learning_rate",
                "cost_reduction",
                "cost_region_2100",
            ]
        )
        .melt(
            id_vars=[
                "message_technology",
                "scenario",
                "reference_region",
                "first_technology_year",
            ],
            var_name="year",
            value_name="inv_cost_ref_region_learning",
        )
        .assign(year=lambda x: x.year.astype(int))
    ).drop_duplicates()

    return df_inv_ref
