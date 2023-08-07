import numpy as np
import pandas as pd

from message_ix_models.tools.costs.config import (
    BASE_YEAR,
    FIRST_MODEL_YEAR,
    LAST_MODEL_YEAR,
    PRE_LAST_YEAR_RATE,
)
from message_ix_models.util import package_data_path


# Function to get GEA based cost reduction data
def get_cost_reduction_data() -> pd.DataFrame:
    """Get cost reduction data

    Raw data on cost reduction in 2100 for technologies are read from \
        :file:`data/costs/gea_cost_reduction.csv`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: technologies included in MESSAGEix
        - technology_type: the technology type (either coal, gas/oil, biomass, CCS, \
            renewable, nuclear, or NA)
        - learning_rate: the learning rate (either low, medium, or high)
        - cost_reduction: cost reduction in 2100 (%)
    """

    # Read in raw data
    gea_file_path = package_data_path("costs", "gea_cost_reduction.csv")
    df_gea = (
        pd.read_csv(gea_file_path, header=6)
        .melt(
            id_vars=["message_technology", "technology_type"],
            var_name="learning_rate",
            value_name="cost_reduction",
        )
        .assign(
            technology_type=lambda x: x.technology_type.fillna("NA"),
            cost_reduction=lambda x: x.cost_reduction.fillna(0),
            learning_rate=lambda x: np.where(
                x.learning_rate == "GEAL",
                "low",
                np.where(x.learning_rate == "GEAM", "medium", "high"),
            ),
        )
        .drop_duplicates()
        .reset_index(drop=1)
    )

    return df_gea


# Function to get technology learning scenarios data
def get_technology_learning_scenarios_data(
    input_base_year: int = BASE_YEAR,
) -> pd.DataFrame:
    """Read in technology first year and learning scenarios data

    Raw data on technology first year and learning scenarios are read from \
        :file:`data/costs/technology_learning_rates.csv`.
    The first year the technology is available in MESSAGEix is adjusted to \
        be the base year if the original first year is before the base year.

    Parameters
    ----------
    input_base_year : int, optional
        The base year, by default set to global BASE_YEAR

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: technology in MESSAGEix
        - first_technology_year: the adjusted first year the technology is \
            available in MESSAGEix
        - scenario: learning scenario (SSP1, SSP2, SSP3, SSP4, or SSP5)
        - learning_rate: the learning rate (either low, medium, or high)
    """

    file = package_data_path("costs", "technology_learning_rates.csv")
    df_learn = (
        pd.read_csv(file)
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original > input_base_year,
                x.first_year_original,
                input_base_year,
            ),
        )
        .drop(columns=["first_year_original"])
        .melt(
            id_vars=["message_technology", "first_technology_year"],
            var_name="scenario",
            value_name="learning_rate",
        )
    )

    return df_learn


# Function to project reference region investment cost using learning rates
def project_ref_region_inv_costs_using_learning_rates(
    regional_diff_df: pd.DataFrame,
    input_node: str = "r12",
    input_ref_region=None,
    input_base_year: int = BASE_YEAR,
) -> pd.DataFrame:
    """Project investment costs using learning rates for reference region

    This function uses the learning rates for each technology under each SSP \
        scenario to project the capital costs for each technology in the \
        reference region.

    Parameters
    ----------
    regional_diff_df : pandas.DataFrame
        Dataframe output from :func:`get_weo_region_differentiated_costs`
    input_node : str, optional
        The reference node, by default "r12"
    input_ref_region : str, optional
        The reference region, by default None (defaults set in function)
    input_base_year : int, optional
        The base year, by default set to global BASE_YEAR

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: technologies included in MESSAGEix
        - scenario: learning scenario (SSP1, SSP2, SSP3, SSP4, or SSP5)
        - year: values from FIRST_MODEL_YEAR to LAST_MODEL_YEAR
        - inv_cost_ref_region_learning: investment cost in reference region \
            using learning rates
    """

    # Set default reference region
    if input_ref_region is None:
        if input_node.upper() == "R11":
            reference_region = "R11_NAM"
        if input_node.upper() == "R12":
            reference_region = "R12_NAM"
        if input_node.upper() == "R20":
            reference_region = "R20_NAM"
    else:
        reference_region = input_ref_region

    # Get cost reduction data
    df_cost_reduction = get_cost_reduction_data()

    # Get learning rates data
    df_learning = get_technology_learning_scenarios_data(input_base_year)

    # Merge cost reduction data with learning rates data
    df_learning_reduction = df_learning.merge(
        df_cost_reduction, on=["message_technology", "learning_rate"], how="left"
    )

    # Filter for reference region, then merge with learning scenarios and discount rates
    # Calculate cost in reference region in 2100
    df_ref = (
        regional_diff_df.query("region == @reference_region")
        .merge(df_learning_reduction, on="message_technology")
        .assign(
            cost_region_2100=lambda x: x.reg_cost_base_year
            - (x.reg_cost_base_year * x.cost_reduction),
            b=lambda x: (1 - PRE_LAST_YEAR_RATE) * x.cost_region_2100,
            r=lambda x: (1 / (LAST_MODEL_YEAR - input_base_year))
            * np.log((x.cost_region_2100 - x.b) / (x.reg_cost_base_year - x.b)),
            reference_region=reference_region,
        )
    )

    seq_years = list(range(FIRST_MODEL_YEAR, LAST_MODEL_YEAR + 5, 5))

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
                "region",
                "reg_cost_ratio",
                "reg_cost_base_year",
                "fix_to_inv_cost_ratio",
                "learning_rate",
                "technology_type",
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
    )

    return df_inv_ref
