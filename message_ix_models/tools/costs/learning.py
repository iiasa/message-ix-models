import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

# Global variables of model years
FIRST_MODEL_YEAR = 2020
LAST_MODEL_YEAR = 2100
PRE_LAST_YEAR_RATE = 0.01

# Dict of technology types and the learning rates under each SSP
# Data translated from excel form into python form from Sheet 1 in
# https://github.com/iiasa/message_data/blob/dev/data/model/investment_cost/SSP_technology_learning.xlsx
DICT_TECH_SSP_LEARNING = {
    "Biomass": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "medium",
    },
    "CCS": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "high",
    },
    "Coal": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "high",
        "SSP4": "medium",
        "SSP5": "medium",
    },
    "Gas/Oil": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "medium",
        "SSP5": "high",
    },
    "Nuclear": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "high",
    },
    "Renewable": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "medium",
    },
    "NA": {
        "SSP1": "none",
        "SSP2": "none",
        "SSP3": "none",
        "SSP4": "none",
        "SSP5": "none",
    },
}


def get_technology_first_year_data() -> pd.DataFrame:
    """Read in technology first year data

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - message_technology: technology in MESSAGEix
        - first_year_original: the original first year the technology is \
            available in MESSAGEix
        - first_technology_year: the adjusted first year the technology is \
            available in MESSAGEix
    """
    file = package_data_path("costs", "technology_first_year.csv")
    df = pd.read_csv(file, header=3).assign(
        first_technology_year=lambda x: np.where(
            x.first_year_original > FIRST_MODEL_YEAR,
            x.first_year_original,
            FIRST_MODEL_YEAR,
        )
    )

    return df


def get_cost_reduction_data() -> pd.DataFrame:
    """Create SSP technological learning data

    Raw data from GEA on cost reduction for technologies are read from \
        :file:`data/costs/gea_cost_reduction.csv`.

    This function takes the raw GEA (low, medium, and high) cost reduction \
        values and assign SSP-specific cost reduction values. The growth rate \
        under each SSP scenario (for each technology) is specified in \
        the input dictionary (`input_dict_tech_learning`). If the SSP \
        learning rate is "low", then the cost reduction rate is the minimum of the GEA \
        values for that technology. If the SSP learning rate is "medium" or "high", \
        then the cost reduction rate is the median of the GEA scenarios or the maximum \
        of the GEA scenarios, respectively.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: technologies included in MESSAGE
        - technology_type: the technology type (either coal, gas/oil, biomass, CCS, \
            renewable, nuclear, or NA)
        - GEAL: cost reduction in 2100 (%) under the low (L) GEA scenario
        - GEAM: cost reduction in 2100 (%) under the medium (M) GEA scenario
        - GEAH: cost reduction in 2100 (%) under the high (H) GEA scenario
        - SSPX_learning: one corresponding column for each SSP scenario \
            (SSP1, SSP2, SSP3, SSP4, SSP5). These columns specify the learning \
            rate for each technology under that specific scenario
        - SSPX_cost_reduction: the cost reduction (%) of the technology under the \
            specific scenario
    """

    input_dict_tech_learning = DICT_TECH_SSP_LEARNING

    # Read in raw data files
    gea_file_path = package_data_path("costs", "gea_cost_reduction.csv")

    # Read in data and assign basic columns
    df_gea = (
        pd.read_csv(gea_file_path, header=6)
        .rename(
            columns={"Technologies": "message_technology", "Type": "technology_type"}
        )
        .assign(
            learning=lambda x: np.where(
                (x["GEAL"] == 0) & (x["GEAM"] == 0) & (x["GEAH"] == 0), "no", "yes"
            ),
            min_gea=lambda x: x[["GEAL", "GEAM", "GEAH"]].min(axis=1),
            median_gea=lambda x: np.median(x[["GEAL", "GEAM", "GEAH"]], axis=1),
            max_gea=lambda x: x[["GEAL", "GEAM", "GEAH"]].max(axis=1),
        )
        .replace({"technology_type": np.nan}, "NA")
    )

    # Assign SSP learning category and SSP-specific cost reduction rate
    def assign_ssp_learning():
        cols = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        for c in cols:
            df_gea[c + "_learning"] = np.where(
                df_gea["learning"] == "no",
                "none",
                df_gea.technology_type.map(lambda x: input_dict_tech_learning[x][c]),
            )
            df_gea[c + "_cost_reduction"] = np.where(
                df_gea[c + "_learning"] == "low",
                df_gea["min_gea"],
                np.where(
                    df_gea[c + "_learning"] == "medium",
                    df_gea["median_gea"],
                    np.where(
                        df_gea[c + "_learning"] == "high",
                        df_gea["max_gea"],
                        0,
                    ),
                ),
            )

    assign_ssp_learning()

    # Convert from wide to long
    df_long = df_gea.melt(
        id_vars=["message_technology", "technology_type"],
        value_vars=[
            "SSP1_cost_reduction",
            "SSP2_cost_reduction",
            "SSP3_cost_reduction",
        ],
        var_name="scenario",
        value_name="cost_reduction",
    ).assign(scenario=lambda x: x.scenario.str.replace("_cost_reduction", ""))

    return df_long


# Function to project investment costs using learning rates for NAM region only
def project_NAM_inv_costs_using_learning_rates(
    regional_diff_df: pd.DataFrame,
    learning_rates_df: pd.DataFrame,
    tech_first_year_df: pd.DataFrame,
) -> pd.DataFrame:
    """Project investment costs using learning rates for NAM region only

    This function uses the learning rates for each technology under each SSP \
        scenario to project the capital costs for each technology in the NAM \
        region. The capital costs for each technology in the NAM region are \
        first calculated by multiplying the regional cost ratio (relative to \
        OECD) by the OECD capital costs. Then, the capital costs are projected \
        using the learning rates under each SSP scenario.

    Parameters
    ----------
    regional_diff_df : pandas.DataFrame
        Dataframe output from :func:`get_region_differentiated_costs`

    learning_rates_df : pandas.DataFrame
        Dataframe output from :func:`get_cost_reduction_data`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: technologies included in MESSAGE
        - technology_type: the technology type (either coal, gas/oil, biomass, CCS, \
            renewable, nuclear, or NA)
        - r11_region: R11 region
        - cost_type: either "inv_cost" or "fom_cost"
        - year: values from 2000 to 2100

    """

    df_reg = regional_diff_df.copy()
    df_discount = learning_rates_df.copy()
    df_tech_first_year = tech_first_year_df.copy()

    # Filter for NAM region and investment cost only, then merge with discount rates,
    # then merge with first year data
    df_nam = (
        df_reg.loc[(df_reg.r11_region == "NAM") & (df_reg.cost_type == "inv_cost")]
        .merge(df_discount, on="message_technology")
        .merge(df_tech_first_year, on="message_technology")
        .assign(
            cost_region_2100=lambda x: x["cost_region_2021"]
            - (x["cost_region_2021"] * x["cost_reduction"]),
            b=lambda x: (1 - PRE_LAST_YEAR_RATE) * x["cost_region_2100"],
            r=lambda x: (1 / (LAST_MODEL_YEAR - FIRST_MODEL_YEAR))
            * np.log(
                (x["cost_region_2100"] - x["b"]) / (x["cost_region_2021"] - x["b"])
            ),
        )
    )

    seq_years = list(range(FIRST_MODEL_YEAR, LAST_MODEL_YEAR + 10, 10))

    for y in seq_years:
        df_nam = df_nam.assign(
            ycur=lambda x: np.where(
                y <= FIRST_MODEL_YEAR,
                x.cost_region_2021,
                (x.cost_region_2021 - x.b) * np.exp(x.r * (y - x.first_technology_year))
                + x.b,
            )
        ).rename(columns={"ycur": y})

    df_nam = (
        df_nam.drop(
            columns=[
                "b",
                "r",
                "r11_region",
                "weo_region",
                "cost_type",
                "cost_NAM_adjusted",
                "technology_type",
                "cost_reduction",
                "cost_ratio",
                "first_year_original",
                "first_technology_year",
                "cost_region_2021",
                "cost_region_2100",
            ]
        )
        .melt(
            id_vars=[
                "scenario",
                "message_technology",
                "weo_technology",
            ],
            var_name="year",
            value_name="inv_cost_learning_NAM",
        )
        .assign(year=lambda x: x.year.astype(int))
    )

    return df_nam
