import numpy as np
import pandas as pd
from scipy.stats import linregress  # type: ignore

from message_ix_models.util import package_data_path


def get_gdp_data() -> pd.DataFrame:
    """Read in raw GDP data for SSP1, SSP2, SSP3 and output GDP ratios

    Data are read from the files
    :file:`data/iea/gdp_pp_per_capita-ssp1_v9.csv`,
    :file:`data/iea/gdp_pp_per_capita-ssp2_v9.csv`, and
    :file:`data/iea/gdp_pp_per_capita-ssp3_v9.csv`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - scenario: SSP1, SSP2, or SSP3
        - r11_region: R11 region
        - year: values from 2000 to 2100
        - gdp_ppp_per_capita: GDP PPP per capita, in units of billion US$2005/yr/million
        - gdp_ratio_reg_to_oecd: the maximum ratio of each region's GDP compared to \
            OECD regions
        - gdp_ratio_reg_to_nam: the ratio of each region's GDP compared to NAM region
    """

    scens = ["ssp1", "ssp2", "ssp3"]
    l_dfs = []
    for s in scens:
        f = package_data_path("costs", "gdp_pp_per_capita-" + str(s) + "_v9.csv")
        df = (
            pd.read_csv(f, header=4)
            .melt(
                id_vars=["Model", "Scenario", "Region", "Variable", "Unit"],
                var_name="year",
                value_name="gdp_ppp_per_capita",
            )
            .drop(columns=["Model", "Scenario", "Variable", "Unit"])
            .rename(columns={"Region": "r11_region", "Scenario": "scenario"})
            .assign(scenario=s.upper(), units="billion US$2005/yr/million")
            .replace({"r11_region": {"R11": ""}}, regex=True)
            .pipe(
                lambda df_: pd.merge(
                    df_,
                    df_.loc[df_.r11_region.isin(["NAM", "PAO", "WEU"])]
                    .groupby("year")["gdp_ppp_per_capita"]
                    .aggregate(["min", "mean", "max"])
                    .reset_index(drop=0),
                    on="year",
                )
            )
            .pipe(
                lambda df_: pd.merge(
                    df_,
                    df_.loc[df_.r11_region == "NAM"][["year", "gdp_ppp_per_capita"]]
                    .rename(columns={"gdp_ppp_per_capita": "gdp_nam"})
                    .reset_index(drop=1),
                    on="year",
                )
            )
            .rename(columns={"min": "oecd_min", "mean": "oecd_mean", "max": "oecd_max"})
            .assign(
                ratio_oecd_min=lambda x: np.where(
                    x.r11_region.isin(["NAM", "PAO", "WEU"]),
                    1,
                    x.gdp_ppp_per_capita / x.oecd_min,
                ),
                ratio_oecd_max=lambda x: np.where(
                    x.r11_region.isin(["NAM", "PAO", "WEU"]),
                    1,
                    x.gdp_ppp_per_capita / x.oecd_max,
                ),
                gdp_ratio_reg_to_oecd=lambda x: np.where(
                    (x.ratio_oecd_min >= 1) & (x.ratio_oecd_max <= 1),
                    1,
                    x[["ratio_oecd_min", "ratio_oecd_min"]].max(axis=1),
                ),
                gdp_ratio_reg_to_nam=lambda x: x.gdp_ppp_per_capita / x.gdp_nam,
            )
            .reindex(
                [
                    "scenario",
                    "r11_region",
                    "year",
                    "gdp_ppp_per_capita",
                    "gdp_ratio_reg_to_oecd",
                    "gdp_ratio_reg_to_nam",
                ],
                axis=1,
            )
        )

        l_dfs.append(df)

    df_gdp = pd.concat(l_dfs).reset_index(drop=1)

    return df_gdp


def linearly_regress_tech_cost_vs_gdp_ratios(
    gdp_ratios: pd.DataFrame, tech_cost_ratios: pd.DataFrame
) -> pd.DataFrame:
    """Compute linear regressions of technology cost ratios to GDP ratios

    Parameters
    ----------
    gdp_ratios : pandas.DataFrame
        Dataframe output from :func:`.get_gdp_data`
    tech_cost_ratios : str -> tuple of (str, str)
        Dataframe output from :func:`.calculate_region_cost_ratios`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - cost_type: either "fix_cost" or "Inv_cost"
        - scenario: SSP1, SSP2, or SSP3
        - weo_technology: WEO technology name
        - slope: slope of the linear regression
        - intercept: intercept of the linear regression
        - rvalue: rvalue of the linear regression
        - pvalue: pvalue of the linear regression
        - stderr: standard error of the linear regression
    """

    gdp_2020 = gdp_ratios.loc[gdp_ratios.year == "2020"][
        ["scenario", "r11_region", "gdp_ratio_reg_to_nam"]
    ].reset_index(drop=1)
    cost_capital_2021 = tech_cost_ratios[
        ["weo_technology", "r11_region", "cost_type", "cost_ratio"]
    ].reset_index(drop=1)

    df_gdp_cost = (
        pd.merge(gdp_2020, cost_capital_2021, on=["r11_region"])
        .reset_index(drop=2)
        .reindex(
            [
                "cost_type",
                "scenario",
                "r11_region",
                "weo_technology",
                "gdp_ratio_reg_to_nam",
                "cost_ratio",
            ],
            axis=1,
        )
        .groupby(["cost_type", "scenario", "weo_technology"])
        .apply(
            lambda x: pd.Series(linregress(x["gdp_ratio_reg_to_nam"], x["cost_ratio"]))
        )
        .rename(
            columns={
                0: "slope",
                1: "intercept",
                2: "rvalue",
                3: "pvalue",
                4: "stderr",
                "scenario": "scenario",
            }
        )
        .reset_index()
    )

    return df_gdp_cost


# Function to calculate adjusted region-differentiated cost ratios
# using the results from the GDP linear regressions
def calculate_adjusted_region_cost_ratios(gdp_df, linear_regression_df):
    """Calculate adjusted region-differentiated cost ratios

    This function calculates the adjusted region-differentiated cost ratios \
        using the results from the GDP linear regressions. The adjusted \
        region-differentiated cost ratios are calculated by multiplying the \
        slope of the linear regression with the GDP ratio of the region \
        compared to NAM and adding the intercept.

    Parameters
    ----------
    gdp_df : pandas.DataFrame
        Dataframe output from :func:`.get_gdp_data`
    linear_regression_df : pandas.DataFrame
        Dataframe output from :func:`.linearly_regress_tech_cost_vs_gdp_ratios`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario: SSP1, SSP2, or SSP3
        - weo_technology: WEO technology name
        - r11_region: R11 region
        - cost_ratio_adj: the adjusted region-differentiated cost ratio
    """

    df = (
        linear_regression_df.loc[linear_regression_df.cost_type == "inv_cost"]
        .drop(columns=["cost_type"])
        .merge(gdp_df, on=["scenario"])
        .drop(
            columns=[
                "gdp_ppp_per_capita",
                "gdp_ratio_reg_to_oecd",
                "rvalue",
                "pvalue",
                "stderr",
            ]
        )
        .assign(
            cost_ratio_adj=lambda x: np.where(
                x.r11_region == "NAM", 1, x.slope * x.gdp_ratio_reg_to_nam + x.intercept
            ),
            year=lambda x: x.year.astype(int),
        )
        .reindex(
            [
                "scenario",
                "weo_technology",
                "r11_region",
                "year",
                "cost_ratio_adj",
            ],
            axis=1,
        )
    )

    return df


# Function to project investment costs by
# multiplying the learning NAM costs with the adjusted regionally
# differentiated cost ratios
# def project_adjusted_inv_costs(
#     nam_learning_df: pd.DataFrame,
#     adj_cost_ratios_df: pd.DataFrame,
#     use_gdp: bool = False,
# ) -> pd.DataFrame:
#     """Project investment costs using adjusted region-differentiated cost ratios

#     This function projects investment costs by \
#         multiplying the learning rates-projected NAM costs with the adjusted \
#             regionally differentiated cost ratios.

#     Parameters
#     ----------
#     nam_learning_df : pandas.DataFrame
#         Dataframe output from :func:`.project_NAM_capital_costs_using_learning_rates`
#     adj_cost_ratios_df : pandas.DataFrame
#         Dataframe output from :func:`.calculate_adjusted_region_cost_ratios`

#     Returns
#     -------
#     pandas.DataFrame
#         DataFrame with columns:
#         - scenario: SSP1, SSP2, or SSP3
#         - message_technology: MESSAGE technology name
#         - weo_technology: WEO technology name
#         - r11_region: R11 region
#         - year: values from 2020 to 2100
#         - inv_cost_learning_region: the adjusted investment cost \
#             (in units of million US$2005/yr) based on the NAM learned costs \
#             and the GDP adjusted region-differentiated cost ratios
#     """

#     df_learning_gdp_regions = (
#         nam_learning_df.merge(
#             adj_cost_ratios_df, on=["scenario", "weo_technology", "year"]
#         )
#         .assign(
#             inv_cost_learning_region=lambda x: x.inv_cost_learning_NAM
#             * x.cost_ratio_adj
#         )
#         .reindex(
#             [
#                 "scenario",
#                 "message_technology",
#                 "weo_technology",
#                 "r11_region",
#                 "year",
#                 "inv_cost_learning_region",
#             ],
#             axis=1,
#         )
#     )

#     return df_learning_gdp_regions
