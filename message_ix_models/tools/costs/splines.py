from itertools import product

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression  # type: ignore
from sklearn.preprocessing import PolynomialFeatures  # type: ignore

from message_ix_models.util import package_data_path

first_model_year = 2020
last_model_year = 2100
pre_last_year_rate = 0.01


def get_technology_first_year_data():
    file = package_data_path("costs", "technology_first_year.csv")
    df = pd.read_csv(file, header=3).assign(
        first_technology_year=lambda x: np.where(
            x.first_year_original > first_model_year,
            x.first_year_original,
            first_model_year,
        )
    )

    return df


def project_capital_costs_using_learning_rates(
    df_learning_rates: pd.DataFrame,
    df_region_diff: pd.DataFrame,
    df_technology_first_year: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate projected technology capital costs until 2100 using learning rates

    Parameters
    ----------
    df_learning_rates : pandas.DataFrame
        Output of `get_cost_reduction_data`
    df_region_diff : pandas.DataFrame
        Output of `get_region_differentiated_costs`
    df_technology_first_year : pandas.DataFrame
        Output of `get_technology_first_year_data`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - cost_type: the type of cost (`capital_costs` or `annual_om_costs`)
        - message_technology: technology in MESSAGEix
        - r11_region: R11 region in MESSAGEix
        - year: the year modeled (2020-2100)
        - cost_projected_learning: the cost of the technology in that region for the
        year modeled (should be between the cost in the year 2021 and the cost in
        the year 2100) based on the learning rates/cost reduction rates

    """

    # List of SSP scenarios
    scens = ["SSP1", "SSP2", "SSP3"]

    list_dfs_cost = []
    for s in scens:
        # Create manual cost reduction rates for CSP technologies
        tech_manual = pd.DataFrame(
            data={
                "message_technology": ["wind_ppf", "csp_sm1_ppl", "csp_sm3_ppl"],
                s + "_cost_reduction": [0.65, 0.56, 0.64],
            }
        )

        # Get cost reduction rates data and add manual CSP values onto it
        df_cost_reduction = (
            df_learning_rates.copy()
            .reindex(["message_technology", s + "_cost_reduction"], axis=1)
            .pipe(lambda x: pd.concat([x, tech_manual]))
            .reset_index(drop=1)
        )

        df = (
            df_region_diff.copy()
            .reindex(
                ["cost_type", "message_technology", "r11_region", "cost_region_2021"],
                axis=1,
            )
            .merge(
                df_technology_first_year.drop(columns=["first_year_original"]),
                on=["message_technology"],
                how="right",
            )
            .merge(df_cost_reduction, on=["message_technology"], how="left")
            .assign(
                cost_region_2100=lambda x: x["cost_region_2021"]
                - (x["cost_region_2021"] * x[s + "_cost_reduction"]),
                b=lambda x: (1 - pre_last_year_rate) * x.cost_region_2100,
                r=lambda x: (1 / (last_model_year - first_model_year))
                * np.log((x.cost_region_2100 - x.b) / (x.cost_region_2021 - x.b)),
            )
        )

        seq_years = list(range(first_model_year, last_model_year + 10, 10))

        for y in seq_years:
            df = df.assign(
                ycur=lambda x: np.where(
                    y <= first_model_year,
                    x.cost_region_2021,
                    (x.cost_region_2021 - x.b)
                    * np.exp(x.r * (y - x.first_technology_year))
                    + x.b,
                )
            ).rename(columns={"ycur": y})

        df = (
            df.drop(columns=["b", "r", "first_technology_year", s + "_cost_reduction"])
            .assign(ssp_scenario=s)
            .loc[lambda x: x.cost_type == "capital_costs"]
            .melt(
                id_vars=[
                    "ssp_scenario",
                    "cost_type",
                    "message_technology",
                    "r11_region",
                    "cost_region_2021",
                    "cost_region_2100",
                ],
                var_name="year",
                value_name="cost_region_projected_init",
            )
        )

        list_dfs_cost.append(df)

    df_cost = pd.concat(list_dfs_cost)

    df_adj = (
        df_cost.loc[df.r11_region == "NAM"]
        .reindex(
            [
                "ssp_scenario",
                "cost_type",
                "message_technology",
                "year",
                "cost_region_projected_init",
            ],
            axis=1,
        )
        .rename(columns={"cost_region_projected_init": "cost_region_projected_nam"})
        .merge(df_cost, on=["ssp_scenario", "cost_type", "message_technology", "year"])
        .assign(
            cost_projected_learning=lambda x: np.where(
                x.year <= 2020,
                x.cost_region_projected_init,
                x.cost_region_projected_nam,
            )
        )
        .reindex(
            [
                "ssp_scenario",
                "cost_type",
                "message_technology",
                "r11_region",
                "year",
                "cost_projected_learning",
            ],
            axis=1,
        )
    )

    return df_adj


def apply_polynominal_regression(
    df_proj_costs_learning: pd.DataFrame,
) -> pd.DataFrame:
    """Perform polynomial regression on projected costs and extract coefs/intercept

    This function applies a third degree polynominal regression on the projected
    investment costs in each region (2020-2100). The coefficients and intercept
    for each technology is saved in a dataframe.

    Parameters
    ----------
    df_proj_costs_learning : pandas.DataFrame
        Output of `project_capital_costs_using_learning_rates`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: the technology in MESSAGEix
        - r11_region: MESSAGEix R11 region
        - beta_1: the coefficient for x^1 for the specific technology
        - beta_2: the coefficient for x^2 for the specific technology
        - beta_3: the coefficient for x^3 for the specific technology
        - intercept: the intercept from the regression

    """

    un_ssp = df_proj_costs_learning.ssp_scenario.unique()
    un_tech = df_proj_costs_learning.message_technology.unique()
    un_reg = df_proj_costs_learning.r11_region.unique()

    data_reg = []
    for i, j, k in product(un_ssp, un_tech, un_reg):
        tech = df_proj_costs_learning.loc[
            (df_proj_costs_learning.ssp_scenario == i)
            & (df_proj_costs_learning.message_technology == j)
            & (df_proj_costs_learning.r11_region == k)
        ]

        if tech.size == 0:
            continue

        x = tech.year.values
        y = tech.cost_projected_learning.values

        # polynomial regression model
        poly = PolynomialFeatures(degree=3, include_bias=False)
        poly_features = poly.fit_transform(x.reshape(-1, 1))

        poly_reg_model = LinearRegression()
        poly_reg_model.fit(poly_features, y)

        data = [
            [
                i,
                j,
                k,
                poly_reg_model.coef_[0],
                poly_reg_model.coef_[1],
                poly_reg_model.coef_[2],
                poly_reg_model.intercept_,
            ]
        ]
        df = pd.DataFrame(
            data,
            columns=[
                "ssp_scenario",
                "message_technology",
                "r11_region",
                "beta_1",
                "beta_2",
                "beta_3",
                "intercept",
            ],
        )

        data_reg.append(df)

    df_regression = pd.concat(data_reg).reset_index(drop=1)

    return df_regression


def project_costs_using_splines(
    input_df_region_diff,
    input_df_technology_first_year,
    input_df_poly_reg,
    input_df_learning_projections,
    input_df_fom_inv_ratios,
):
    df = (
        input_df_region_diff.loc[input_df_region_diff.cost_type == "capital_costs"]
        .reindex(
            ["cost_type", "message_technology", "r11_region", "cost_region_2021"],
            axis=1,
        )
        .merge(
            input_df_technology_first_year.drop(columns=["first_year_original"]),
            on=["message_technology"],
            how="right",
        )
        .merge(input_df_poly_reg, on=["message_technology", "r11_region"])
    )

    seq_years = list(range(first_model_year, last_model_year + 10, 10))
    for y in seq_years:
        df = df.assign(
            ycur=lambda x: np.where(
                y <= x.first_technology_year,
                x.cost_region_2021,
                (x.beta_1 * y)
                + (x.beta_2 * (y**2))
                + (x.beta_3 * (y**3))
                + x.intercept,
            )
        ).rename(columns={"ycur": y})

    df_long = (
        df.drop(
            columns=["first_technology_year", "beta_1", "beta_2", "beta_3", "intercept"]
        )
        .melt(
            id_vars=[
                "cost_type",
                "ssp_scenario",
                "message_technology",
                "r11_region",
                "cost_region_2021",
            ],
            var_name="year",
            value_name="cost_projected_splines",
        )
        .merge(
            input_df_learning_projections,
            on=[
                "cost_type",
                "ssp_scenario",
                "message_technology",
                "r11_region",
                "year",
            ],
        )
        .assign(
            inv_cost=lambda x: np.where(
                x.r11_region == "NAM",
                x.cost_projected_learning,
                x.cost_projected_splines,
            )
        )
        .merge(input_df_fom_inv_ratios, on=["message_technology", "r11_region"])
        .assign(fix_cost=lambda x: x.inv_cost * x.fom_to_inv_cost_ratio)
        .reindex(
            [
                "ssp_scenario",
                "message_technology",
                "r11_region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
    )

    return df_long
