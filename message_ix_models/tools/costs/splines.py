import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

from message_ix_models.util import package_data_path

first_model_year = 2020
last_model_year = 2100
pre_last_year_rate = 0.01


def get_technology_first_year_data():
    file = package_data_path("costs", "technology_first_year.csv")
    df = pd.read_csv(file, header=3)

    return df


def calculate_NAM_projected_capital_costs(
    df_learning_rates: pd.DataFrame,
    df_region_diff: pd.DataFrame,
    df_technology_first_year: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate projected capital costs for NAM region until 2100

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
        - cost_region_2021: the cost of that technology in that region in the \
            year 2021 (from WEO data)
        - cost_region_2100: the projected cost of the technology in that region \
            in the year 2100 (based on SSP learning rate)
        - year: the year modeled (2020-2100)
        - cost_region_projected: the cost of the technology in that region for the
        year modeled (should be between the cost in the year 2021 and the cost in
        the year 2100)

    """

    # List of SSP scenarios
    scens = ["SSP1", "SSP1", "SSP3"]
    s = scens[0]

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
        .merge(df_technology_first_year, on=["message_technology"], how="right")
        .assign(
            first_technology_year=lambda x: np.where(
                x.first_year_original > first_model_year,
                x.first_year_original,
                first_model_year,
            )
        )
        .drop(columns=["first_year_original"])
        .merge(df_cost_reduction, on=["message_technology"], how="left")
        .assign(
            cost_region_2100=lambda x: x["cost_region_2021"]
            - (x["cost_region_2021"] * x[s + "_cost_reduction"]),
            b=lambda x: (1 - pre_last_year_rate) * x.cost_region_2100,
            r=lambda x: (1 / (last_model_year - first_model_year))
            * np.log((x.cost_region_2100 - x.b) / (x.cost_region_2021 - x.b)),
        )
        .loc[lambda x: x["r11_region"] == "NAM"]
    )

    seq_years = list(range(first_model_year, last_model_year + 10, 10))

    for y in seq_years:
        df = df.assign(
            ycur=lambda x: np.where(
                y <= first_model_year,
                x.cost_region_2021,
                (x.cost_region_2021 - x.b) * np.exp(x.r * (y - x.first_technology_year))
                + x.b,
            )
        ).rename(columns={"ycur": y})

    df = (
        df.drop(columns=["b", "r", "first_technology_year", s + "_cost_reduction"])
        .loc[lambda x: x.cost_type == "capital_costs"]
        .melt(
            id_vars=[
                "cost_type",
                "message_technology",
                "r11_region",
                "cost_region_2021",
                "cost_region_2100",
            ],
            var_name="year",
            value_name="cost_region_projected",
        )
    )

    return df


def apply_polynominal_regression_NAM_costs(df_nam_costs: pd.DataFrame) -> pd.DataFrame:
    """Perform polynomial regression on NAM projected costs and extract coefs/intercept

    This function applies a third degree polynominal regression on the projected
    investment costs in the NAM region (2020-2100). The coefficients and intercept
    for each technology is saved in a dataframe.

    Parameters
    ----------
    df_nam_costs : pandas.DataFrame
        Output of `calculate_NAM_projected_capital_costs`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: the technology in MESSAGEix
        - beta_1: the coefficient for x^1 for the specific technology
        - beta_2: the coefficient for x^2 for the specific technology
        - beta_3: the coefficient for x^3 for the specific technology
        - intercept: the intercept from the regression

    """

    un_tech = df_nam_costs.message_technology.unique()

    i = "gas_cc"

    data_reg = []
    for i in un_tech:
        tech = df_nam_costs.loc[df_nam_costs.message_technology == i]

        x = tech.year.values
        y = tech.cost_region_projected.values

        # polynomial regression model
        poly = PolynomialFeatures(degree=3, include_bias=False)
        poly_features = poly.fit_transform(x.reshape(-1, 1))

        poly_reg_model = LinearRegression()
        poly_reg_model.fit(poly_features, y)

        data = [
            [
                i,
                poly_reg_model.coef_[0],
                poly_reg_model.coef_[1],
                poly_reg_model.coef_[2],
                poly_reg_model.intercept_,
            ]
        ]
        df = pd.DataFrame(
            data,
            columns=["message_technology", "beta_1", "beta_2", "beta_3", "intercept"],
        )

        data_reg.append(df)

    df_regression = pd.concat(data_reg).reset_index(drop=1)

    return df_regression
