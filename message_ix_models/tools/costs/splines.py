from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

if TYPE_CHECKING:
    from .config import Config


# Function to apply polynomial regression to convergence costs
def apply_splines_to_convergence(
    df_reg: pd.DataFrame, column_name: str, config: "Config"
) -> pd.DataFrame:
    """Apply splines to convergence projections

    This function performs a polynomial regression on the convergence costs and returns
    the coefficients for the regression model. The regression model is then used to
    project the convergence costs for the years after the convergence year.

    The returned data have the list of periods given by :attr:`.Config.seq_years`.

    Parameters
    ----------
    df_reg : pd.DataFrame
        Dataframe containing the convergence costs
    column_name : str
        Name of the column containing the convergence costs
    config : .Config
        The code responds to:
        :attr:`~.Config.convergence_year`, and
        :attr:`~.Config.y0`.

    Returns
    -------
    df_long : pd.DataFrame
        Dataframe containing the costs with the columns:

        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost_splines: costs after applying the splines
    """

    def _poly_coeffs(df: pd.DataFrame) -> pd.Series:
        """Return polynomial coefficients fit on `df`."""
        x = df.year.values
        y = df[[column_name]].values

        # polynomial regression model
        poly = PolynomialFeatures(degree=3, include_bias=False)
        poly_features = poly.fit_transform(x.reshape(-1, 1))

        poly_reg_model = LinearRegression()
        poly_reg_model.fit(poly_features, y)

        return pd.Series(
            {
                "beta_1": poly_reg_model.coef_[0][0],
                "beta_2": poly_reg_model.coef_[0][1],
                "beta_3": poly_reg_model.coef_[0][2],
                "intercept": poly_reg_model.intercept_[0],
            }
        )

    # - Subset data from y₀ or the convergence year or later
    # - Group by scenario, technology, and region (preserve keys).
    # - Compute polynomial coefficients.
    # - Reset group keys from index to columns.
    df_out = (
        df_reg.query("year == @config.y0 or year >= @config.convergence_year")
        .groupby(["scenario", "message_technology", "region"], group_keys=True)
        .apply(_poly_coeffs)
        .reset_index()
    )

    df_wide = (
        df_reg.reindex(
            [
                "scenario",
                "message_technology",
                "region",
                "first_technology_year",
                "reg_cost_base_year",
            ],
            axis=1,
        )
        .drop_duplicates()
        .merge(df_out, on=["scenario", "message_technology", "region"])
    )

    for y in config.seq_years:
        df_wide = df_wide.assign(
            ycur=lambda x: np.where(
                y <= x.first_technology_year,
                x.reg_cost_base_year,
                (x.beta_1 * y)
                + (x.beta_2 * (y**2))
                + (x.beta_3 * (y**3))
                + x.intercept,
            )
        ).rename(columns={"ycur": y})

    df_long = df_wide.drop(
        columns=[
            "first_technology_year",
            "beta_1",
            "beta_2",
            "beta_3",
            "intercept",
            "reg_cost_base_year",
        ]
    ).melt(
        id_vars=[
            "scenario",
            "message_technology",
            "region",
        ],
        var_name="year",
        value_name="inv_cost_splines",
    )

    return df_long
