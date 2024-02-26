from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from numpy.polynomial import Polynomial

if TYPE_CHECKING:
    from .config import Config


def apply_splines_to_convergence(
    df_reg: pd.DataFrame, column_name: str, config: "Config"
) -> pd.DataFrame:
    """Apply polynomial regression to convergence projections.

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
    y_predict = np.array(config.seq_years)
    y_index = pd.Index(config.seq_years, name="year")

    def _predict(df: pd.DataFrame) -> pd.Series:
        """Fit a degree-3 polynomial to `df` and predict for :attr:`.seq_years`."""
        # Fit
        p = Polynomial.fit(df.year, df[column_name], deg=3)

        # - Predict using config.seq_years.
        # - Assemble a single-column data frame with "year" as the index name.
        return pd.DataFrame({"inv_cost_splines": p(y_predict)}, index=y_index)

    # Columns for grouping and merging
    cols = ["scenario", "message_technology", "region"]

    # Columns needed from df_reg
    other_cols = ["first_technology_year", "reg_cost_base_year"]

    # - Subset data from y₀ or the convergence year or later
    # - Group by scenario, technology, and region (preserve keys).
    # - Fit a spline and predict values for all config.seq_years.
    # - Reset group keys from index to columns.
    # - Reattach `df_reg` for first_technology_year and reg_cost_base_year.
    # - Use the predicted value for periods after first_technology_year; else
    #   reg_cost_base_year.
    # - Drop intermediate columns and sort.
    return (
        df_reg.query("year == @config.y0 or year >= @config.convergence_year")
        .groupby(cols[:3], group_keys=True)
        .apply(_predict)
        .reset_index()
        .merge(df_reg[cols + other_cols].drop_duplicates(), on=cols)
        .assign(
            inv_cost_splines=lambda df: df.inv_cost_splines.where(
                df.first_technology_year < df.year, df.reg_cost_base_year
            )
        )
        .drop(other_cols, axis=1)
        .sort_values(cols + ["year"])
    )
