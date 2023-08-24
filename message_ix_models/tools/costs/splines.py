from itertools import product

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

from message_ix_models.tools.costs.config import (
    FIRST_MODEL_YEAR,
    LAST_MODEL_YEAR,
    TIME_STEPS,
)


# Function to apply polynomial regression to convergence costs
def apply_splines_to_convergence(
    input_df: pd.DataFrame,
    column_name,
    input_convergence_year,
):
    """Apply polynomial regression and splines to convergence"""

    # un_vers = input_df.scenario_version.unique()
    un_ssp = input_df.scenario.unique()
    un_tech = input_df.message_technology.unique()
    un_reg = input_df.region.unique()

    data_reg = []
    for i, j, k in product(un_ssp, un_tech, un_reg):
        tech = input_df.query(
            "scenario == @i and message_technology == @j \
                and region == @k"
        ).query("year == @FIRST_MODEL_YEAR or year >= @input_convergence_year")

        if tech.size == 0:
            continue

        x = tech.year.values
        y = tech[[column_name]].values

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
                poly_reg_model.coef_[0][0],
                poly_reg_model.coef_[0][1],
                poly_reg_model.coef_[0][2],
                poly_reg_model.intercept_[0],
            ]
        ]

        df = pd.DataFrame(
            data,
            columns=[
                "scenario",
                "message_technology",
                "region",
                "beta_1",
                "beta_2",
                "beta_3",
                "intercept",
            ],
        )

        data_reg.append(df)

    df_reg = pd.concat(data_reg).reset_index(drop=1)
    df_wide = (
        input_df.reindex(
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
        .merge(df_reg, on=["scenario", "message_technology", "region"])
    )

    seq_years = list(range(FIRST_MODEL_YEAR, LAST_MODEL_YEAR + TIME_STEPS, TIME_STEPS))

    for y in seq_years:
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


# Function to project investment costs
# using learning rates, GDP adjusted cost ratios, and convergence
# to a single value
def project_all_inv_costs(
    reg_diff_df: pd.DataFrame,
    ref_reg_learning_df: pd.DataFrame,
    gdp_adj_ratios_df: pd.DataFrame,
    input_convergence_year,
    input_scenario_version,
    input_scenario,
) -> pd.DataFrame:
    """Project investment costs using all methods

    Use three different methods to calculate investment costs:
    - Learning rates
    - GDP adjusted cost ratios
    - Convergence to a single value

    Parameters
    ----------
    reg_diff_df : pandas.DataFrame
        Output of :func:`.get_weo_region_differentiated_costs`
    ref_reg_learning_df : pandas.DataFrame
        Output of :func:`.project_ref_region_inv_costs_using_learning_rates`
    gdp_adj_ratios_df : pandas.DataFrame
        Output of :func:`.calculate_gdp_adjusted_region_cost_ratios`
    input_convergence_year : int, optional
        The year to converge to a single value, by default 2050
    input_scenario_version : str, optional
        If want to subset by scenario version, by default None
        Valid options are: "all", "updated", "original"
    input_scenario : str, optional
        If want to subset by scenario, by default None
        Valid options are: "all", "ssp1", "ssp2", "ssp3", "ssp4", "ssp5"

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario_version: the scenario version (Review (2023) or Previous (2013))
        - scenario: the SSP scenario
        - message_technology: the technology in MESSAGEix
        - region: the region in MESSAGEix
        - year: the year modeled (2020-2100)
        - reference_region: the reference region
        - reg_cost_base_year: the investment cost in the reference region \
            in the base year
        - reg_cost_ratio: the ratio of the investment cost in the each region \
            to the investment cost in the reference region
        - reg_cost_ratio_adj: the ratio of the investment cost in the each region \
            to the investment cost in the reference region, adjusted for GDP
        - fix_to_inv_cost_ratio: the ratio of the fixed O&M cost to the \
            investment cost
        - first_technology_year: the first year the technology is deployed
        - inv_cost_ref_region_learning: the investment cost in the reference \
            region using learning rates
        - inv_cost_learning_only: the investment cost in each region \
            using learning rates
        - inv_cost_gdp_adj: the investment cost in the each region \
            using learning rates and GDP adjusted cost ratios
        - inv_cost_converge: the investment cost in the each region \
            applying a convergence year and reference region (but no splines)
        - inv_cost_splines: the investment cost in the each region \
            after applying a polynomial regression and splines to convergence
    """

    # If no scenario version is specified, do not filter for scenario version
    # If it specified, then filter as below:
    if input_scenario_version is not None:
        if input_scenario_version == "all":
            sel_scen_vers = ["Review (2023)", "Previous (2013)"]
        elif input_scenario_version == "updated":
            sel_scen_vers = ["Review (2023)"]
        elif input_scenario_version == "original":
            sel_scen_vers = ["Previous (2013)"]

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if input_scenario is not None:
        if input_scenario == "all":
            sel_scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        else:
            sel_scen = input_scenario.upper()

    # Repeating to avoid linting error
    sel_scen_vers = sel_scen_vers
    sel_scen = sel_scen

    # Merge dataframes
    df_reg_costs = (
        reg_diff_df.merge(ref_reg_learning_df, on="message_technology")
        .merge(
            gdp_adj_ratios_df, on=["scenario", "message_technology", "region", "year"]
        )
        .assign(
            inv_cost_learning_only=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
            ),
            inv_cost_gdp_adj=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio_adj,
            ),
            inv_cost_converge=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                np.where(
                    x.year < input_convergence_year,
                    x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                    x.inv_cost_ref_region_learning,
                ),
            ),
        )
    )

    if input_scenario_version is not None or input_scenario is not None:
        df_reg_costs = df_reg_costs.query(
            "scenario_version == @sel_scen_vers and scenario == @sel_scen"
        )

    df_splines = apply_splines_to_convergence(
        df_reg_costs,
        column_name="inv_cost_converge",
        input_convergence_year=input_convergence_year,
    )

    df_inv_fom = df_reg_costs.merge(
        df_splines,
        on=["scenario_version", "scenario", "message_technology", "region", "year"],
        how="outer",
    ).reindex(
        [
            "scenario_version",
            "scenario",
            "message_technology",
            "region",
            "year",
            "reference_region",
            "reg_cost_base_year",
            "reg_cost_ratio",
            "reg_cost_ratio_adj",
            "fix_to_inv_cost_ratio",
            "first_technology_year",
            "inv_cost_ref_region_learning",
            "inv_cost_learning_only",
            "inv_cost_gdp_adj",
            "inv_cost_converge",
            "inv_cost_splines",
        ],
        axis=1,
    )

    return df_inv_fom


# Function to project final investment costs and FOM costs
# based on specified method
def get_final_inv_and_fom_costs(
    inv_costs_df: pd.DataFrame, input_method: str = "convergence"
):
    """Get final investment and FOM costs based on specified method

    Parameters
    ----------
    inv_costs_df : pandas.DataFrame
        Output of :func:`project_all_inv_costs`
    input_method : str, optional
        Method to use to project costs, by default "convergence"
        Valid options are: "learning", "gdp", "convergence"

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario_version: the scenario version (Review (2023) or Previous (2013))
        - scenario: the SSP scenario
        - message_technology: the technology in MESSAGEix
        - region: MESSAGEix region
        - year: the year modeled (2020-2100)
        - inv_cost: the investment cost in units of USD/kW
        - fix_cost: the fixed O&M cost in units of USD/kW
    """

    df = inv_costs_df.assign(
        inv_cost=lambda x: np.where(
            input_method == "learning",
            x.inv_cost_learning_only,
            np.where(input_method == "gdp", x.inv_cost_gdp_adj, x.inv_cost_splines),
        ),
        fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
    ).reindex(
        [
            "scenario_version",
            "scenario",
            "message_technology",
            "region",
            "year",
            "inv_cost",
            "fix_cost",
        ],
        axis=1,
    )

    return df

    # if input_method == "learning":
    #     df = get_cost_projections(
    #         cost_type="inv_cost",
    #         scenario="ssp2",
    #         format="message",
    #         converge_costs=False,
    #         use_gdp=False,
    #     ).assign(type="Learning", convergence_year=np.NaN)
    # elif input_method == "gdp":
    #     df = get_cost_projections(
    #         cost_type="inv_cost",
    #         scenario="ssp2",
    #         format="message",
    #         converge_costs=False,
    #         use_gdp=True,
    #     ).assign(type="GDP", convergence_year=np.NaN)
    # elif input_method == "convergence":
    #     df = get_cost_projections(
    #         cost_type="inv_cost",
    #         scenario="ssp2",
    #         format="message",
    #         converge_costs=True,
    #         use_gdp=False,
    #     ).assign(type="Convergence", convergence_year=2050)
    # else:
    #     raise ValueError("Invalid method specified")

    # return df
