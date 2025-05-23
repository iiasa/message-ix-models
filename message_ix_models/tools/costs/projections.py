import logging
from collections.abc import Mapping
from itertools import product

import numpy as np
import pandas as pd
from numpy.polynomial import Polynomial

from .config import Config
from .decay import project_ref_region_inv_costs_using_reduction_rates
from .gdp import adjust_cost_ratios_with_gdp
from .regional_differentiation import apply_regional_differentiation

log = logging.getLogger(__name__)


def smaller_than(sequence, value):
    return [item for item in sequence if item < value]


def larger_than(sequence, value):
    return [item for item in sequence if item > value]


def _maybe_query_scenario(df: pd.DataFrame, config: "Config") -> pd.DataFrame:
    """Filter `df` for :attr`.Config.scenario`, if any is specified."""
    if config.scenario == "all":
        scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]  # noqa: F841
        return df.query("scenario in @scen")
    elif config.scenario is not None:
        return df.query(f"scenario == {config.scenario.upper()!r}")
    else:
        return df


def _maybe_query_scenario_version(df: pd.DataFrame, config: "Config") -> pd.DataFrame:
    """Filter `df` for :attr`.Config.scenario_version`, if any is specified."""
    if config.scenario_version is None:
        return df

    # NB "all" does not appear in Config
    scen_vers = {  # noqa: F841
        "all": ["Review (2023)", "Previous (2013)"],
        "updated": ["Review (2023)"],
        "original": ["Previous (2013)"],
    }[config.scenario_version]

    return df.query("scenario_version in @scen_vers")


def create_projections_constant(config: "Config"):
    """Create cost projections using assuming constant regional cost ratios.

    Parameters
    ----------
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.module`,
        :attr:`~.Config.node`,
        :attr:`~.Config.ref_region`, and
        :attr:`~.Config.scenario`.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for constant method, only
          "Not applicable")
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    log.info(f"Selected scenario: {config.scenario}")
    log.info(
        "For the constant method, only the SSP scenario(s) itself needs to be "
        "specified. No scenario version (previous vs. updated) is needed."
    )

    log.info("Calculate regional differentiation in base year+region")
    df_region_diff = apply_regional_differentiation(config)

    log.info("Apply cost reduction rates rates to reference region")
    df_ref_reg_decay = project_ref_region_inv_costs_using_reduction_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    df_costs = (
        df_region_diff.merge(df_ref_reg_decay, on="message_technology")
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= config.base_year,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_decay * x.reg_cost_ratio,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
            scenario_version="Not applicable",
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "first_technology_year",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
        .drop_duplicates()
    )

    return df_costs


def create_projections_gdp(config: "Config"):
    """Create cost projections using the GDP method.

    Parameters
    ----------
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.module`,
        :attr:`~.Config.node`,
        :attr:`~.Config.ref_region`,
        :attr:`~.Config.scenario`, and
        :attr:`~.Config.scenario_version`.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for gdp method, either "Review (2023)" or
          "Previous (2013)"
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    # Print selection of scenario version and scenario
    log.info(f"Selected scenario: {config.scenario}")
    log.info(f"Selected scenario version: {config.scenario_version}")

    log.info("Calculate regional differentiation in base year+region")
    df_region_diff = apply_regional_differentiation(config)

    log.info("Apply cost reduction rates to reference region")
    df_ref_reg_reduction = project_ref_region_inv_costs_using_reduction_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    log.info("Adjust ratios using GDP data")
    # - Compute adjustment
    # - Filter by Config.scenario, if given.
    # - Filter by Config.scenario_version, if given.
    df_adj_cost_ratios = (
        adjust_cost_ratios_with_gdp(df_region_diff, config)
        .pipe(_maybe_query_scenario, config)
        .pipe(_maybe_query_scenario_version, config)
    )

    df_costs = (
        df_region_diff.merge(df_ref_reg_reduction, on="message_technology")
        .merge(
            df_adj_cost_ratios, on=["scenario", "message_technology", "region", "year"]
        )
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= config.base_year,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_decay * x.reg_cost_ratio_adj,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "first_technology_year",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
        .drop_duplicates()
    )

    return df_costs


def create_projections_converge(config: "Config"):
    """Create cost projections using the convergence method.

    Parameters
    ----------
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.convergence_year`,
        :attr:`~.Config.module`,
        :attr:`~.Config.node`,
        :attr:`~.Config.ref_region`, and
        :attr:`~.Config.scenario`.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for convergence method, only "Not
          applicable")
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    log.info(f"Selected scenario: {config.scenario}")
    log.info(f"Selected convergence year: {config.convergence_year}")
    log.info(
        "For the convergence method, only the SSP scenario(s) itself needs to be "
        "specified. No scenario version (previous vs. updated) is needed."
    )

    log.info("Calculate regional differentiation in base year+region")
    df_region_diff = apply_regional_differentiation(config)

    log.info("Apply cost reduction rates to reference region")
    df_ref_reg_cost_reduction = project_ref_region_inv_costs_using_reduction_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    df_tmp_costs = (
        df_region_diff.merge(df_ref_reg_cost_reduction, on="message_technology")
        .assign(
            inv_cost_tmp=lambda x: np.where(
                x.year <= config.base_year,
                x.reg_cost_base_year,
                np.where(
                    x.year < config.convergence_year,
                    x.inv_cost_ref_region_decay * x.reg_cost_ratio,
                    x.inv_cost_ref_region_decay,
                ),
            ),
        )
        .drop_duplicates()
    )

    y_predict = np.array(config.seq_years)
    y_index = pd.Index(config.seq_years, name="year")

    def _predict(df: pd.DataFrame) -> pd.Series:
        """Fit a degree-3 polynomial to `df` and predict for :attr:`.seq_years`."""
        # Fit
        p = Polynomial.fit(df.year, df.inv_cost_tmp, deg=1)

        # - Predict using config.seq_years.
        # - Assemble a single-column data frame with "year" as the index name.
        return pd.DataFrame({"inv_pre_converge_decay": p(y_predict)}, index=y_index)

    # Columns for grouping and merging
    cols = ["scenario", "message_technology", "region"]

    # Apply polynomial regression to costs at base year and convergence year
    # (interpolating)
    df_pre_converge_costs = (
        df_tmp_costs.query(
            "year == @config.base_year or year == @config.convergence_year"
        )
        .groupby(cols[:3], group_keys=True)
        .apply(_predict)
        .reset_index()
    )

    # Get final investment costs
    df_inv_costs_final = (
        df_tmp_costs.merge(
            df_pre_converge_costs,
            on=["scenario", "message_technology", "region", "year"],
        )
        .assign(
            inv_cost_converge=lambda x: np.where(
                x.year <= config.base_year,
                x.reg_cost_base_year,
                np.where(
                    x.region == config.ref_region,
                    x.inv_cost_ref_region_decay,
                    np.where(
                        x.year < config.convergence_year,
                        x.inv_pre_converge_decay,
                        x.inv_cost_ref_region_decay,
                    ),
                ),
            ),
        )
        .drop_duplicates()
    )

    # Get fixed O&M costs
    df_costs = (
        df_inv_costs_final.rename(columns={"inv_cost_converge": "inv_cost"})
        .assign(
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
            scenario_version="Not applicable",
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "first_technology_year",
                "region",
                "year",
                "inv_cost",
                "fix_cost",
            ],
            axis=1,
        )
        .drop_duplicates()
    )

    return df_costs


def create_message_outputs(
    df_projections: pd.DataFrame, config: "Config"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create MESSAGEix outputs for investment and fixed costs.

    The returned data have the model periods given by :attr:`.Config.Y`.

    Parameters
    ----------
    df_projections : pd.DataFrame
        Dataframe containing the cost projections for each technology.
        Output of func:`create_cost_projections`.
    config : .Config
        The function responds to the fields
        :attr:`~.Config.fom_rate` and
        :attr:`~.Config.Y`.

    Returns
    -------
    inv: pd.DataFrame
        Dataframe containing investment costs.
    fom: pd.DataFrame
        Dataframe containing fixed operating and maintenance costs.

    """
    log.info("Convert {fix,inv}_cost data to MESSAGE structure")

    y_base = config.base_year

    dims = [
        "scenario_version",
        "scenario",
        "message_technology",
        "first_technology_year",
        "region",
        "year",
    ]

    df_prod = pd.DataFrame(
        product(
            df_projections.scenario_version.unique(),
            df_projections.scenario.unique(),
            df_projections.message_technology.unique(),
            df_projections.first_technology_year.unique(),
            df_projections.region.unique(),
            config.seq_years,
        ),
        columns=dims,
    )

    val_2020 = (
        df_projections.query("year == 2020")
        .rename(columns={"inv_cost": "inv_cost_2020", "fix_cost": "fix_cost_2020"})
        .drop(columns=["year"])
    )

    val_fin_proj_year = (
        df_projections.query("year == @config.final_projection_year")
        .drop(columns=["year"])
        .rename(
            columns={
                "inv_cost": "inv_cost_fin_proj_year",
                "fix_cost": "fix_cost_fin_proj_year",
            }
        )
    )

    df_merge = (
        (
            df_prod.merge(val_2020, on=dims[:-1])
            .merge(val_fin_proj_year, on=dims[:-1])
            .merge(df_projections, on=dims, how="left")
        )
        .assign(
            inv_cost=lambda x: np.where(x.year <= y_base, x.inv_cost_2020, x.inv_cost),
            fix_cost=lambda x: np.where(x.year <= y_base, x.fix_cost_2020, x.fix_cost),
        )
        .assign(
            # NOTE: This portion carries over the final projection year values to
            # the final mdoel year.
            # This is applicable in the case where
            # `attr`:Config.final_model_year > `attr`:Config.final_projection_year.
            inv_cost=lambda x: np.where(
                x.year >= config.final_projection_year,
                x.inv_cost_fin_proj_year,
                x.inv_cost,
            ),
            fix_cost=lambda x: np.where(
                x.year >= config.final_projection_year,
                x.fix_cost_fin_proj_year,
                x.fix_cost,
            ),
        )
        .drop(
            columns=[
                "inv_cost_2020",
                "fix_cost_2020",
                "inv_cost_fin_proj_year",
                "fix_cost_fin_proj_year",
            ]
        )
        .rename(columns={"year": "year_vtg"})
        .drop_duplicates()
    )

    dtypes = dict(
        scenario_version=str,
        scenario=str,
        node_loc=str,
        technology=str,
        first_technology_year=str,
        unit=str,
        year_vtg=int,
        value=float,
    )

    inv = (
        df_merge.copy()
        .assign(unit="USD/kWa")
        .rename(
            columns={
                "inv_cost": "value",
                "message_technology": "technology",
                "region": "node_loc",
            }
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "node_loc",
                "technology",
                "first_technology_year",
                "year_vtg",
                "value",
                "unit",
            ],
            axis=1,
        )
        .astype(dtypes)
        .query("year_vtg in @config.Y")
        .astype({"first_technology_year": float})  # has to be float; int gives error
        .query("year_vtg >= first_technology_year")
        .reset_index(drop=True)
        .drop_duplicates()
        .drop("first_technology_year", axis=1)
    )

    dtypes.update(year_act=int)

    to_merge = pd.DataFrame(
        {"year_act" if config.use_vintages else "year_vtg": config.seq_years}
    ).assign(key=1)

    def _compute_value(df: pd.DataFrame) -> pd.Series:
        if not config.use_vintages:
            return df.fix_cost

        rate = 1.0 + config.fom_rate

        return np.where(
            df.year_vtg <= y_base,
            np.where(
                df.year_act <= y_base,
                df.fix_cost,
                # NB if fom_rate was 0, the latter terms collapse to 1.0 ** (…) = 1.0
                df.fix_cost * rate ** (df.year_act - y_base),
            ),
            df.fix_cost * rate ** (df.year_act - df.year_vtg),
        )

    fom = (
        df_merge.copy()
        .drop(columns=["inv_cost"])
        .rename(columns={"year_vtg": "year_vtg" if config.use_vintages else "year_act"})
        .assign(key=1)
        .merge(to_merge, on="key")
        .drop(columns=["key"])
        .query("year_act >= year_vtg")
        .assign(value=_compute_value, unit="USD/kWa")
        .rename(columns={"message_technology": "technology", "region": "node_loc"})
        .reindex(
            [
                "scenario_version",
                "scenario",
                "node_loc",
                "technology",
                "first_technology_year",
                "year_vtg",
                "year_act",
                "value",
                "unit",
            ],
            axis=1,
        )
        .astype(dtypes)
        .query("year_act in @config.Y and year_vtg in @config.Y")
        .astype({"first_technology_year": float})  # has to be float; int gives error
        .query("year_vtg >= first_technology_year")
        .reset_index(drop=True)
        .drop_duplicates()
        .drop("first_technology_year", axis=1)
    )

    return inv, fom


def create_iamc_outputs(
    msg_inv: pd.DataFrame, msg_fix: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create IAMC outputs for investment and fixed costs.

    Parameters
    ----------
    msg_inv : pd.DataFrame
        Dataframe containing investment costs in MESSAGEix format.
        Output of func:`create_message_outputs`.
    msg_fix : pd.DataFrame
        Dataframe containing fixed operating and maintenance costs in MESSAGEix format.
        Output of func:`create_message_outputs`.

    Returns
    -------
    iamc_inv : pd.DataFrame
        Dataframe containing investment costs in IAMC format.
    iamc_fix : pd.DataFrame
        Dataframe containing fixed operating and maintenance costs in IAMC format.
    """
    log.info("Convert {fix,inv}_cost data to IAMC structure")

    iamc_inv = (
        (
            msg_inv.assign(
                Variable=lambda x: "Capital Cost|Electricity|" + x.technology,
            )
            .rename(
                columns={
                    "scenario_version": "SSP_Scenario_Version",
                    "scenario": "SSP_Scenario",
                    "year_vtg": "Year",
                    "node_loc": "Region",
                    "unit": "Unit",
                }
            )
            .drop(columns=["technology"])
        )
        .pivot_table(
            index=[
                "SSP_Scenario_Version",
                "SSP_Scenario",
                "Region",
                "Variable",
                "Unit",
            ],
            columns="Year",
            values="value",
        )
        .reset_index()
        .rename_axis(None, axis=1)
        .drop_duplicates()
    )

    iamc_fix = (
        (
            msg_fix.assign(
                Variable=lambda x: "OM Cost|Electricity|"
                + x.technology
                + "|Vintage="
                + x.year_vtg.astype(str),
            )
            .rename(
                columns={
                    "scenario_version": "SSP_Scenario_Version",
                    "scenario": "SSP_Scenario",
                    "year_act": "Year",
                    "node_loc": "Region",
                    "unit": "Unit",
                }
            )
            .drop(columns=["technology", "year_vtg"])
        )
        .pivot_table(
            index=[
                "SSP_Scenario_Version",
                "SSP_Scenario",
                "Region",
                "Variable",
                "Unit",
            ],
            columns="Year",
            values="value",
        )
        .reset_index()
        .rename_axis(None, axis=1)
        .drop_duplicates()
    )

    return iamc_inv, iamc_fix


def create_cost_projections(config: "Config") -> Mapping[str, pd.DataFrame]:
    """Get investment and fixed cost projections.

    This is the main function to get investment and fixed cost projections. It calls the
    other functions in this module, and returns the projections in the specified format.

    Parameters
    ----------
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.convergence_year`,
        :attr:`~.Config.fom_rate`,
        :attr:`~.Config.format`,
        :attr:`~.Config.method`,
        :attr:`~.Config.module`,
        :attr:`~.Config.node`,
        :attr:`~.Config.ref_region`,
        :attr:`~.Config.scenario`, and
        :attr:`~.Config.scenario_version`.

    Returns
    -------
    dict
        Keys are "fix_cost" and "inv_cost", each mapped to a
        :class:`~.pandas.DataFrame`.

        If :attr:`.Config.format` is "message", the data frames have the same columns as
        required by :mod:`message_ix` for the respective parameter—for instance, the
        columns given by :py:`make_df("fix_cost", ...)`—*plus* columns named "scenario"
        and "scenario_version".
    """
    # Validate configuration
    config.check()

    # Display configuration using the default __repr__ provided by @dataclass
    log.info(f"Configuration: {config!r}")

    # Select function according to `config.method`
    func = {
        "convergence": create_projections_converge,
        "gdp": create_projections_gdp,
        "constant": create_projections_constant,
    }[config.method]

    # Create projections
    df_costs = func(config)

    # Convert to MESSAGEix format
    df_inv, df_fom = create_message_outputs(df_costs, config)

    if config.format == "iamc":
        df_inv, df_fom = create_iamc_outputs(df_inv, df_fom)

    return {"inv_cost": df_inv, "fix_cost": df_fom}
