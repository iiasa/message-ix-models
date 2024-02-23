from itertools import product

import numpy as np
import pandas as pd

from .config import BASE_YEAR, FIRST_MODEL_YEAR, HORIZON_END, HORIZON_START, Config
from .gdp import adjust_cost_ratios_with_gdp
from .learning import project_ref_region_inv_costs_using_learning_rates
from .regional_differentiation import apply_regional_differentiation
from .splines import apply_splines_to_convergence


class projections:
    def __init__(self, inv_cost, fix_cost):
        self.inv_cost = inv_cost
        self.fix_cost = fix_cost


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


def create_projections_learning(config: "Config"):
    """Create cost projections using the learning method

    Parameters
    ----------
    in_module : str
        Module to use.
    in_node : str
        Spatial resolution.
    in_ref_region : str
        Reference region.
    in_base_year : int
        Base year.
    in_scenario : str
        Scenario to use.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for learning method, \
            only "Not applicable")
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    print(f"Selected scenario: {config.scenario}")
    print(
        "For the learning method, only the SSP scenario(s) itself needs to be "
        "specified. No scenario version (previous vs. updated) is needed."
    )

    print("...Calculating regional differentiation in base year+region...")
    df_region_diff = apply_regional_differentiation(config)

    print("...Applying learning rates to reference region...")
    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    df_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
            scenario_version="Not applicable",
        )
        .reindex(
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
        .drop_duplicates()
    )

    return df_costs


def create_projections_gdp(config: "Config"):
    """Create cost projections using the GDP method

    Parameters
    ----------
    in_node : str
        Spatial resolution.
    in_ref_region : str
        Reference region.
    in_base_year : int
        Base year.
    in_module : str
        Module to use.
    in_scenario : str
        Scenario to use.
    in_scenario_version : str
        Scenario version to use.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for gdp method, \
            either "Review (2023)" or "Previous (2013)"
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    # Print selection of scenario version and scenario
    print(f"Selected scenario: {config.scenario}")
    print(f"Selected scenario version: {config.scenario_version}")

    print("...Calculating regional differentiation in base year+region...")
    df_region_diff = apply_regional_differentiation(config)

    print("...Applying learning rates to reference region...")
    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    print("...Adjusting ratios using GDP data...")
    # - Compute adjustment
    # - Filter by Config.scenario, if given.
    # - Filter by Config.scenario_version, if given.
    df_adj_cost_ratios = (
        adjust_cost_ratios_with_gdp(df_region_diff, config)
        .pipe(_maybe_query_scenario, config)
        .pipe(_maybe_query_scenario_version, config)
    )

    df_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .merge(
            df_adj_cost_ratios, on=["scenario", "message_technology", "region", "year"]
        )
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio_adj,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
        )
        .reindex(
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
        .drop_duplicates()
    )

    return df_costs


def create_projections_converge(config: "Config"):
    """Create cost projections using the convergence method

    Parameters
    ----------
    - in_node : str
        Spatial resolution.
    - in_ref_region : str
        Reference region.
    - in_base_year : int
        Base year.
    - in_module : str
        Module to use.
    - in_scenario : str
        Scenario to use.
    - in_convergence_year : int
        Year to converge costs to.

    Returns
    -------
    df_costs : pd.DataFrame
        Dataframe containing the cost projections with the columns:
        - scenario_version: scenario version (for convergence method, \
            only "Not applicable")
        - scenario: scenario name (SSP1, SSP2, SSP3, SSP4, SSP5, or LED)
        - message_technology: technology name
        - region: region name
        - year: year
        - inv_cost: investment cost
        - fix_cost: fixed operating and maintenance cost
    """
    print(f"Selected scenario: {config.scenario}")
    print(f"Selected convergence year: {config.convergence_year}")
    print(
        "For the convergence method, only the SSP scenario(s) itself needs to be "
        "specified. No scenario version (previous vs. updated) is needed."
    )

    print("...Calculating regional differentiation in base year+region...")
    df_region_diff = apply_regional_differentiation(config)

    print("...Applying learning rates to reference region...")
    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff, config
    ).pipe(_maybe_query_scenario, config)

    df_pre_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .assign(
            inv_cost_converge=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                np.where(
                    x.year < config.convergence_year,
                    x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                    x.inv_cost_ref_region_learning,
                ),
            ),
        )
        .drop_duplicates()
    )

    print("...Applying splines to converge...")
    df_splines = apply_splines_to_convergence(
        df_pre_costs,
        column_name="inv_cost_converge",
        convergence_year=config.convergence_year,
    )

    df_costs = (
        df_pre_costs.merge(
            df_splines,
            on=["scenario", "message_technology", "region", "year"],
            how="outer",
        )
        .rename(columns={"inv_cost_splines": "inv_cost"})
        .assign(
            fix_cost=lambda x: x.inv_cost * x.fix_ratio,
            scenario_version="Not applicable",
        )
        .reindex(
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
        .drop_duplicates()
    )

    return df_costs


def create_message_outputs(df_projections: pd.DataFrame, fom_rate: float):
    """Create MESSAGEix outputs for investment and fixed costs.

    Parameters
    ----------
    df_projections : pd.DataFrame
        Dataframe containing the cost projections for each technology. \
            Output of func:`create_cost_projections`.
    fom_rate : float
        Rate of increase/decrease of fixed operating and maintenance costs.

    Returns
    -------
    inv: pd.DataFrame
        Dataframe containing investment costs.
    fom: pd.DataFrame
        Dataframe containing fixed operating and maintenance costs.

    """
    seq_years = list(range(HORIZON_START, HORIZON_END + 5, 5))

    df_prod = pd.DataFrame(
        product(
            df_projections.scenario_version.unique(),
            df_projections.scenario.unique(),
            df_projections.message_technology.unique(),
            df_projections.region.unique(),
            seq_years,
        ),
        columns=[
            "scenario_version",
            "scenario",
            "message_technology",
            "region",
            "year",
        ],
    )

    val_2020 = (
        df_projections.query("year == 2020")
        .rename(columns={"inv_cost": "inv_cost_2020", "fix_cost": "fix_cost_2020"})
        .drop(columns=["year"])
    )

    val_2100 = (
        df_projections.query("year == 2100")
        .drop(columns=["year"])
        .rename(columns={"inv_cost": "inv_cost_2100", "fix_cost": "fix_cost_2100"})
    )

    df_merge = (
        (
            df_prod.merge(
                val_2020,
                on=["scenario_version", "scenario", "message_technology", "region"],
            )
            .merge(
                val_2100,
                on=["scenario_version", "scenario", "message_technology", "region"],
            )
            .merge(
                df_projections,
                on=[
                    "scenario_version",
                    "scenario",
                    "message_technology",
                    "region",
                    "year",
                ],
                how="left",
            )
        )
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= BASE_YEAR, x.inv_cost_2020, x.inv_cost
            ),
            fix_cost=lambda x: np.where(
                x.year <= BASE_YEAR, x.fix_cost_2020, x.fix_cost
            ),
        )
        .assign(
            inv_cost=lambda x: np.where(x.year >= 2100, x.inv_cost_2100, x.inv_cost),
            fix_cost=lambda x: np.where(x.year >= 2100, x.fix_cost_2100, x.fix_cost),
        )
        .drop(
            columns=["inv_cost_2020", "fix_cost_2020", "inv_cost_2100", "fix_cost_2100"]
        )
        .rename(columns={"year": "year_vtg"})
        .drop_duplicates()
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
                "year_vtg",
                "value",
                "unit",
            ],
            axis=1,
        )
        .assign(
            scenario_version=lambda x: x.scenario_version.astype("string"),
            scenario=lambda x: x.scenario.astype("string"),
            node_loc=lambda x: x.node_loc.astype("string"),
            technology=lambda x: x.technology.astype("string"),
            unit=lambda x: x.unit.astype("string"),
            year_vtg=lambda x: x.year_vtg.astype(int),
            value=lambda x: x.value.astype(float),
        )
        .query("year_vtg <= 2060 or year_vtg % 10 == 0")
        .reset_index(drop=True)
        .drop_duplicates()
    )

    fom = (
        df_merge.copy()
        .drop(columns=["inv_cost"])
        .assign(key=1)
        .merge(pd.DataFrame(data={"year_act": seq_years}).assign(key=1), on="key")
        .drop(columns=["key"])
        .query("year_act >= year_vtg")
        .assign(
            val=lambda x: np.where(
                x.year_vtg <= BASE_YEAR,
                np.where(
                    x.year_act <= BASE_YEAR,
                    x.fix_cost,
                    x.fix_cost * (1 + (fom_rate)) ** (x.year_act - BASE_YEAR),
                ),
                x.fix_cost * (1 + (fom_rate)) ** (x.year_act - x.year_vtg),
            )
        )
        .assign(unit="USD/kWa")
        .rename(
            columns={
                "val": "value",
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
                "year_vtg",
                "year_act",
                "value",
                "unit",
            ],
            axis=1,
        )
        .assign(
            scenario_version=lambda x: x.scenario_version.astype("string"),
            scenario=lambda x: x.scenario.astype("string"),
            node_loc=lambda x: x.node_loc.astype("string"),
            technology=lambda x: x.technology.astype("string"),
            unit=lambda x: x.unit.astype("string"),
            year_vtg=lambda x: x.year_vtg.astype(int),
            year_act=lambda x: x.year_act.astype(int),
            value=lambda x: x.value.astype(float),
        )
        .query("year_vtg <= 2060 or year_vtg % 10 == 0")
        .query("year_act <= 2060 or year_act % 10 == 0")
        .reset_index(drop=True)
    ).drop_duplicates()

    return inv, fom


def create_iamc_outputs(msg_inv: pd.DataFrame, msg_fix: pd.DataFrame):
    """Create IAMC outputs for investment and fixed costs.

    Parameters
    ----------
    msg_inv : pd.DataFrame
        Dataframe containing investment costs in MESSAGEix format. \
            Output of func:`create_message_outputs`.
    msg_fix : pd.DataFrame
        Dataframe containing fixed operating and maintenance costs in MESSAGEix \
            format. Output of func:`create_message_outputs`.

    Returns
    -------
    iamc_inv : pd.DataFrame
        Dataframe containing investment costs in IAMC format.
    iamc_fix : pd.DataFrame
        Dataframe containing fixed operating and maintenance costs in IAMC format.
    """
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


def create_cost_projections(config: "Config") -> projections:
    """Get investment and fixed cost projections

    This is the main function to get investment and fixed cost projections. \
        It calls the other functions in this module, and returns the \
        projections in the specified format.

    Parameters
    ----------
    node : str, optional
        Spatial resolution, by default "r12". Options are "r11", "r12", and "r20"
    ref_region : str, optional
        Reference region, by default R12_NAM for R12, R11_NAM for R11, and \
            R20_NAM for R20
    base_year : int, optional
        Base year, by default BASE_YEAR specified in the config file
    module : str, optional
        Module to use, by default "base". Options are "base" and "materials"
    method : str, optional
        Method to use, by default "gdp". Options are "learning", "gdp", \
            and "convergence"
    scenario_version : str, optional
        Scenario version, by default "updated". Options are "updated" and "original"
    scenario : str, optional
        Scenario, by default "all"
    convergence_year : int, optional
        Year to converge costs to, by default 2050
    fom_rate : float, optional
        Rate of increase/decrease of fixed operating and maintenance costs, \
            by default 0.025
    format : str, optional
        Format of output, by default "message". Options are "message" and "iamc"

    Returns
    -------
    projections
        Object containing investment and fixed cost projections

    """
    # Validate configuration
    config.check()

    # Display configuration using the default __repr__ provided by @dataclass
    print(f"Selected configuration: {config!r}")

    # Select function according to `config.method`
    func = {
        "convergence": create_projections_converge,
        "gdp": create_projections_gdp,
        "learning": create_projections_learning,
    }[config.method]

    df_costs = func(config)

    if config.format == "message":
        print("...Creating MESSAGE outputs...")
        df_inv, df_fom = create_message_outputs(df_costs, fom_rate=config.fom_rate)

        return projections(df_inv, df_fom)
    elif config.format == "iamc":
        print("...Creating MESSAGE outputs first...")
        df_inv, df_fom = create_message_outputs(df_costs, fom_rate=config.fom_rate)

        print("...Creating IAMC format outputs...")
        df_inv_iamc, df_fom_iamc = create_iamc_outputs(df_inv, df_fom)

        return projections(df_inv_iamc, df_fom_iamc)
