from itertools import product

import numpy as np
import pandas as pd

from message_ix_models.tools.costs.config import (
    BASE_YEAR,
    FIRST_MODEL_YEAR,
    HORIZON_END,
    HORIZON_START,
)
from message_ix_models.tools.costs.gdp import (
    calculate_indiv_adjusted_region_cost_ratios,
)
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    get_weo_region_differentiated_costs,
)
from message_ix_models.tools.costs.splines import apply_splines_to_convergence


class projections:
    def __init__(self, inv_cost, fix_cost):
        self.inv_cost = inv_cost
        self.fix_cost = fix_cost


def smaller_than(sequence, value):
    return [item for item in sequence if item < value]


def larger_than(sequence, value):
    return [item for item in sequence if item > value]


def create_projections_learning(
    in_node, in_ref_region, in_base_year, in_module, in_scenario
):
    print("Selected scenario: " + in_scenario)
    print(
        "For the learning method, only the SSP scenario(s) itself \
            needs to be specified. \
        No scenario version (previous vs. updated) is needed."
    )

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
        else:
            scen = in_scenario.upper()

    # Repeating to avoid linting error
    scen = scen

    df_region_diff = get_weo_region_differentiated_costs(
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @scen")

    df_costs = (
        df_region_diff.merge(df_ref_reg_learning, on="message_technology")
        .assign(
            inv_cost=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
            ),
            fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
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
    )

    return df_costs


def create_projections_gdp(
    in_node, in_ref_region, in_base_year, in_module, in_scenario, in_scenario_version
):
    # Print selection of scenario version and scenario
    print("Selected scenario: " + in_scenario)
    print("Selected scenario version: " + in_scenario_version)

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
        else:
            scen = in_scenario.upper()

    # If no scenario version is specified, do not filter for scenario version
    # If it specified, then filter as below:
    if in_scenario_version is not None:
        if in_scenario_version == "all":
            scen_vers = ["Review (2023)", "Previous (2013)"]
        elif in_scenario_version == "updated":
            scen_vers = ["Review (2023)"]
        elif in_scenario_version == "original":
            scen_vers = ["Previous (2013)"]

    # Repeating to avoid linting error
    scen = scen
    scen_vers = scen_vers

    df_region_diff = get_weo_region_differentiated_costs(
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    df_adj_cost_ratios = calculate_indiv_adjusted_region_cost_ratios(
        df_region_diff,
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @scen")
        df_adj_cost_ratios = df_adj_cost_ratios.query(
            "scenario_version == @scen_vers and scenario == @scen"
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
            fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
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
    )

    return df_costs


def create_projections_converge(
    in_node, in_ref_region, in_base_year, in_module, in_scenario, in_convergence_year
):
    print("Selected scenario: " + in_scenario)
    print("Selected convergence year: " + str(in_convergence_year))
    print(
        "For the convergence method, only the SSP scenario(s) itself \
        needs to be specified. \
        No scenario version (previous vs. updated) is needed."
    )

    # If no scenario is specified, do not filter for scenario
    # If it specified, then filter as below:
    if in_scenario is not None:
        if in_scenario == "all":
            scen = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
        else:
            scen = in_scenario.upper()

    # Repeating to avoid linting error
    scen = scen

    df_region_diff = get_weo_region_differentiated_costs(
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        node=in_node,
        ref_region=in_ref_region,
        base_year=in_base_year,
        module=in_module,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @scen")

    df_pre_costs = df_region_diff.merge(
        df_ref_reg_learning, on="message_technology"
    ).assign(
        inv_cost_converge=lambda x: np.where(
            x.year <= FIRST_MODEL_YEAR,
            x.reg_cost_base_year,
            np.where(
                x.year < in_convergence_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                x.inv_cost_ref_region_learning,
            ),
        ),
    )

    df_splines = apply_splines_to_convergence(
        df_pre_costs,
        column_name="inv_cost_converge",
        convergence_year=in_convergence_year,
    )

    df_costs = (
        df_pre_costs.merge(
            df_splines,
            on=["scenario", "message_technology", "region", "year"],
            how="outer",
        )
        .rename(columns={"inv_cost_splines": "inv_cost"})
        .assign(
            fix_cost=lambda x: x.inv_cost * x.fix_to_inv_cost_ratio,
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
    )

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
        .pivot(
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
        .pivot(
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
    )

    return iamc_inv, iamc_fix


def create_cost_projections(
    node,
    ref_region,
    base_year,
    module,
    method,
    scenario_version,
    scenario,
    convergence_year,
    fom_rate,
    format,
):
    """Get investment and fixed cost projections

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
    # Change node selection to upper case
    node_up = node.upper()

    # Check if node selection is valid
    if node_up not in ["R11", "R12", "R20"]:
        return "Please select a valid spatial resolution: R11, R12, or R20"
    else:
        # Set default values for input arguments
        # If specified node is R11, then use R11_NAM as the reference region
        # If specified node is R12, then use R12_NAM as the reference region
        # If specified node is R20, then use R20_NAM as the reference region
        # However, if a reference region is specified, then use that instead
        if ref_region is None:
            if node_up == "R11":
                ref_region = "R11_NAM"
            if node_up == "R12":
                ref_region = "R12_NAM"
            if node_up == "R20":
                ref_region = "R20_NAM"
        elif ref_region is not None:
            ref_region = ref_region.upper()

        # Print final selection of regions, reference regions, and base year
        print("Selected module: " + module)
        print("Selected node: " + node_up)
        print("Selected reference region: " + ref_region)
        print("Selected base year: " + str(base_year))
        print("Selected method: " + method)
        print("Selected fixed O&M rate: " + str(fom_rate))
        print("Selected format: " + format)

        # If method is learning, then use the learning method
        if method == "learning":
            df_costs = create_projections_learning(
                in_node=node_up,
                in_ref_region=ref_region,
                in_base_year=base_year,
                in_module=module,
                in_scenario=scenario,
            )

        # If method is GDP, then use the GDP method
        if method == "gdp":
            df_costs = create_projections_gdp(
                in_node=node_up,
                in_ref_region=ref_region,
                in_base_year=base_year,
                in_module=module,
                in_scenario=scenario,
                in_scenario_version=scenario_version,
            )

        # If method is convergence, then use the convergence method
        if method == "convergence":
            df_costs = create_projections_converge(
                in_node=node_up,
                in_ref_region=ref_region,
                in_base_year=base_year,
                in_module=module,
                in_scenario=scenario,
                in_convergence_year=convergence_year,
            )

        if format == "message":
            df_inv, df_fom = create_message_outputs(df_costs, fom_rate=fom_rate)

            proj = projections(df_inv, df_fom)
            return proj

        if format == "iamc":
            df_inv, df_fom = create_message_outputs(df_costs, fom_rate=fom_rate)
            df_inv_iamc, df_fom_iamc = create_iamc_outputs(df_inv, df_fom)

            proj = projections(df_inv_iamc, df_fom_iamc)

            return proj
