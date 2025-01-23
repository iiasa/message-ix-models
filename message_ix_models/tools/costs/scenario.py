import logging

import message_ix
import pandas as pd

from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections

log = logging.getLogger(__name__)


def replace_pre_base_year_cost(
    scen: message_ix.Scenario, config: Config, par: str
) -> pd.DataFrame:
    """
    Replace cost values with the base year value for years before the base year.

    The base year is specified in the Config object.

    Parameters
    ----------
    scen: message_ix.Scenario
        The scenario object containing the parameter data.
    config: Config
        Config object, from `tools.costs`, which has the `base_year` parameter.
    par: str
        The cost parameter to be modified. Must be either 'inv_cost' or 'fix_cost'.

    Returns
    -------
    pandas.DataFrame
        The modified parameter data with pre-base year values replaced.

    Raises
    ------
    ValueError
        If the parameter is not 'inv_cost' or 'fix_cost'.
    """
    df = scen.par(par)
    base_year = config.base_year

    def replace_values(group):
        if par == "inv_cost":
            base_year_value = group.loc[group["year_vtg"] == base_year, "value"]
            if not base_year_value.empty:
                group.loc[group["year_vtg"] < base_year, "value"] = (
                    base_year_value.iloc[0]
                )
        elif par == "fix_cost":
            base_year_value = group.loc[
                (group["year_vtg"] == base_year) & (group["year_act"] == base_year),
                "value",
            ]
            if not base_year_value.empty:
                group.loc[
                    group["year_act"] < base_year,
                    "value",
                ] = base_year_value.iloc[0]
        return group

    if par == "inv_cost":
        return df.groupby(["technology", "node_loc"]).apply(replace_values)
    elif par == "fix_cost":
        return df.groupby(["technology", "node_loc"]).apply(replace_values)
    else:
        raise ValueError("Parameter must be either 'inv_cost' or 'fix_cost'")


def filter_fix_cost_by_lifetime(scen: message_ix.Scenario) -> pd.DataFrame:
    """
    Filter the fixed cost parameter by the technical lifetime of technologies.

    This function retrieves the 'technical_lifetime' and 'fix_cost' parameters from the
    given scenario object, merges them to associate the lifetime with each
    technology-node_loc-year_vtg combination, and filters the fixed costs based on the
    lifetime. Meaning, only fix_cost values that are within the lifetime of the
    technology are retained.

    If a combination is missing lifetime data, the fixed cost is retained for all years.

    Parameters
    ----------
    scen: message_ix.Scenario
        The scenario object from which to retrieve parameters.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the filtered fixed costs, with lifetime data applied.
    """

    # Retrieve the technical lifetime parameter
    lifetime_df = scen.par("technical_lifetime")

    # Retrieve the fix_cost parameter
    fix_cost_df = scen.par("fix_cost")

    # Merge fix_cost with lifetime to get the lifetime for each
    # technology-node_loc combination
    merged_df = fix_cost_df.merge(
        lifetime_df,
        on=["technology", "node_loc", "year_vtg"],
        suffixes=("", "_lifetime"),
        how="left",
    )

    # Identify combinations of node_loc, technology, and year_vtg that are missing lifetime data
    missing_lifetime = merged_df[merged_df["value_lifetime"].isna()][
        ["technology", "node_loc", "year_vtg"]
    ].drop_duplicates()

    # Filter fix_cost based on the lifetime
    filtered_fix_cost = merged_df[
        (
            merged_df["value_lifetime"].notna()
            & (
                merged_df["year_act"]
                <= merged_df["year_vtg"] + merged_df["value_lifetime"]
            )
        )
        | merged_df[["technology", "node_loc", "year_vtg"]]
        .isin(missing_lifetime)
        .all(axis=1)
    ]

    # Drop the extra lifetime columns
    filtered_fix_cost = filtered_fix_cost.drop(
        columns=["value_lifetime", "unit_lifetime"]
    )

    return filtered_fix_cost


def update_scenario_costs(scen: message_ix.Scenario, config: Config):
    """
    Update investment and fixed costs in a MESSAGEix scenario based on cost projections.

    This function performs the following steps:
    1. Retrieves cost projections using the provided configuration.
    2. Filters the cost projections to include only the technologies
    that exist in the scenario.
    3. Updates the scenario's investment and fixed costs with the filtered projections.
    4. Replaces pre-base year values with base year values
    for both investment and fixed costs.
    5. Filters the fixed costs based on the technical lifetime of the technologies.
    6. Updates the scenario with the filtered fixed costs.

    Parameters
    ----------
    scen: message_ix.Scenario
        The MESSAGEix scenario to be updated.
    config: Config
        Config object containing the cost projection settings.

    """
    # Get cost projections
    log.info("Creating cost projections")
    cost_projections = create_cost_projections(config)

    # Retrieve model technologies
    model_tec_set = list(scen.set("technology"))

    # Filter inv_cost and fix_cost based on model technologies
    log.info("Filtering cost projections based on model technologies")
    inv = (
        cost_projections["inv_cost"][
            cost_projections["inv_cost"]["technology"].isin(model_tec_set)
        ]
        .drop(columns={"scenario_version", "scenario"})
        .drop_duplicates()
    )
    fix = (
        cost_projections["fix_cost"][
            cost_projections["fix_cost"]["technology"].isin(model_tec_set)
        ]
        .drop(columns=["scenario_version", "scenario"])
        .drop_duplicates()
    )

    # Update inv_cost and fix_cost in scenario
    log.info("Updating inv_cost and fix_cost in scenario")
    with scen.transact("Update inv_cost and fix_cost"):
        scen.add_par("inv_cost", inv)
        scen.add_par("fix_cost", fix)

    # Replace pre-base year values with base year values for inv_cost and fix_cost
    log.info("Get new costs with pre-base year values replaced")
    updated_inv_cost = replace_pre_base_year_cost(scen, config, "inv_cost")
    updated_fix_cost = replace_pre_base_year_cost(scen, config, "fix_cost")

    # Update the scenario with the new values
    log.info("Updating scenario with new pre-base year costs")
    with scen.transact("Replace pre-base year costs"):
        scen.add_par("inv_cost", updated_inv_cost)
        scen.add_par("fix_cost", updated_fix_cost)
