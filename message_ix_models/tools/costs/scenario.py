import logging
from typing import TYPE_CHECKING

from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util import add_par_data

if TYPE_CHECKING:
    from message_ix import Scenario
    from pandas import DataFrame

    from message_ix_models.tools.costs.config import Config

log = logging.getLogger(__name__)


def _replace_inner(group: "DataFrame", par: str, base_year: int) -> "DataFrame":
    if par == "inv_cost":
        base_year_value = group.loc[group["year_vtg"] == base_year, "value"]
        if not base_year_value.empty:
            group.loc[group["year_vtg"] < base_year, "value"] = base_year_value.iloc[0]
    elif par == "fix_cost":
        for year_vtg in group["year_vtg"].unique():
            if year_vtg < base_year:
                for year_act in group["year_act"].unique():
                    # If year_act is greater than or equal to base_year, replace the
                    # value with the year_vtg = base year value at the same year_act
                    if year_act >= base_year:
                        base_year_value = group.loc[
                            (group["year_vtg"] == base_year)
                            & (group["year_act"] == year_act),
                            "value",
                        ]
                        if not base_year_value.empty:
                            group.loc[
                                (group["year_vtg"] == year_vtg)
                                & (group["year_act"] == year_act),
                                "value",
                            ] = base_year_value.iloc[0]
                    # If year_act is less than base_year, replace the value with the
                    # base year value at year_vtg = base_year and year_act = base_year
                    elif year_act < base_year:
                        base_year_value = group.loc[
                            (group["year_vtg"] == base_year)
                            & (group["year_act"] == base_year),
                            "value",
                        ]
                        if not base_year_value.empty:
                            group.loc[
                                (group["year_vtg"] == year_vtg)
                                & (group["year_act"] == year_act),
                                "value",
                            ] = base_year_value.iloc[0]
    return group


def replace_pre_base_year_cost(
    scen: "Scenario", config: "Config", par: str
) -> "DataFrame":
    """Replace cost values with the base year value for years before the base year.

    The base year is specified via `config`.

    Parameters
    ----------
    scen :
        The scenario object containing the parameter data.
    config :
        with :attr:`.Config.base_year` set.
    par :
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
    if par not in ("fix_cost", "inv_cost"):
        raise ValueError("Parameter must be either 'inv_cost' or 'fix_cost'")

    return (
        scen.par(par)
        .groupby(["technology", "node_loc"])
        # include_groups=False is default in pandas 3.0, not in 2.x
        # TODO Remove once pandas 3.x is the minimum supported version
        .apply(
            _replace_inner, par=par, base_year=config.base_year, include_groups=False
        )
        .reset_index()
        .sort_values(["year_vtg"] + (["year_act"] if par == "fix_cost" else []))
    )


def filter_fix_cost_by_lifetime(scen: "Scenario") -> "DataFrame":
    """Filter the fixed cost parameter by the technical lifetime of technologies.

    This function retrieves the 'technical_lifetime' and 'fix_cost' parameters from the
    given scenario object, merges them to associate the lifetime with each (node_loc,
    technology, year_vtg) combination, and filters the fixed costs based on the
    lifetime. Meaning, only fix_cost values that are within the lifetime of the
    technology are retained.

    If a combination is missing lifetime data, the fixed cost is retained for all years.

    Parameters
    ----------
    scen :
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

    # Merge fix_cost with lifetime to get the lifetime for each (node_loc, technology)
    columns = ["node_loc", "technology", "year_vtg"]
    merged_df = fix_cost_df.merge(
        lifetime_df, on=columns, suffixes=("", "_lifetime"), how="left"
    ).astype({"year_act": int, "year_vtg": int})

    # Identify combinations of node_loc, technology, and year_vtg that are missing
    # lifetime data
    missing_lifetime = merged_df[merged_df["value_lifetime"].isna()][
        columns
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
        | merged_df[columns].isin(missing_lifetime).all(axis=1)
    ]

    # Drop the extra lifetime columns
    return filtered_fix_cost.drop(columns=["value_lifetime", "unit_lifetime"])


def update_scenario_costs(scen: "Scenario", config: "Config"):
    """Update investment and fixed costs in `scen` based on cost projections.

    This function performs the following steps:

    1. Retrieve cost projections using the provided `config`.
    2. Filter (1) to include only the technologies that exist in `scen`.
    3. Update the scenario's investment and fixed costs with the (2).
    4. Replace pre-base year values with base year values for both investment and fixed
       costs.
    5. Filter fixed costs based on the technical lifetime of the technologies.
    6. Updates the scenario with (6).

    Parameters
    ----------
    scen :
        Scenario to be updated.
    config :
        Cost projection settings, passed to :func:`create_cost_projections`.
    """
    # Retrieve model technologies
    model_tec_set = list(scen.set("technology"))

    log.info("Create cost projections and filter for model technologies")
    data = {
        par_name: df[df.technology.isin(model_tec_set)]
        .drop(columns=["scenario", "scenario_version"])
        .drop_duplicates()
        for par_name, df in create_cost_projections(config).items()
    }

    msg = "Update fix_cost and inv_cost"
    log.info(msg)
    with scen.transact(msg):
        add_par_data(scen, data)

    # Replace pre-base year values with base year values
    data = {
        k: replace_pre_base_year_cost(scen, config, k) for k in ("fix_cost", "inv_cost")
    }

    msg = f"Update pre-{config.base_year} cost values"
    log.info(msg)
    with scen.transact(msg):
        add_par_data(scen, data)

    del model_tec_set
