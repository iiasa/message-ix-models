"""Prepare data for adding demands"""

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df as make_df

from message_ix_models.model.water.data.demand_rules import (
    HISTORICAL_ACTIVITY,
    HISTORICAL_CAPACITY,
    INDUSTRIAL_DEMAND,
    RURAL_DEMAND,
    RURAL_WST,
    SHARE_CONSTRAINTS_GW,
    SHARE_CONSTRAINTS_RECYCLING,
    URBAN_DEMAND,
    URBAN_WST,
    WATER_AVAILABILITY,
)
from message_ix_models.model.water.dsl_engine import build_standard
from message_ix_models.model.water.utils import safe_concat
from message_ix_models.util import minimum_version, package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context


def load_rules_special(rule: dict, df_processed: pd.DataFrame = None) -> pd.DataFrame:
    """
    Wrapper on build_standard, since most demand rules don't require additional
    arguments.
    """
    r = rule.copy()
    rule_dfs = df_processed.copy()
    base_args = {"rule_dfs": rule_dfs}
    df_rule = build_standard(r, base_args)
    return df_rule


def get_basin_sizes(
    basin: pd.DataFrame, node: str
) -> Sequence[Union[pd.Series, Literal[0]]]:
    """Returns the sizes of developing and developed basins for a given node"""
    temp = basin[basin["BCU_name"] == node]
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    sizes_dev = sizes["DEV"] if "DEV" in sizes.index else 0
    sizes_ind = sizes["IND"] if "IND" in sizes.index else 0
    return_tuple: tuple[Union[pd.Series, Literal[0]], Union[pd.Series, Literal[0]]] = (
        sizes_dev,
        sizes_ind,
    )
    return return_tuple


def set_target_rate(df: pd.DataFrame, node: str, year: int, target: float) -> None:
    """Sets the target value for a given node and year"""
    indices = df[df["node"] == node][df[df["node"] == node]["year"] == year].index
    for index in indices:
        if (
            df[df["node"] == node][df[df["node"] == node]["year"] == year].at[
                index, "value"
            ]
            < target
        ):
            df.at[index, "value"] = target


@minimum_version("message_ix 3.7")
def target_rate(df: pd.DataFrame, basin: pd.DataFrame, val: float) -> pd.DataFrame:
    """
    Sets target connection and sanitation rates for SDG scenario.
    The function filters out the basins as developing and
    developed based on the countries overlapping basins.
    If the number of developing countries in the basins are
    more than basin is categorized as developing and vice versa.
    If the number of developing and developed countries are equal
    in a basin, then the basin is assumed developing.
    For developed basins, target is set at 2030.
    For developing basins, the access target is set at
    2040 and 2035 target is the average of
    2030 original rate and 2040 target.

    Returns
    -------
        df (pandas.DataFrame): Data frame with updated value column.
    """
    for node in df.node.unique():
        dev_size, ind_size = get_basin_sizes(basin, node)

        is_developed = dev_size >= ind_size
        if is_developed:
            set_target_rate(df, node, 2030, val)
        else:
            for i in df.index:
                if df.at[i, "node"] == node and df.at[i, "year"] == 2030:
                    value_2030 = df.at[i, "value"]
                    break
            set_target_rate(df, node, 2035, (value_2030 + val) / 2)
            set_target_rate(df, node, 2040, val)


def target_rate_trt(df: pd.DataFrame, basin: pd.DataFrame) -> pd.DataFrame:
    """
    Sets target treatment rates for SDG scenario. The target value for
    developed and developing regions is making sure that the amount of untreated
    wastewater is halved beyond 2030 & 2040 respectively.

    Returns
    -------
    data : pandas.DataFrame
    """
    updates = []  # Will hold tuples of (index, new_value)
    for node in df.node.unique():
        basin_node = basin[basin["BCU_name"] == node]
        sizes = basin_node.pivot_table(index=["STATUS"], aggfunc="size")

        # Use pattern matching to decide on the threshold year.
        is_dev = sizes["DEV"] >= sizes["IND"]
        if len(sizes) > 1:
            threshold = 2040 if is_dev else 2030
        else:
            threshold = 2040 if sizes.index[0] == "DEV" else 2030

        # Filter rows for this node and the chosen threshold year.
        node_rows = df[(df["node"] == node) & (df["year"] >= threshold)]
        for j in node_rows.index:
            old_val = df.at[j, "value"]
            new_val = old_val + (1 - old_val) / 2
            updates.append((j, np.float64(new_val)))

    # Create a temporary DataFrame from the updates.
    update_df = pd.DataFrame(updates, columns=["Index", "Value"])

    # Update the main DataFrame with new values.
    for _, row in update_df.iterrows():
        df.at[row["Index"], "Value"] = row["Value"]

    # Combine new values with original ones.
    real_value = df["Value"].combine_first(df["value"])
    df.drop(["value", "Value"], axis=1, inplace=True)
    df["value"] = real_value

    return df


def _preprocess_availability_data(
    df: pd.DataFrame, monthly: bool = False, df_x: pd.DataFrame = None, info=None
) -> pd.DataFrame:
    """
    Preprocesses availability data
    """
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    df.index = df_x["BCU_name"].index
    df = df.stack().reset_index()
    df.columns = pd.Index(["Region", "years", "value"])
    df.fillna(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["year"] = pd.DatetimeIndex(df["years"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["years"]).month
    df["Region"] = df["Region"].map(df_x["BCU_name"])
    df2210 = df[df["year"] == 2100].copy()
    df2210["year"] = 2110
    df = safe_concat([df, df2210])
    df = df[df["year"].isin(info.Y)]
    return df


@minimum_version("python 3.10")
def read_water_availability(context: "Context") -> Sequence[pd.DataFrame]:
    """
    Reads water availability data and bias correct
    it for the historical years and no climate
    scenario assumptions.

    Requires Python 3.10+ for pattern matching support.
    """

    # Reference to the water configuration
    info = context["water build info"]
    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)
    monthly = False
    match context.time:
        case "year":
            # path for reading basin delineation file
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
            # Reading data, the data is spatially and temprally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case "month":
            monthly = True
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )

            # Reading data, the data is spatially and temporally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time period: {context.time}")

    df_sw = pd.read_csv(path1)
    df_sw = _preprocess_availability_data(df_sw, monthly=monthly, df_x=df_x, info=info)

    df_gw = pd.read_csv(path2)
    df_gw = _preprocess_availability_data(df_gw, monthly=monthly, df_x=df_x, info=info)

    return df_sw, df_gw


def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds water supply constraints

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.

    """

    # define an empty dictionary
    results = {}
    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs

    df_sw, df_gw = read_water_availability(context)
    water_availability = []
    avail_dfs = {"df_sw": df_sw, "df_gw": df_gw}
    # WATER_AVAILABILITY.change_unit("km3/year")
    for rule in WATER_AVAILABILITY.get_rule():
        water_availability.append(load_rules_special(rule, avail_dfs))
    dmd_df = safe_concat(water_availability)

    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)

    results["demand"] = dmd_df

    share_constraints_gw = []
    share_dfs = {"df_gw": df_gw, "df_sw": df_sw}
    for rule in SHARE_CONSTRAINTS_GW.get_rule():
        share_constraints_gw.append(load_rules_special(rule, share_dfs))

    share_constraints_gw = safe_concat(share_constraints_gw)
    share_constraints_gw["value"] = share_constraints_gw["value"].fillna(0)

    results["share_commodity_lo"] = share_constraints_gw

    return results


def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds endogenous irrigation water demands from GLOBIOM emulator

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # define an empty dictionary
    results = {}

    scen = context.get_scenario()
    # add water for irrigation from globiom
    land_out_1 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"}
    )
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"}
    )
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"}
    )
    land_out_3["level"] = "irr_sugarcrops"

    land_out = safe_concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"

    land_out["value"] = 1e-3 * land_out["value"]

    # take land_out edited and add as a demand in  land_input
    results["land_input"] = land_out

    return results


def _preprocess_demand_data_stage1(context: "Context") -> pd.DataFrame:
    """
    Pre-process the DataFrame to prepare it for the rule evaluation.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : pandas.DataFrame

    """

    # read and clean raw demand data to standardized format
    region = f"{context.regions}"
    # get data path using package_data_path
    path = package_data_path("water", "demands", "harmonized", region, ".")
    # get the csv files matching format
    list_of_csvs = list(path.glob("ssp2_regional_*.csv"))
    fns = [os.path.splitext(os.path.basename(x))[0] for x in list_of_csvs]
    fns = " ".join(fns).replace("ssp2_regional_", "").split()
    d: dict[str, pd.DataFrame] = {}
    for i in range(len(fns)):
        d[fns[i]] = pd.read_csv(list_of_csvs[i])
    dfs = {}
    for key, df in d.items():
        df.rename(columns={"Unnamed: 0": "year"}, inplace=True)
        df.set_index("year", inplace=True)
        dfs[key] = df
    # combine dataframes using xarray and interpolate selected years
    df_x = xr.Dataset(dfs).to_array()
    df_x_interp = df_x.interp(year=[2015, 2025, 2035, 2045, 2055])
    df_x_c = df_x.combine_first(df_x_interp)
    df_f = df_x_c.to_dataframe("").unstack()
    # stack to obtain standardized dataframe with columns year, node, variable, value
    df_dmds = df_f.stack(future_stack=True).reset_index(level=0).reset_index()
    df_dmds.columns = ["year", "node", "variable", "value"]
    df_dmds.sort_values(["year", "node", "variable", "value"], inplace=True)
    df_dmds["time"] = "year"
    # if sub-annual timesteps are used, merge with monthly data
    if "year" not in context.time:
        PATH = package_data_path(
            "water", "demands", "harmonized", region, "ssp2_m_water_demands.csv"
        )
        df_m: pd.DataFrame = pd.read_csv(PATH)
        df_m.value *= 30  # conversion from mcm/day to mcm/month
        df_m.loc[df_m["sector"] == "industry", "sector"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m.loc[df_m["variable"] == "urban_withdrawal_baseline", "variable"] = (
            "urbann_withdrawal2_baseline"
        )
        df_m.loc[df_m["variable"] == "urban_return_baseline", "variable"] = (
            "urbann_return2_baseline"
        )
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = pd.Index(["year", "node", "variable", "value", "time"])
        # remove yearly parts before merging with monthly data
        df_dmds = df_dmds[
            ~df_dmds["variable"].isin(
                [
                    "urban_withdrawal2_baseline",
                    "rural_withdrawal_baseline",
                    "manufacturing_withdrawal_baseline",
                    "manufacturing_return_baseline",
                    "urban_return2_baseline",
                    "rural_return_baseline",
                ]
            )
        ]
        df_dmds = safe_concat([df_dmds, df_m])
    return df_dmds


def _preprocess_demand_data_stage2(df_dmds: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
     Second stage of pre-processing the DataFrame to prepare it for the rule evaluation.

    Parameters
    ----------
    df_dmds : pandas.DataFrame

    Returns
    """
    variables_operations = {
        "urban_withdrawal2_baseline": {
            "df_name": "urban_withdrawal_df",
            "reset_index": False,
        },
        "rural_withdrawal_baseline": {
            "df_name": "rural_withdrawal_df",
            "reset_index": False,
        },
        "manufacturing_withdrawal_baseline": {
            "df_name": "industrial_withdrawals_df",
            "reset_index": False,
        },
        "manufacturing_return_baseline": {
            "df_name": "industrial_return_df",
            "reset_index": False,
        },
        "urban_return2_baseline": {"df_name": "urban_return_df", "reset_index": True},
        "rural_return_baseline": {"df_name": "rural_return_df", "reset_index": True},
        "urban_connection_rate_baseline": {
            "df_name": "urban_connection_rate_df",
            "reset_index": True,
        },
        "rural_connection_rate_baseline": {
            "df_name": "rural_connection_rate_df",
            "reset_index": True,
        },
        "urban_treatment_rate_baseline": {
            "df_name": "urban_treatment_rate_df",
            "reset_index": True,
        },
        "rural_treatment_rate_baseline": {
            "df_name": "rural_treatment_rate_df",
            "reset_index": True,
        },
        "urban_recycling_rate_baseline": {
            "df_name": "df_recycling",
            "reset_index": True,
        },
    }
    Results = {}
    for variable, attrs in variables_operations.items():
        df_name, reset_index = attrs["df_name"], attrs["reset_index"]
        df_name = df_dmds[df_dmds["variable"] == variable]
        if reset_index:
            df_name.reset_index(drop=True, inplace=True)
        Results[attrs["df_name"]] = df_name

    return Results


def _apply_sdg_adjustments(
    context: "Context", df_dmds: pd.DataFrame, processed_data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Applies SDG adjustments to water demand data."""

    pol_scen = context.SDG

    # Retrieve baseline dataframes from processed_data
    rural_treatment_rate_df = processed_data["rural_treatment_rate_df"]
    urban_treatment_rate_df = processed_data["urban_treatment_rate_df"]
    urban_connection_rate_df = processed_data["urban_connection_rate_df"]
    rural_connection_rate_df = processed_data["rural_connection_rate_df"]
    df_recycling = processed_data["df_recycling"]

    SDG_RATES = {
        "rural_rate": 0.8,
        "urban_rate": 0.95,
        "urban_connection_rate": 0.99,
        "rural_connection_rate": 0.8,
    }

    if pol_scen == "SDG":
        FILE2 = f"basins_country_{context.regions}.csv"
        PATH = package_data_path("water", "delineation", FILE2)
        df_basin = pd.read_csv(PATH)

        # Apply target rates using the helper function
        rural_treatment_rate_df_sdg = target_rate(
            rural_treatment_rate_df, df_basin, SDG_RATES["rural_rate"]
        )
        urban_treatment_rate_df_sdg = target_rate(
            urban_treatment_rate_df, df_basin, SDG_RATES["urban_rate"]
        )
        urban_connection_rate_df_sdg = target_rate(
            urban_connection_rate_df, df_basin, SDG_RATES["urban_connection_rate"]
        )
        rural_connection_rate_df_sdg = target_rate(
            rural_connection_rate_df, df_basin, SDG_RATES["rural_connection_rate"]
        )
        df_recycling_sdg = target_rate_trt(df_recycling, df_basin)

    else:  # Handle other policy scenarios
        # Check if policy data exists
        check_dm = df_dmds[df_dmds["variable"] == f"urban_connection_rate_{pol_scen}"]
        if check_dm.empty:
            raise ValueError(f"Policy data is missing for the {pol_scen} scenario.")

        # Load policy-specific data
        urban_connection_rate_df_sdg = df_dmds[
            df_dmds["variable"] == f"urban_connection_rate_{pol_scen}"
        ].reset_index(drop=True)
        rural_connection_rate_df_sdg = df_dmds[
            df_dmds["variable"] == f"rural_connection_rate_{pol_scen}"
        ].reset_index(drop=True)
        urban_treatment_rate_df_sdg = df_dmds[
            df_dmds["variable"] == f"urban_treatment_rate_{pol_scen}"
        ].reset_index(drop=True)
        rural_treatment_rate_df_sdg = df_dmds[
            df_dmds["variable"] == f"rural_treatment_rate_{pol_scen}"
        ].reset_index(drop=True)
        df_recycling_sdg = df_dmds[
            df_dmds["variable"] == f"urban_recycling_rate_{pol_scen}"
        ].reset_index(drop=True)

    # Update the processed_data dictionary with SDG-adjusted dataframes
    processed_data["rural_treatment_rate_df"] = rural_treatment_rate_df_sdg
    processed_data["urban_treatment_rate_df"] = urban_treatment_rate_df_sdg
    processed_data["urban_connection_rate_df"] = urban_connection_rate_df_sdg
    processed_data["rural_connection_rate_df"] = rural_connection_rate_df_sdg
    processed_data["df_recycling"] = df_recycling_sdg

    # Combine and save all rates if needed (optional, could be moved)
    all_rates_sdg = safe_concat(
        [
            urban_connection_rate_df_sdg,
            rural_connection_rate_df_sdg,
            urban_treatment_rate_df_sdg,
            rural_treatment_rate_df_sdg,
            df_recycling_sdg,
        ]
    )
    all_rates_sdg["variable"] = [
        x.replace("baseline", pol_scen) for x in all_rates_sdg["variable"]
    ]
    # Note: The original code saves 'all_rates_SSP2.csv' here.
    # This might be better handled outside this function if the saved
    # file isn't directly used by subsequent steps within add_sectoral_demands.
    # For now, we'll keep it for consistency.
    save_path = package_data_path("water", "demands", "harmonized", context.regions)
    all_rates_sdg.to_csv(save_path / f"all_rates_{pol_scen}.csv", index=False)

    return processed_data


def _rate_value_adjustment(
    df: pd.DataFrame, rate_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns two dataframes: one with the value multiplied
    by the rate and one with the value multiplied by (1 - rate).
    Helps to avoid code duplication.
    """
    df = df.reset_index(drop=True)

    rate_df = rate_df.drop(columns=["variable", "time"])

    merged_df = df.merge(rate_df.rename(columns={"value": "rate"}))
    # Create the non-inverted result (value multiplied by rate)
    normal_df = merged_df.copy(deep=True)
    normal_df["value"] = normal_df["value"] * normal_df["rate"]
    # Create the inverted result (value multiplied by (1 - rate))
    inverted_df = merged_df.copy(deep=True)
    inverted_df["value"] = inverted_df["value"] * (1 - inverted_df["rate"])
    return normal_df, inverted_df


@minimum_version("message_ix 3.7")
def add_sectoral_demands(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds water sectoral demands

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    results = {}
    info = context["water build info"]
    sub_time = context.time

    # Stage 1: Preprocess raw data
    df_dmds = _preprocess_demand_data_stage1(context)

    # Stage 2: Separate data by variable
    processed_data = _preprocess_demand_data_stage2(df_dmds)

    # Stage 3: Apply SDG adjustments if necessary
    if context.SDG != "baseline":
        processed_data = _apply_sdg_adjustments(context, df_dmds, processed_data)

    # Unpack potentially adjusted dataframes
    urban_withdrawal_df = processed_data["urban_withdrawal_df"]
    rural_withdrawal_df = processed_data["rural_withdrawal_df"]
    industrial_withdrawals_df = processed_data["industrial_withdrawals_df"]
    industrial_return_df = processed_data["industrial_return_df"]
    urban_return_df = processed_data["urban_return_df"]
    rural_return_df = processed_data["rural_return_df"]
    urban_conn_rate_df = processed_data["urban_connection_rate_df"]
    rural_conn_rate_df = processed_data["rural_connection_rate_df"]
    urban_treat_rate_df = processed_data["urban_treatment_rate_df"]
    rural_treat_rate_df = processed_data["rural_treatment_rate_df"]
    df_recycling = processed_data["df_recycling"]

    # --- Process Demand Rules ---
    all_demands = []  # List to hold all demand dataframes

    # Urban Demands
    urban_dmds = []
    # Both urban_mw and urban_disconnected use the same df with the rate inverted
    dfs_urban = dict(
        zip(
            ("urban_mw", "urban_dis"),
            _rate_value_adjustment(urban_withdrawal_df, urban_conn_rate_df),
        )
    )
    # URBAN_DEMAND.change_unit("km3/year")
    for r in URBAN_DEMAND.get_rule():
        urban_dmds.append(load_rules_special(r, dfs_urban))
    all_demands.append(safe_concat(urban_dmds))

    # Rural Demands
    rural_dmds = []
    dfs_rural = dict(
        zip(
            ("rural_mw", "rural_dis"),
            _rate_value_adjustment(rural_withdrawal_df, rural_conn_rate_df),
        )
    )
    # RURAL_DEMAND.change_unit("km3/year")
    for r in RURAL_DEMAND.get_rule():
        rural_dmds.append(load_rules_special(r, dfs_rural))
    all_demands.append(safe_concat(rural_dmds))

    # Industrial Demands
    industrial_dmds = []
    dfs_industrial = {
        "manuf_mw": industrial_withdrawals_df.reset_index(drop=True),
        "manuf_uncollected_wst": industrial_return_df.reset_index(drop=True),
    }
    # INDUSTRIAL_DEMAND.change_unit("km3/year")
    for r in INDUSTRIAL_DEMAND.get_rule():
        industrial_dmds.append(load_rules_special(r, dfs_industrial))
    all_demands.append(safe_concat(industrial_dmds))

    urban_coll_wst_df = []

    dfs_urban_wst = dict(
        zip(
            ("urban_collected_wst", "urban_uncollected_wst"),
            _rate_value_adjustment(urban_return_df, urban_treat_rate_df),
        )
    )
    # URBAN_WST.change_unit("km3/year")
    for r in URBAN_WST.get_rule():
        urban_coll_wst_df.append(load_rules_special(r, dfs_urban_wst))
    all_demands.append(safe_concat(urban_coll_wst_df))

    # Rural Collected Wastewater
    rural_coll_wst_df = []
    dfs_rural_wst = dict(
        zip(
            ("rural_collected_wst", "rural_uncollected_wst"),
            _rate_value_adjustment(rural_return_df, rural_treat_rate_df),
        )
    )
    # RURAL_WST.change_unit("km3/year")
    for r in RURAL_WST.get_rule():
        rural_coll_wst_df.append(load_rules_special(r, dfs_rural_wst))
    all_demands.append(safe_concat(rural_coll_wst_df))

    dmd_df = safe_concat(all_demands)

    # --- Historical Data & Shares ---

    # Separate historical data (2010, 2015) from projection years
    h_act_raw = dmd_df[dmd_df["year"].isin([2010, 2015])].copy()
    dmd_df = dmd_df[dmd_df["year"].isin(info.Y)]
    results["demand"] = dmd_df

    # Process historical activity
    h_act_processed = _process_historical_activity(h_act_raw)
    results["historical_activity"] = h_act_processed

    # Process historical capacity (based on raw activity data with 'year')
    results["historical_new_capacity"] = _process_historical_capacity(
        h_act_raw  # Use h_act_raw which has the 'year' column
    )

    # Share constraint lower bound on urban_Water recycling
    share_constraint_df = []
    for rule in SHARE_CONSTRAINTS_RECYCLING.get_rule():
        base_args = {
            "rule_dfs": df_recycling,
            "sub_time": pd.Series(sub_time),
        }
        df_share_wat = build_standard(rule, base_args)
        share_constraint_df.append(df_share_wat)

    share_commodity_lo = safe_concat(share_constraint_df)
    results["share_commodity_lo"] = share_commodity_lo[
        share_commodity_lo["year_act"].isin(info.Y)
    ]

    return results


def _process_historical_activity(h_act: pd.DataFrame) -> pd.DataFrame:
    """Processes historical activity data."""
    # Define conditions and corresponding values for commodity mapping
    conditions = [
        (h_act["commodity"] == "urban_mw"),
        (h_act["commodity"] == "industry_mw"),
        (h_act["commodity"] == "rural_mw"),
        (h_act["commodity"] == "urban_disconnected"),
        (h_act["commodity"] == "rural_disconnected"),
        (h_act["commodity"] == "urban_collected_wst"),
        (h_act["commodity"] == "rural_collected_wst"),
        (h_act["commodity"] == "urban_uncollected_wst"),
        (h_act["commodity"] == "industry_uncollected_wst"),
        (h_act["commodity"] == "rural_uncollected_wst"),
    ]
    values = [
        "urban_t_d",
        "industry_unconnected",
        "rural_t_d",
        "urban_unconnected",
        "rural_unconnected",
        "urban_sewerage",
        "rural_sewerage",
        "urban_untreated",
        "industry_untreated",
        "rural_untreated",
    ]

    # Apply mapping and ensure positive values
    h_act["commodity"] = np.select(conditions, values, "Unknown commodity")
    h_act["value"] = h_act["value"].abs()

    # Process historical activities using rules
    historical_activity_df = []
    # HISTORICAL_ACTIVITY.change_unit("km3/year")
    for rule in HISTORICAL_ACTIVITY.get_rule():
        historical_activity_df.append(load_rules_special(rule, h_act))

    return safe_concat(historical_activity_df)


def _process_historical_capacity(h_act: pd.DataFrame) -> pd.DataFrame:
    """Processes historical capacity data based on activity."""
    h_cap = h_act[h_act["year"] >= 2015]
    h_cap = (
        h_cap.groupby(["node", "commodity", "level", "year", "unit"])["value"]
        .sum()
        .reset_index()
    )
    historical_capacity_df = []
    # HISTORICAL_CAPACITY.change_unit("km3/year")
    for rule in HISTORICAL_CAPACITY.get_rule():
        historical_capacity_df.append(load_rules_special(rule, h_cap))
    return safe_concat(historical_capacity_df)
