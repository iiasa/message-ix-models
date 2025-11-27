"""Aggregation utilities.

Functions for aggregating basin-level data to regional levels.
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def aggregate_by_year(
    data: pd.DataFrame,
    group_col: str = "year",
    value_col: str = "value",
    agg_func: str = "sum",
) -> pd.Series:
    """Aggregate data by year.

    Parameters
    ----------
    data : pd.DataFrame
        Long-form DataFrame with year and value columns
    group_col : str
        Column to group by (default: 'year')
    value_col : str
        Column containing values (default: 'value')
    agg_func : str
        Aggregation function ('sum' or 'mean')

    Returns
    -------
    pd.Series
        Aggregated values indexed by group_col
    """
    grouped = data.groupby(group_col)[value_col]
    if agg_func == "sum":
        return grouped.sum()
    elif agg_func == "mean":
        return grouped.mean()
    else:
        raise ValueError(f"Unknown agg_func: {agg_func}")


def aggregate_to_r12(
    basin_data: pd.DataFrame,
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Aggregate basin-level data to R12 regional level.

    Parameters
    ----------
    basin_data : pd.DataFrame
        Wide-format DataFrame with basins as index (B{id}|{region}) and years as columns
    agg_func : str
        Aggregation function: 'sum' for hydrology, 'mean' for intensive properties

    Returns
    -------
    pd.DataFrame
        Regional data with R12 regions as index and years as columns
    """
    # Extract region from basin node format: B{id}|{region}
    def extract_region(node: str) -> str:
        if "|" in node:
            return node.split("|")[-1]
        # Handle R12_XXX format
        if node.startswith("R12_"):
            return node
        return "UNKNOWN"

    basin_data = basin_data.copy()
    basin_data["region"] = basin_data.index.map(extract_region)

    # Filter to basin nodes only (exclude R12_* aggregates if present)
    basin_only = basin_data[basin_data.index.str.startswith("B")]

    if len(basin_only) == 0:
        # Data is already regional, just return
        return basin_data.drop(columns=["region"], errors="ignore")

    # Group and aggregate
    grouped = basin_only.groupby("region")
    if agg_func == "sum":
        regional = grouped.sum(numeric_only=True)
    else:
        regional = grouped.mean(numeric_only=True)

    regional.index.name = "region"
    return regional


def compute_basin_contributions(
    cost_diffs: dict[str, pd.DataFrame],
    water_diffs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Correlate per-basin water changes with cost changes.

    Parameters
    ----------
    cost_diffs : dict[str, pd.DataFrame]
        Scenario name -> basin cost differences (wide format)
    water_diffs : dict[str, pd.DataFrame]
        Scenario name -> basin water differences (wide format)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: [basin, scenario, water_change, cost_change]
    """
    rows = []

    for scenario in cost_diffs.keys():
        if scenario not in water_diffs:
            continue

        cost_df = cost_diffs[scenario]
        water_df = water_diffs[scenario]

        # Align indices
        common_basins = cost_df.index.intersection(water_df.index)

        for basin in common_basins:
            cost_total = cost_df.loc[basin].sum()
            water_total = water_df.loc[basin].sum()

            rows.append({
                "basin": basin,
                "scenario": scenario,
                "water_change": water_total,
                "cost_change": cost_total,
            })

    return pd.DataFrame(rows)


def compute_regional_summary(
    basin_data: dict[str, pd.DataFrame],
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Compute regional summary across scenarios.

    Parameters
    ----------
    basin_data : dict[str, pd.DataFrame]
        Scenario name -> basin-level wide-format DataFrame
    agg_func : str
        Aggregation function for basin-to-region

    Returns
    -------
    pd.DataFrame
        Regional totals with regions as index, scenarios as columns
    """
    regional = {}

    for name, df in basin_data.items():
        r12 = aggregate_to_r12(df, agg_func)
        regional[name] = r12.sum(axis=1)  # Sum across years

    result = pd.DataFrame(regional)
    result.index.name = "region"
    return result
