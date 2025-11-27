"""Cross-scenario comparison utilities.

Functions for computing differences and building comparison tables.
"""

from __future__ import annotations

import pandas as pd


def compute_scenario_diffs(
    data: dict[str, pd.DataFrame],
    baseline_key: str,
) -> dict[str, pd.DataFrame]:
    """Compute differences vs baseline for all scenarios.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Mapping from scenario name to wide-format DataFrame (e.g., basins × years)
    baseline_key : str
        Key identifying the baseline scenario

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping from scenario name to difference DataFrame (scenario - baseline)
    """
    baseline = data[baseline_key]
    diffs = {}

    for name, df in data.items():
        if name == baseline_key:
            continue
        # Verify alignment
        if not baseline.index.equals(df.index):
            raise ValueError(f"{name}: index doesn't match baseline")
        if not baseline.columns.equals(df.columns):
            raise ValueError(f"{name}: columns don't match baseline")

        diffs[name] = df - baseline

    return diffs


def compute_relative_change(
    actual: pd.DataFrame | float,
    baseline: pd.DataFrame | float,
    as_percent: bool = True,
) -> pd.DataFrame | float:
    """Compute relative change: (actual - baseline) / baseline.

    Parameters
    ----------
    actual : pd.DataFrame or float
        Actual values
    baseline : pd.DataFrame or float
        Baseline values for comparison
    as_percent : bool
        If True, multiply by 100 to get percentage

    Returns
    -------
    pd.DataFrame or float
        Relative change values
    """
    change = (actual - baseline) / baseline
    if as_percent:
        change = change * 100
    return change


def build_comparison_table(
    data: dict[str, pd.DataFrame],
    baseline_key: str,
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Build summary comparison table across scenarios.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Mapping from scenario name to wide-format DataFrame
    baseline_key : str
        Key identifying the baseline scenario
    agg_func : str
        Aggregation function ('sum' or 'mean')

    Returns
    -------
    pd.DataFrame
        Summary table with columns: scenario, total, diff_vs_baseline, pct_change
    """
    baseline_total = data[baseline_key].values.sum() if agg_func == "sum" else data[baseline_key].values.mean()

    rows = []
    for name, df in data.items():
        total = df.values.sum() if agg_func == "sum" else df.values.mean()

        if name == baseline_key:
            diff = 0.0
            pct = 0.0
        else:
            diff = total - baseline_total
            pct = 100 * diff / baseline_total if baseline_total != 0 else 0.0

        rows.append({
            "scenario": name,
            "total": total,
            "diff_vs_baseline": diff,
            "pct_change": pct,
        })

    return pd.DataFrame(rows)


def compute_yearly_comparison(
    data: dict[str, pd.DataFrame],
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Aggregate by year across scenarios for timeseries comparison.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Mapping from scenario name to wide-format DataFrame (rows × years)
    agg_func : str
        Aggregation function ('sum' or 'mean')

    Returns
    -------
    pd.DataFrame
        DataFrame with years as index and scenarios as columns
    """
    yearly = {}

    for name, df in data.items():
        if agg_func == "sum":
            yearly[name] = df.sum(axis=0)
        else:
            yearly[name] = df.mean(axis=0)

    result = pd.DataFrame(yearly)
    result.index.name = "year"
    return result


def compute_yearly_diffs(
    data: dict[str, pd.DataFrame],
    baseline_key: str,
    agg_func: str = "sum",
) -> pd.DataFrame:
    """Compute year-by-year differences vs baseline.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Mapping from scenario name to wide-format DataFrame
    baseline_key : str
        Key identifying the baseline scenario
    agg_func : str
        Aggregation function ('sum' or 'mean')

    Returns
    -------
    pd.DataFrame
        DataFrame with years as index and scenario diffs as columns
    """
    yearly = compute_yearly_comparison(data, agg_func)
    baseline_yearly = yearly[baseline_key]

    diffs = yearly.drop(columns=[baseline_key]).subtract(baseline_yearly, axis=0)
    return diffs
