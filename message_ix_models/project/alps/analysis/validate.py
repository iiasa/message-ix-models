"""Validation utilities for CID scenario ensembles.

Functions for checking monotonicity and temporal coherence of RIME predictions.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from scipy import stats


# Forcing order from low to high warming
FORCING_ORDER = ["600f", "850f", "1100f", "1350f", "1850f", "2100f", "2350f"]


def compute_basin_monotonicity(
    water_data: dict[str, pd.DataFrame],
    forcing_order: list[str] | None = None,
    year: int | None = None,
) -> pd.DataFrame:
    """Compute Spearman correlation per basin across forcing scenarios.

    Tests whether water availability increases/decreases monotonically
    with forcing level. Positive rho = more water with higher forcing.

    Parameters
    ----------
    water_data : dict[str, pd.DataFrame]
        Scenario name -> wide-format DataFrame (basins × years)
        Scenario names should contain forcing level (e.g., 'nexus_baseline_600f_annual')
    forcing_order : list[str], optional
        Ordered forcing levels. Default: ['600f', '850f', ..., '2350f']
    year : int, optional
        Specific year to analyze. If None, sum across all years.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: [basin, spearman_rho, p_value, n_scenarios]
    """
    if forcing_order is None:
        forcing_order = FORCING_ORDER

    # Extract forcing level from scenario name
    def get_forcing(name: str) -> str | None:
        for f in forcing_order:
            if f in name:
                return f
        return None

    # Build forcing-indexed data
    forcing_data = {}
    for name, df in water_data.items():
        forcing = get_forcing(name)
        if forcing is None:
            continue
        if year is not None:
            if year in df.columns:
                forcing_data[forcing] = df[year]
            else:
                continue
        else:
            forcing_data[forcing] = df.sum(axis=1)

    if len(forcing_data) < 3:
        raise ValueError(f"Need at least 3 forcing scenarios, got {len(forcing_data)}")

    # Order by forcing
    ordered_forcings = [f for f in forcing_order if f in forcing_data]
    forcing_ranks = list(range(len(ordered_forcings)))

    # Get all basins
    basins = forcing_data[ordered_forcings[0]].index

    # Compute Spearman correlation per basin
    results = []
    for basin in basins:
        values = [forcing_data[f].loc[basin] for f in ordered_forcings]

        # Skip if any NaN
        if any(pd.isna(v) for v in values):
            results.append({
                "basin": basin,
                "spearman_rho": np.nan,
                "p_value": np.nan,
                "n_scenarios": len(ordered_forcings),
            })
            continue

        rho, pval = stats.spearmanr(forcing_ranks, values)
        results.append({
            "basin": basin,
            "spearman_rho": rho,
            "p_value": pval,
            "n_scenarios": len(ordered_forcings),
        })

    return pd.DataFrame(results)


def compute_temporal_coherence(
    water_data: dict[str, pd.DataFrame],
    baseline_key: str,
) -> pd.DataFrame:
    """Check temporal smoothness of scenario differences.

    Computes autocorrelation of year-to-year changes in scenario differences.
    High autocorrelation = smooth trends (good)
    Low/negative autocorrelation = erratic jumps (investigate)

    Parameters
    ----------
    water_data : dict[str, pd.DataFrame]
        Scenario name -> wide-format DataFrame (basins × years)
    baseline_key : str
        Key identifying the baseline scenario

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: [basin, scenario, autocorr]
    """
    baseline = water_data[baseline_key]
    results = []

    for name, df in water_data.items():
        if name == baseline_key:
            continue

        # Compute difference
        diff = df - baseline

        # For each basin, compute autocorrelation of differences across years
        for basin in diff.index:
            series = diff.loc[basin].values
            if len(series) < 3:
                autocorr = np.nan
            else:
                # Lag-1 autocorrelation
                autocorr = np.corrcoef(series[:-1], series[1:])[0, 1]

            results.append({
                "basin": basin,
                "scenario": name,
                "autocorr": autocorr,
            })

    return pd.DataFrame(results)


def validate_scenario_ensemble(
    water_data: dict[str, pd.DataFrame],
    baseline_key: str,
    forcing_order: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Run full validation suite on scenario ensemble.

    Parameters
    ----------
    water_data : dict[str, pd.DataFrame]
        Scenario name -> wide-format DataFrame (basins × years)
    baseline_key : str
        Key identifying the baseline scenario
    forcing_order : list[str], optional
        Ordered forcing levels for monotonicity check

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with keys:
        - 'monotonicity': per-basin Spearman correlations
        - 'coherence': per-basin temporal autocorrelations
        - 'summary': aggregate statistics
    """
    # Monotonicity check
    mono = compute_basin_monotonicity(water_data, forcing_order)

    # Coherence check
    coherence = compute_temporal_coherence(water_data, baseline_key)

    # Summary statistics
    summary_rows = [
        {
            "metric": "monotonicity_mean_rho",
            "value": mono["spearman_rho"].mean(),
        },
        {
            "metric": "monotonicity_median_rho",
            "value": mono["spearman_rho"].median(),
        },
        {
            "metric": "monotonicity_std_rho",
            "value": mono["spearman_rho"].std(),
        },
        {
            "metric": "monotonicity_pct_positive",
            "value": 100 * (mono["spearman_rho"] > 0).mean(),
        },
        {
            "metric": "monotonicity_pct_significant",
            "value": 100 * (mono["p_value"] < 0.05).mean(),
        },
        {
            "metric": "coherence_mean_autocorr",
            "value": coherence["autocorr"].mean(),
        },
        {
            "metric": "coherence_median_autocorr",
            "value": coherence["autocorr"].median(),
        },
        {
            "metric": "coherence_pct_high",
            "value": 100 * (coherence["autocorr"] > 0.5).mean(),
        },
        {
            "metric": "n_basins",
            "value": len(mono),
        },
        {
            "metric": "n_scenarios",
            "value": len(water_data) - 1,
        },
    ]
    summary = pd.DataFrame(summary_rows)

    return {
        "monotonicity": mono,
        "coherence": coherence,
        "summary": summary,
    }


def compute_per_year_monotonicity(
    water_data: dict[str, pd.DataFrame],
    years: list[int],
    forcing_order: list[str] | None = None,
) -> pd.DataFrame:
    """Compute monotonicity per basin per year.

    Parameters
    ----------
    water_data : dict[str, pd.DataFrame]
        Scenario name -> wide-format DataFrame (basins × years)
    years : list[int]
        Years to analyze
    forcing_order : list[str], optional
        Ordered forcing levels

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: [basin, year, spearman_rho, p_value]
    """
    results = []

    for year in years:
        mono = compute_basin_monotonicity(water_data, forcing_order, year=year)
        mono["year"] = year
        results.append(mono)

    return pd.concat(results, ignore_index=True)
