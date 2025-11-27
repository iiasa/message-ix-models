"""Scenario data extraction utilities.

Functions for loading scenarios and extracting costs/water CID parameters.
"""

from __future__ import annotations

import pandas as pd
from ixmp import Platform
from message_ix import Scenario


# Scenario alias expansion
SCENARIO_ALIASES = {
    "annual": [
        "nexus_baseline_annual",
        "nexus_baseline_600f_annual",
        "nexus_baseline_850f_annual",
        "nexus_baseline_1100f_annual",
        "nexus_baseline_1350f_annual",
        "nexus_baseline_1850f_annual",
        "nexus_baseline_2100f_annual",
        "nexus_baseline_2350f_annual",
    ],
    "seasonal": [
        "nexus_baseline_seasonal",
        "nexus_baseline_600f_seasonal",
        "nexus_baseline_850f_seasonal",
        "nexus_baseline_1100f_seasonal",
        "nexus_baseline_1350f_seasonal",
        "nexus_baseline_1850f_seasonal",
        "nexus_baseline_2100f_seasonal",
        "nexus_baseline_2350f_seasonal",
    ],
}


def expand_scenario_alias(scenarios_str: str) -> list[str]:
    """Expand scenario alias or parse comma-separated names.

    Parameters
    ----------
    scenarios_str : str
        Either 'annual', 'seasonal', or comma-separated scenario names

    Returns
    -------
    list[str]
        List of scenario names
    """
    if scenarios_str in SCENARIO_ALIASES:
        return SCENARIO_ALIASES[scenarios_str]
    return [s.strip() for s in scenarios_str.split(",")]


def load_scenarios(
    model: str,
    scenario_names: list[str],
    platform_name: str = "ixmp_dev",
    check_solved: bool = True,
) -> tuple[Platform, dict[str, Scenario]]:
    """Load multiple scenarios from ixmp platform.

    Parameters
    ----------
    model : str
        Model name
    scenario_names : list[str]
        List of scenario names to load
    platform_name : str
        ixmp platform name (default: ixmp_dev)
    check_solved : bool
        If True, skip scenarios without solutions

    Returns
    -------
    tuple[Platform, dict[str, Scenario]]
        Platform object (must be kept alive) and mapping from scenario name
        to loaded Scenario object
    """
    mp = Platform(platform_name)
    scenarios = {}

    for name in scenario_names:
        try:
            scen = Scenario(mp, model=model, scenario=name)
            if check_solved and not scen.has_solution():
                print(f"  Skipping {name}: no solution")
                continue
            scenarios[name] = scen
            print(f"  Loaded {name} (v{scen.version})")
        except Exception as e:
            print(f"  Error loading {name}: {e}")

    return mp, scenarios


def extract_nodal_costs(
    scenarios: dict[str, Scenario],
) -> dict[str, pd.DataFrame]:
    """Extract COST_NODAL variable from solved scenarios.

    Parameters
    ----------
    scenarios : dict[str, Scenario]
        Mapping from scenario name to Scenario object

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping from scenario name to DataFrame with columns [node, year, cost]
    """
    results = {}

    for name, scen in scenarios.items():
        costs = scen.var("COST_NODAL")
        df = costs[["node", "year", "lvl"]].copy()
        df.columns = ["node", "year", "cost"]
        results[name] = df
        print(f"  {name}: {len(df)} cost entries")

    return results


def extract_water_cids(
    scenarios: dict[str, Scenario],
    negate_demand: bool = True,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Extract water CID parameters from scenarios.

    Parameters
    ----------
    scenarios : dict[str, Scenario]
        Mapping from scenario name to Scenario object
    negate_demand : bool
        If True (default), negate demand values so positive = more water available.
        MESSAGE convention uses negative demand for supply constraints.

    Returns
    -------
    dict[str, dict[str, pd.DataFrame]]
        Nested dict: scenario_name -> {
            'surfacewater': DataFrame (positive = more water),
            'groundwater': DataFrame (positive = more water),
            'gw_share': DataFrame
        }
    """
    results = {}

    for name, scen in scenarios.items():
        # Surfacewater demand (qtot-based)
        sw = scen.par("demand", {"commodity": "surfacewater_basin"})

        # Groundwater demand (qr-based)
        gw = scen.par("demand", {"commodity": "groundwater_basin"})

        # Groundwater share constraint
        share = scen.par("share_commodity_lo", {"shares": "share_low_lim_GWat"})

        # Negate so positive = more water available
        if negate_demand:
            sw = sw.copy()
            gw = gw.copy()
            sw["value"] = -sw["value"]
            gw["value"] = -gw["value"]

        results[name] = {
            "surfacewater": sw,
            "groundwater": gw,
            "gw_share": share,
        }
        print(f"  {name}: sw={len(sw)}, gw={len(gw)}, share={len(share)} rows")

    return results


def pivot_to_wide(
    df: pd.DataFrame,
    index_col: str,
    columns_col: str,
    values_col: str,
) -> pd.DataFrame:
    """Pivot long-form DataFrame to wide format.

    Parameters
    ----------
    df : pd.DataFrame
        Long-form DataFrame
    index_col : str
        Column to use as index (e.g., 'node')
    columns_col : str
        Column to use for column headers (e.g., 'year')
    values_col : str
        Column containing values (e.g., 'cost', 'value')

    Returns
    -------
    pd.DataFrame
        Wide-format DataFrame (e.g., basins × years)
    """
    wide = df.pivot(index=index_col, columns=columns_col, values=values_col)
    return wide.sort_index()
