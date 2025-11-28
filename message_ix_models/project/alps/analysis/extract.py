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


# Water demand commodity mappings
SECTORAL_DEMAND_COMMODITIES = {
    "urban": "urban_mw",
    "rural": "rural_mw",
    "manufacturing": "industry_mw",  # actual commodity name in scenario
    "urban_disconnected": "urban_disconnected",
    "rural_disconnected": "rural_disconnected",
}

# GLOBIOM land_output commodities for irrigation water
IRRIGATION_LAND_OUTPUT_COMMODITIES = [
    "Water|Withdrawal|Irrigation|Cereals",
    "Water|Withdrawal|Irrigation|Oilcrops",
    "Water|Withdrawal|Irrigation|Sugarcrops",
]


def extract_sectoral_demands(
    scen: Scenario,
) -> dict[str, pd.DataFrame]:
    """Extract sectoral water demands from a scenario.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with nexus module

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping from sector name to demand DataFrame with columns:
        [node, year, time, value] where value is in MCM/year
    """
    results = {}

    # Extract municipal/industrial demands by commodity
    for sector, commodity in SECTORAL_DEMAND_COMMODITIES.items():
        df = scen.par("demand", {"commodity": commodity})
        if not df.empty:
            results[sector] = df[["node", "year", "time", "value"]].copy()

    # Extract irrigation from land_output (GLOBIOM linkage)
    # Irrigation is at regional level (R12_*), not basin level
    irr_df = _extract_irrigation_from_land_output(scen)
    if irr_df is not None and not irr_df.empty:
        results["irrigation"] = irr_df

    return results


def _extract_irrigation_from_land_output(
    scen: Scenario,
) -> pd.DataFrame | None:
    """Extract irrigation water demand from GLOBIOM land_output parameter.

    GLOBIOM provides irrigation via land_output with commodities:
    - Water|Withdrawal|Irrigation|Cereals
    - Water|Withdrawal|Irrigation|Oilcrops
    - Water|Withdrawal|Irrigation|Sugarcrops

    These are at regional level (R12_*), not basin level.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with GLOBIOM linkage

    Returns
    -------
    pd.DataFrame or None
        Irrigation demand at regional level with columns [node, year, time, value]
        Returns None if land_output not available
    """
    try:
        land_out = scen.par("land_output")
    except Exception:
        return None

    if land_out.empty:
        return None

    # Filter to irrigation commodities
    irr_commodities = IRRIGATION_LAND_OUTPUT_COMMODITIES
    irr_df = land_out[land_out["commodity"].isin(irr_commodities)].copy()

    if irr_df.empty:
        return None

    # Filter to baseline land_scenario (BIO00GHG000 = no bioenergy mandate, no carbon price)
    # The land_output parameter contains all 84 GLOBIOM land_scenarios; the optimization
    # selects which one is active. For ex-ante analysis, use baseline scenario.
    baseline_scenario = "BIO00GHG000"
    if "land_scenario" in irr_df.columns:
        irr_df = irr_df[irr_df["land_scenario"] == baseline_scenario]
        if irr_df.empty:
            return None

    # Aggregate across crop types (cereals, oilcrops, sugarcrops)
    # land_output has: node, land_scenario, year, commodity, level, time, value, unit
    irr_agg = irr_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()

    # Convert km3 to MCM (land_output is typically in km3)
    # Check units - if already MCM, don't convert
    if "unit" in irr_df.columns:
        sample_unit = irr_df["unit"].iloc[0] if len(irr_df) > 0 else ""
        if "km3" in str(sample_unit).lower():
            irr_agg["value"] = irr_agg["value"] * 1000  # km3 -> MCM

    return irr_agg


def extract_basin_allocation_shares(
    scen: Scenario,
) -> pd.DataFrame:
    """Extract basin-to-region allocation shares from scenario.

    These shares define how regional water demand (irrigation, cooling) is
    allocated to basins proportionally based on water availability.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with nexus module

    Returns
    -------
    pd.DataFrame
        DataFrame with columns [region, basin, year, time, share]
        where share is the fraction of regional demand allocated to each basin
    """
    shares = scen.par("share_mode_up", {"shares": "share_basin"})

    if shares.empty:
        return pd.DataFrame(columns=["region", "basin", "year", "time", "share"])

    # Use only one technology to avoid double-counting
    shares = shares[shares["technology"] == "basin_to_reg_core"].copy()

    # Extract basin from mode (M{id}|{region} -> B{id}|{region})
    shares["basin"] = shares["mode"].str.replace("^M", "B", regex=True)

    # Rename columns for clarity
    result = shares[["node_share", "basin", "year_act", "time", "value"]].copy()
    result.columns = ["region", "basin", "year", "time", "share"]

    return result


def disaggregate_regional_to_basin(
    regional_df: pd.DataFrame,
    shares_df: pd.DataFrame,
) -> pd.DataFrame:
    """Disaggregate regional demand to basin level using allocation shares.

    Parameters
    ----------
    regional_df : pd.DataFrame
        Regional demand with columns [node, year, time, value]
        where node is R12_* region
    shares_df : pd.DataFrame
        Basin allocation shares from extract_basin_allocation_shares()

    Returns
    -------
    pd.DataFrame
        Basin-level demand with columns [node, year, time, value]
        where node is B{id}|{region} basin
    """
    if regional_df.empty or shares_df.empty:
        return pd.DataFrame(columns=["node", "year", "time", "value"])

    # Rename value column to avoid collision
    regional_renamed = regional_df.rename(columns={"value": "regional_value"})

    # Merge regional demand with basin shares
    merged = regional_renamed.merge(
        shares_df,
        left_on=["node", "year", "time"],
        right_on=["region", "year", "time"],
        how="inner",
    )

    if merged.empty:
        return pd.DataFrame(columns=["node", "year", "time", "value"])

    # Compute basin demand = regional demand * share
    merged["value"] = merged["regional_value"] * merged["share"]

    # Return basin-level result
    result = merged[["basin", "year", "time", "value"]].copy()
    result.columns = ["node", "year", "time", "value"]

    return result


def extract_water_availability(
    scen: Scenario,
) -> pd.DataFrame:
    """Extract total water availability (surface + ground) from scenario.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with nexus module

    Returns
    -------
    pd.DataFrame
        DataFrame with columns [node, year, time, surfacewater, groundwater, total]
        All values positive (MCM/year)
    """
    sw = scen.par("demand", {"commodity": "surfacewater_basin"})
    gw = scen.par("demand", {"commodity": "groundwater_basin"})

    # Negate (MESSAGE convention: negative demand = supply)
    sw = sw[["node", "year", "time", "value"]].copy()
    sw["value"] = -sw["value"]
    sw = sw.rename(columns={"value": "surfacewater"})

    gw = gw[["node", "year", "time", "value"]].copy()
    gw["value"] = -gw["value"]
    gw = gw.rename(columns={"value": "groundwater"})

    # Merge on basin-year-time
    merged = sw.merge(gw, on=["node", "year", "time"], how="outer")
    merged = merged.fillna(0)
    merged["total"] = merged["surfacewater"] + merged["groundwater"]

    return merged


def extract_sectoral_demands_from_solution(
    scen: Scenario,
    output_path: str | None = None,
) -> pd.DataFrame:
    """Extract sectoral water demands from scenario solution.

    Uses Reporter to get actual solution values (activity levels) for
    irrigation, cooling, and municipal water demands.

    Parameters
    ----------
    scen : Scenario
        Solved MESSAGE scenario with nexus module
    output_path : str, optional
        If provided, save CSV to this path

    Returns
    -------
    pd.DataFrame
        Long-form DataFrame with columns [node, year, time, sector, value]
        where value is water demand in km3/year
    """
    from message_ix import Reporter

    if not scen.has_solution():
        raise ValueError(f"Scenario {scen.scenario} has no solution")

    rep = Reporter.from_scenario(scen)

    # Get input flows from solution
    # Reporter columns: nl, t, ya, m, no, c, l, h, value
    in_flows = rep.get("in:nl-t-ya-m-h-no-c-l")
    df_in = in_flows.to_dataframe().reset_index()
    df_in = df_in.rename(columns={"nl": "node", "t": "technology", "ya": "year",
                                   "m": "mode", "h": "time", "no": "node_out",
                                   "c": "commodity", "l": "level"})

    # Get output flows from solution
    out_flows = rep.get("out:nl-t-ya-m-h-nd-c-l")
    df_out = out_flows.to_dataframe().reset_index()
    df_out = df_out.rename(columns={"nl": "node", "t": "technology", "ya": "year",
                                     "m": "mode", "h": "time", "nd": "node_dest",
                                     "c": "commodity", "l": "level"})

    results = []

    # --- Irrigation (from input flows) ---
    # in|water_supply|freshwater|irrigation_cereal|M1
    # in|water_supply|freshwater|irrigation_oilcrops|M1
    # in|water_supply|freshwater|irrigation_sugarcrops|M1
    irr_techs = ["irrigation_cereal", "irrigation_oilcrops", "irrigation_sugarcrops"]
    irr_df = df_in[
        (df_in["technology"].isin(irr_techs)) &
        (df_in["commodity"] == "freshwater") &
        (df_in["level"] == "water_supply")
    ].copy()

    if not irr_df.empty:
        irr_agg = irr_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()
        irr_agg["sector"] = "irrigation"
        results.append(irr_agg[["node", "year", "time", "sector", "value"]])

    # --- Cooling (from water_for_ppl.py) ---
    # Cooling techs have pattern {parent}__  and draw surfacewater at water_supply
    # Freshwater cooling: ot_fresh, cl_fresh (exclude ot_saline, air)
    cool_df = df_in[
        (df_in["technology"].str.contains("__", na=False)) &
        (~df_in["technology"].str.endswith(("ot_saline", "air"), na=False)) &
        (df_in["commodity"] == "surfacewater") &
        (df_in["level"] == "water_supply")
    ].copy()

    if not cool_df.empty:
        cool_agg = cool_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()
        cool_agg["sector"] = "cooling"
        results.append(cool_agg[["node", "year", "time", "sector", "value"]])

    # --- Municipal: Urban connected ---
    # out|final|urban_mw|urban_t_d|M1
    urban_df = df_out[
        (df_out["technology"] == "urban_t_d") &
        (df_out["commodity"] == "urban_mw") &
        (df_out["level"] == "final")
    ].copy()

    if not urban_df.empty:
        urban_agg = urban_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()
        urban_agg["sector"] = "urban"
        results.append(urban_agg[["node", "year", "time", "sector", "value"]])

    # --- Municipal: Rural connected ---
    # out|final|rural_mw|rural_t_d|M1
    rural_df = df_out[
        (df_out["technology"] == "rural_t_d") &
        (df_out["commodity"] == "rural_mw") &
        (df_out["level"] == "final")
    ].copy()

    if not rural_df.empty:
        rural_agg = rural_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()
        rural_agg["sector"] = "rural"
        results.append(rural_agg[["node", "year", "time", "sector", "value"]])

    # --- Manufacturing/Industry ---
    # out|final|industry_mw|industry_unconnected|M1
    mfg_df = df_out[
        (df_out["commodity"] == "industry_mw") &
        (df_out["level"] == "final")
    ].copy()

    if not mfg_df.empty:
        mfg_agg = mfg_df.groupby(["node", "year", "time"], as_index=False)["value"].sum()
        mfg_agg["sector"] = "manufacturing"
        results.append(mfg_agg[["node", "year", "time", "sector", "value"]])

    # Combine all sectors
    if results:
        demand_df = pd.concat(results, ignore_index=True)
    else:
        demand_df = pd.DataFrame(columns=["node", "year", "time", "sector", "value"])

    # Save if path provided
    if output_path:
        demand_df.to_csv(output_path, index=False)
        print(f"Saved demands to {output_path}")

    return demand_df
