"""Basin sensitivity analysis: water/cost response to GMT.

Computes per-basin sensitivity (slope) of water availability or costs
with respect to global mean temperature, using the exact GMT point estimates
from the CID scenario generation pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable

import numpy as np
import pandas as pd
from diskcache import FanoutCache
from scipy import stats

from message_ix_models.util import package_data_path

# MAGICC output directory
MAGICC_OUTPUT_DIR = package_data_path("report", "legacy", "reporting_output", "magicc_output")

# Cache setup for GMT lookups
CACHE_DIR = Path(__file__).parent.parent / ".cache" / "gmt_lookups"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
_gmt_cache = FanoutCache(str(CACHE_DIR), shards=4)


def get_magicc_file_for_scenario(scenario_name: str, model_prefix: str = "SSP_SSP2_v6.5_CID") -> Path:
    """Map MESSAGE scenario name to MAGICC output file.

    Parameters
    ----------
    scenario_name : str
        MESSAGE scenario name (e.g., 'nexus_baseline_600f_annual')
    model_prefix : str
        Model prefix for MAGICC files

    Returns
    -------
    Path
        Path to MAGICC all_runs file
    """
    # Extract budget from scenario name
    # nexus_baseline_annual -> baseline
    # nexus_baseline_600f_annual -> baseline_600f
    parts = scenario_name.replace("nexus_", "").replace("_annual", "").replace("_seasonal", "")

    magicc_file = MAGICC_OUTPUT_DIR / f"{model_prefix}_{parts}_magicc_all_runs.xlsx"
    return magicc_file


def extract_expected_gmt_for_scenario(
    scenario_name: str,
    model_prefix: str = "SSP_SSP2_v6.5_CID",
    n_runs: int = 100,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Extract expected GMT trajectory for a scenario - matches CID pipeline exactly.

    Uses the same approach as scenario_generator.py:
    1. Load MAGICC file
    2. Extract GMT for runs 0 to n_runs-1
    3. Average across runs

    Parameters
    ----------
    scenario_name : str
        MESSAGE scenario name
    model_prefix : str
        Model prefix for MAGICC files
    n_runs : int
        Number of MAGICC runs to average (default: 100, matching CID pipeline)
    use_cache : bool
        Whether to use disk cache (default: True)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['year', 'gmt'] for MESSAGE model years
    """
    cache_key = f"gmt_{model_prefix}_{scenario_name}_{n_runs}runs"

    if use_cache and cache_key in _gmt_cache:
        return _gmt_cache[cache_key]

    from ..rime import extract_temperature_timeseries, extract_all_run_ids

    magicc_file = get_magicc_file_for_scenario(scenario_name, model_prefix)

    if not magicc_file.exists():
        raise FileNotFoundError(f"MAGICC file not found: {magicc_file}")

    magicc_df = pd.read_excel(magicc_file, sheet_name='data')

    # Get run IDs (same as CID pipeline)
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = all_run_ids[:n_runs]

    # Extract GMT for each run
    gmt_trajectories = []
    years = None
    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_trajectories.append(temp_df["gsat_anomaly_K"].values)
        if years is None:
            years = temp_df["year"].values

    # Average across runs (expectation)
    gmt_array = np.array(gmt_trajectories)  # (n_runs, n_years)
    gmt_expected = gmt_array.mean(axis=0)  # (n_years,)

    # Build DataFrame
    result = pd.DataFrame({
        'year': years,
        'gmt': gmt_expected,
    })

    # Filter to MESSAGE model years
    message_years = [2020, 2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100, 2110]
    result = result[result['year'].isin(message_years)].copy()

    if use_cache:
        _gmt_cache[cache_key] = result

    return result


def build_gmt_scenario_mapping(
    scenario_names: list[str],
    model_prefix: str = "SSP_SSP2_v6.5_CID",
    n_runs: int = 100,
    use_cache: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """Build mapping from (scenario, year) to expected GMT.

    Parameters
    ----------
    scenario_names : list[str]
        List of MESSAGE scenario names
    model_prefix : str
        Model prefix for MAGICC files
    n_runs : int
        Number of MAGICC runs for expectation
    use_cache : bool
        Whether to use disk cache (default: True)
    verbose : bool
        Whether to print progress (default: True)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['scenario', 'year', 'gmt']
    """
    # Check cache for full mapping
    scenarios_key = "_".join(sorted(scenario_names))
    cache_key = f"gmt_mapping_{model_prefix}_{n_runs}runs_{hash(scenarios_key)}"

    if use_cache and cache_key in _gmt_cache:
        if verbose:
            print(f"  Cache hit for GMT mapping ({len(scenario_names)} scenarios)")
        return _gmt_cache[cache_key]

    records = []

    for scen in scenario_names:
        try:
            gmt_df = extract_expected_gmt_for_scenario(scen, model_prefix, n_runs, use_cache)
            for _, row in gmt_df.iterrows():
                records.append({
                    'scenario': scen,
                    'year': int(row['year']),
                    'gmt': row['gmt'],
                })
            if verbose:
                print(f"  {scen}: GMT range [{gmt_df['gmt'].min():.2f}, {gmt_df['gmt'].max():.2f}]K")
        except FileNotFoundError as e:
            if verbose:
                print(f"  Warning: {e}")
            continue

    result = pd.DataFrame(records)

    if use_cache and not result.empty:
        _gmt_cache[cache_key] = result

    return result


def compute_basin_sensitivity(
    water_data: dict[str, pd.DataFrame],
    gmt_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-basin sensitivity (slope) of water vs GMT.

    Parameters
    ----------
    water_data : dict[str, pd.DataFrame]
        Mapping from scenario name to wide-format DataFrame (basins x years)
        with basin codes as index and years as columns
    gmt_mapping : pd.DataFrame
        DataFrame with columns ['scenario', 'year', 'gmt']

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['basin', 'slope', 'intercept', 'r_squared', 'p_value', 'n_points']
        Slope units: value_units / K (e.g., MCM/K for water)
    """
    # Collect all (basin, gmt, value) tuples
    records = []

    for scenario, df in water_data.items():
        # Get GMT values for this scenario
        scen_gmt = gmt_mapping[gmt_mapping['scenario'] == scenario]
        if scen_gmt.empty:
            continue

        gmt_by_year = dict(zip(scen_gmt['year'], scen_gmt['gmt']))

        # Iterate over basins (rows) and years (columns)
        for basin in df.index:
            for col in df.columns:
                try:
                    year = int(col)
                except (ValueError, TypeError):
                    continue

                if year not in gmt_by_year:
                    continue

                value = df.loc[basin, col]
                if pd.isna(value):
                    continue

                records.append({
                    'basin': basin,
                    'gmt': gmt_by_year[year],
                    'value': value,
                })

    if not records:
        return pd.DataFrame(columns=['basin', 'slope', 'intercept', 'r_squared', 'p_value', 'n_points'])

    pool_df = pd.DataFrame(records)

    # Fit regression per basin
    results = []
    for basin, group in pool_df.groupby('basin'):
        if len(group) < 3:
            continue

        x = group['gmt'].values
        y = group['value'].values

        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

        results.append({
            'basin': basin,
            'slope': slope,
            'intercept': intercept,
            'r_squared': r_value ** 2,
            'p_value': p_value,
            'n_points': len(group),
            'std_err': std_err,
        })

    return pd.DataFrame(results)


def compute_sensitivity_from_scenarios(
    scenarios: dict,
    variable: str,
    model_prefix: str = "SSP_SSP2_v6.5_CID",
    n_runs: int = 100,
    use_cache: bool = True,
) -> pd.DataFrame:
    """End-to-end sensitivity computation from loaded scenarios.

    Parameters
    ----------
    scenarios : dict[str, Scenario]
        Loaded MESSAGE scenarios
    variable : str
        'surfacewater', 'groundwater', or 'costs'
    model_prefix : str
        Model prefix for MAGICC files
    n_runs : int
        Number of MAGICC runs for expectation
    use_cache : bool
        Whether to use disk cache for GMT lookups (default: True)

    Returns
    -------
    pd.DataFrame
        Per-basin sensitivity results
    """
    from . import extract_water_cids, extract_nodal_costs, pivot_to_wide

    # Extract data based on variable type
    print(f"Extracting {variable} data...")
    if variable in ("surfacewater", "groundwater"):
        cids = extract_water_cids(scenarios)
        wide_data = {}
        for name, params in cids.items():
            df = params[variable]
            wide_data[name] = pivot_to_wide(df, "node", "year", "value")
    elif variable == "costs":
        costs = extract_nodal_costs(scenarios)
        wide_data = {}
        for name, df in costs.items():
            wide_data[name] = pivot_to_wide(df, "node", "year", "cost")
    else:
        raise ValueError(f"Unknown variable: {variable}. Use 'surfacewater', 'groundwater', or 'costs'")

    # Build GMT mapping
    print("Loading expected GMT from MAGICC files...")
    gmt_mapping = build_gmt_scenario_mapping(
        list(scenarios.keys()),
        model_prefix,
        n_runs,
        use_cache=use_cache,
    )
    print(f"  Loaded GMT for {len(gmt_mapping)} (scenario, year) pairs")

    # Compute sensitivity
    print("Computing per-basin sensitivity...")
    sensitivity = compute_basin_sensitivity(wide_data, gmt_mapping)
    print(f"  Computed sensitivity for {len(sensitivity)} basins")

    return sensitivity


def compute_sensitivity_from_data(
    data: dict[str, pd.DataFrame],
    gmt_mapping: pd.DataFrame,
    index_col: str = "node",
    year_col: str = "year",
    value_col: str = "value",
) -> pd.DataFrame:
    """Compute sensitivity from pre-extracted data.

    Lower-level function for custom data extraction workflows.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Mapping from scenario name to long-format DataFrame
    gmt_mapping : pd.DataFrame
        DataFrame with columns ['scenario', 'year', 'gmt']
    index_col : str
        Column name for entity index (e.g., 'node', 'basin')
    year_col : str
        Column name for year
    value_col : str
        Column name for values

    Returns
    -------
    pd.DataFrame
        Per-entity sensitivity results
    """
    from . import pivot_to_wide

    wide_data = {}
    for name, df in data.items():
        wide_data[name] = pivot_to_wide(df, index_col, year_col, value_col)

    return compute_basin_sensitivity(wide_data, gmt_mapping)


def clear_gmt_cache() -> None:
    """Clear the GMT lookup cache."""
    print(f"Clearing GMT cache at {CACHE_DIR}...")
    _gmt_cache.clear()
    print("  Cache cleared")


def compute_rime_sensitivity(
    variable: str,
    temporal_res: str = "annual",
    gmt_min: float = 0.6,
    gmt_max: float = 7.4,
    n_points: int = 100,
    hydro_model: str = "CWatM",
) -> pd.DataFrame:
    """Compute per-basin sensitivity directly from RIME emulator.

    Samples GMT range uniformly and regresses RIME predictions against GMT.
    This gives the fundamental emulator sensitivity without scenario filtering.

    Parameters
    ----------
    variable : str
        RIME variable ('qtot_mean' or 'qr')
    temporal_res : str
        'annual' or 'seasonal2step'
    gmt_min : float
        Minimum GMT in K (default: 0.6, RIME lower bound)
    gmt_max : float
        Maximum GMT in K (default: 7.4, RIME upper bound)
    n_points : int
        Number of GMT sample points (default: 100)
    hydro_model : str
        Hydrological model ('CWatM' or 'H08')

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['basin', 'slope', 'intercept', 'r_squared', 'p_value', 'n_points', 'std_err']
        For seasonal: separate rows for dry/wet with basin suffix (e.g., 'B107|AFR_dry')
    """
    from ..rime import predict_rime, load_basin_mapping

    # Load basin mapping for codes
    basin_df = load_basin_mapping()
    basin_codes = basin_df["basin_code"].tolist()

    # Sample GMT range
    gmt_range = np.linspace(gmt_min, gmt_max, n_points)

    # Get RIME predictions and convert km³ → MCM (×1000)
    predictions = predict_rime(gmt_range, variable, temporal_res=temporal_res, hydro_model=hydro_model)
    if variable in ("qtot_mean", "qr"):
        if temporal_res == "annual":
            predictions = predictions * 1000
        else:
            predictions = (predictions[0] * 1000, predictions[1] * 1000)

    def _regress_basin(basin_idx: int, basin_code: str, values: np.ndarray, suffix: str = "") -> dict:
        """Fit linear regression for one basin."""
        valid = ~np.isnan(values)
        if valid.sum() < 3:
            return None

        x = gmt_range[valid]
        y = values[valid]
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

        return {
            'basin': f"{basin_code}{suffix}",
            'slope': slope,
            'intercept': intercept,
            'r_squared': r_value ** 2,
            'p_value': p_value,
            'n_points': int(valid.sum()),
            'std_err': std_err,
        }

    results = []

    if temporal_res == "annual":
        # predictions is (217, n_points)
        for basin_idx, basin_code in enumerate(basin_codes):
            result = _regress_basin(basin_idx, basin_code, predictions[basin_idx, :])
            if result:
                results.append(result)
    else:
        # seasonal: predictions is tuple (dry, wet) each (217, n_points)
        dry_preds, wet_preds = predictions
        for basin_idx, basin_code in enumerate(basin_codes):
            dry_result = _regress_basin(basin_idx, basin_code, dry_preds[basin_idx, :], "_dry")
            wet_result = _regress_basin(basin_idx, basin_code, wet_preds[basin_idx, :], "_wet")
            if dry_result:
                results.append(dry_result)
            if wet_result:
                results.append(wet_result)

    return pd.DataFrame(results)
