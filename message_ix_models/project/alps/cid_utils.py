"""Common utilities for CID (Climate Impact Driver) workflows.

Shared functions for water CID replacement scripts (annual and seasonal).
"""

from pathlib import Path
import pandas as pd
import numpy as np
from diskcache import FanoutCache

from ixmp import Platform
from message_ix import Scenario
from message_ix_models.project.alps.rime import (
    predict_rime,
    _get_gmt_ensemble,
    get_rime_dataset_path,
    extract_all_run_ids,
    load_basin_mapping,
)
from message_ix_models.project.alps.constants import MAGICC_OUTPUT_DIR

# Cache setup
CACHE_DIR = Path(__file__).parent / ".cache" / "rime_predictions"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
cache = FanoutCache(str(CACHE_DIR), shards=8)


def extract_region_code(node: str) -> str:
    """Extract short region code from MESSAGE node name.

    Parameters
    ----------
    node : str
        MESSAGE node name (e.g., 'R12_AFR', 'AFR')

    Returns
    -------
    str
        Short region code (e.g., 'AFR')
    """
    return node[4:] if node.startswith("R12_") else node


def get_magicc_file(model: str, scenario: str) -> Path:
    """Get path to MAGICC output file for a model/scenario.

    Parameters
    ----------
    model : str
        Model name (e.g., 'SSP_SSP2_v6.4')
    scenario : str
        Scenario name (e.g., 'baseline', 'baseline_1000f')

    Returns
    -------
    Path
        Path to MAGICC all_runs Excel file
    """
    filename = f"{model}_{scenario}_magicc_all_runs.xlsx"
    return MAGICC_OUTPUT_DIR / filename


def cached_rime_prediction(
    magicc_df: pd.DataFrame,
    run_ids: tuple,
    variable: str,
    temporal_res: str = "annual"
) -> np.ndarray:
    """Cached RIME expectation prediction.

    Extracts GMT ensemble from MAGICC, runs predict_rime with 2D input,
    and returns E[RIME(GMT_i)] as ndarray.

    Parameters
    ----------
    magicc_df : pd.DataFrame
        MAGICC output DataFrame (IAMC format). Loaded once at top level.
    run_ids : tuple
        Run IDs to process (must be tuple for hashing)
    variable : str
        Variable to predict ('qtot_mean', 'qr', 'capacity_factor', 'EI_cool', 'EI_heat')
    temporal_res : str
        Temporal resolution ('annual' or 'seasonal2step')
        Note: 'capacity_factor' and 'EI_*' only support 'annual'

    Returns
    -------
    np.ndarray or tuple
        For annual: ndarray (217, n_years) for basin vars, (12, n_years) for regional
        For seasonal: tuple (dry, wet) where each is (217, n_years)
    """
    source_name = magicc_df["Scenario"].iloc[0] if "Scenario" in magicc_df.columns else "unknown"
    cache_key = f"{source_name}_{variable}_{temporal_res}_{len(run_ids)}runs_{hash(run_ids)}_v2"

    if cache_key in cache:
        print(f"   Cache hit for {variable} ({temporal_res})")
        return cache[cache_key]

    print(f"   Cache miss for {variable} ({temporal_res}) - computing predictions...")

    # Extract GMT as 2D array
    gmt_dict, years = _get_gmt_ensemble(magicc_df, list(run_ids))
    gmt_2d = np.array([gmt_dict[rid] for rid in run_ids])

    # Run predict_rime with 2D input → returns E[RIME(GMT_i)]
    result = predict_rime(gmt_2d, variable, temporal_res)

    cache[cache_key] = result
    return result


def load_scenario_for_cid(
    platform_name: str,
    model: str,
    scenario: str,
    clone_without_solution: bool = True
) -> Scenario:
    """Load and validate MESSAGE scenario for CID replacement.

    Parameters
    ----------
    platform_name : str
        ixmp platform name (e.g., 'ixmp_dev', 'local')
    model : str
        MESSAGE model name
    scenario : str
        MESSAGE scenario name
    clone_without_solution : bool
        If True and scenario has solution, clone without it

    Returns
    -------
    Scenario
        Loaded scenario ready for CID replacement

    Raises
    ------
    ValueError
        If scenario lacks required water parameters (nexus module)
    """
    print(f"Loading scenario from {platform_name}...")
    mp = Platform(platform_name)
    scen = Scenario(mp, model, scenario)
    print(f"   Loaded: {scen.model} / {scen.scenario} (version {scen.version})")
    print(f"   Has solution: {scen.has_solution()}")

    # Clone without solution if needed
    if scen.has_solution() and clone_without_solution:
        print("   Cloning scenario without solution...")
        scen = scen.clone(keep_solution=False)
        print(f"   Cloned to version {scen.version}")

    # Verify nexus module exists
    verify_nexus_module(scen)

    return scen


def verify_nexus_module(scen: Scenario) -> None:
    """Verify scenario has nexus module (water availability parameters).

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario to verify

    Raises
    ------
    ValueError
        If required water parameters are missing
    """
    try:
        existing_sw = scen.par("demand", {"commodity": "surfacewater_basin"})
        existing_gw = scen.par("demand", {"commodity": "groundwater_basin"})
        print(
            f"   Scenario has nexus module "
            f"({len(existing_sw)} surfacewater, {len(existing_gw)} groundwater rows)"
        )
    except Exception as e:
        raise ValueError(f"Scenario missing water parameters (nexus module): {e}")


def verify_timeslices(scen: Scenario, expected_times: set = None) -> bool:
    """Verify scenario has required timeslices.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario to verify
    expected_times : set, optional
        Expected timeslice names (default: {'h1', 'h2'})

    Returns
    -------
    bool
        True if timeslices are present
    """
    if expected_times is None:
        expected_times = {'h1', 'h2'}

    time_set = set(scen.set("time").tolist())
    subannual_times = time_set - {'year'}

    if expected_times <= subannual_times:
        print(f"   Scenario has timeslices: {subannual_times}")
        return True
    else:
        print(f"   Scenario timeslices: {subannual_times}")
        print(f"   Missing required: {expected_times - subannual_times}")
        return False


def verify_parameter_structure(
    sw_demand: pd.DataFrame,
    gw_demand: pd.DataFrame,
    gw_share: pd.DataFrame,
    temporal_res: str = "annual"
) -> None:
    """Verify CID parameters have correct structure.

    Parameters
    ----------
    sw_demand : pd.DataFrame
        Surfacewater demand parameter
    gw_demand : pd.DataFrame
        Groundwater demand parameter
    gw_share : pd.DataFrame
        Groundwater share parameter
    temporal_res : str
        Expected temporal resolution ('annual' or 'seasonal')

    Raises
    ------
    AssertionError
        If parameter structure is invalid
    """
    # Check columns
    demand_cols = {"node", "commodity", "level", "year", "time", "value", "unit"}
    share_cols = {"shares", "node_share", "year_act", "time", "value", "unit"}

    assert set(sw_demand.columns) == demand_cols, f"sw_demand columns: {set(sw_demand.columns)}"
    assert set(gw_demand.columns) == demand_cols, f"gw_demand columns: {set(gw_demand.columns)}"
    assert set(gw_share.columns) == share_cols, f"gw_share columns: {set(gw_share.columns)}"

    # Check time values
    if temporal_res == "annual":
        expected_times = {'year'}
    else:
        expected_times = {'h1', 'h2'}

    assert set(sw_demand['time'].unique()) == expected_times, f"sw_demand time: {sw_demand['time'].unique()}"
    assert set(gw_demand['time'].unique()) == expected_times, f"gw_demand time: {gw_demand['time'].unique()}"
    assert set(gw_share['time'].unique()) == expected_times, f"gw_share time: {gw_share['time'].unique()}"

    print("   Parameter structure verified")


def report_nan_values(
    sw_demand: pd.DataFrame,
    gw_demand: pd.DataFrame,
    gw_share: pd.DataFrame
) -> int:
    """Report NaN values in CID parameters.

    Returns
    -------
    int
        Total number of NaN values across all parameters
    """
    nan_sw = sw_demand["value"].isna().sum()
    nan_gw = gw_demand["value"].isna().sum()
    nan_share = gw_share["value"].isna().sum()

    if nan_sw > 0:
        print(f"   Warning: {nan_sw} NaN values in sw_demand")
    if nan_gw > 0:
        print(f"   Warning: {nan_gw} NaN values in gw_demand")
    if nan_share > 0:
        print(f"   Warning: {nan_share} NaN values in gw_share")

    total_nan = nan_sw + nan_gw + nan_share
    if total_nan == 0:
        print("   No NaN values detected")

    return total_nan


def report_value_ranges(
    sw_demand: pd.DataFrame,
    gw_demand: pd.DataFrame,
    gw_share: pd.DataFrame
) -> None:
    """Report value ranges for CID parameters."""
    print(f"   sw_demand: [{sw_demand['value'].min():.2f}, {sw_demand['value'].max():.2f}] MCM/year")
    print(f"   gw_demand: [{gw_demand['value'].min():.2f}, {gw_demand['value'].max():.2f}] MCM/year")
    print(f"   gw_share:  [{gw_share['value'].min():.4f}, {gw_share['value'].max():.4f}] (fraction)")


def clear_cache() -> None:
    """Clear the RIME prediction cache."""
    print(f"Clearing cache at {CACHE_DIR}...")
    cache.clear()
    print("   Cache cleared")
