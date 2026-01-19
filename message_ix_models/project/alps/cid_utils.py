"""Common utilities for CID (Climate Impact Driver) workflows.

Shared functions for water CID replacement scripts (annual and seasonal).
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from diskcache import FanoutCache
from ixmp import Platform
from message_ix import Scenario

from message_ix_models.project.alps.constants import MAGICC_OUTPUT_DIR, MESSAGE_YEARS
from message_ix_models.project.alps.rime import (
    get_gmt_ensemble,
    predict_rime,
)

log = logging.getLogger(__name__)

# Cache setup
CACHE_DIR = Path(__file__).parent / ".cache" / "rime_predictions"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
cache = FanoutCache(str(CACHE_DIR), shards=8)


def sample_to_message_years(
    df: pd.DataFrame,
    id_cols: list[str],
    method: str = "point",
) -> pd.DataFrame:
    """Sample annual data to MESSAGE timesteps.

    Parameters
    ----------
    df : pd.DataFrame
        Wide DataFrame with annual year columns (2020-2100) and ID columns
    id_cols : list of str
        Non-year columns to preserve (e.g., ['BCU_name'], ['region'])
    method : str
        'point' - take value at MESSAGE year
        'average' - average preceding period (e.g., 2026-2030 for timestep 2030)

    Returns
    -------
    pd.DataFrame
        DataFrame with MESSAGE_YEARS columns (2020-2110)
    """
    msg_years_input = MESSAGE_YEARS[:-1]  # 2020-2100, input doesn't have 2110

    if method == "point":
        result = df[id_cols + msg_years_input].copy()
    elif method == "average":
        result = df[id_cols].copy()
        for i, y in enumerate(msg_years_input):
            start = MESSAGE_YEARS[i - 1] + 1 if i > 0 else 2020
            result[y] = df[list(range(start, y + 1))].mean(axis=1)
    else:
        raise ValueError(f"method must be 'point' or 'average', got {method}")

    result[2110] = result[2100]
    return result


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
    magicc_df: pd.DataFrame, run_ids: tuple, variable: str, temporal_res: str = "annual"
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
    source_name = (
        magicc_df["Scenario"].iloc[0] if "Scenario" in magicc_df.columns else "unknown"
    )
    cache_key = (
        f"{source_name}_{variable}_{temporal_res}_{len(run_ids)}runs_{hash(run_ids)}_v2"
    )

    if cache_key in cache:
        log.debug(f"Cache hit for {variable} ({temporal_res})")
        return cache[cache_key]

    log.info(f"Cache miss for {variable} ({temporal_res}) - computing predictions...")

    # Extract GMT as 2D array
    gmt_dict, years = get_gmt_ensemble(magicc_df, list(run_ids))
    gmt_2d = np.array([gmt_dict[rid] for rid in run_ids])

    # Run predict_rime with 2D input → returns E[RIME(GMT_i)]
    result = predict_rime(gmt_2d, variable, temporal_res)

    cache[cache_key] = result
    return result
