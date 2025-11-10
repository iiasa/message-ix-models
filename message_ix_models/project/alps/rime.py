"""RIME processing with importance weighting and CVaR calculation.

Core functions for:
- Extracting temperature timeseries from MAGICC output
- Running vectorized RIME predictions across multiple runs
- Computing weighted expectations and CVaR risk metrics
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional

from rime.rime_functions import predict_from_gmt
from .weighted_cvar import compute_weighted_cvar


def extract_temperature_timeseries(magicc_df: pd.DataFrame,
                                   percentile: Optional[float] = None,
                                   run_id: Optional[int] = None) -> pd.DataFrame:
    """Extract GSAT temperature trajectory from MAGICC DataFrame.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)
        percentile: Which percentile to use (5, 10, 50, 90, 95, etc.)
        run_id: Specific run_id to extract (0-599). Takes precedence over percentile.

    Returns:
        DataFrame with columns: ['year', 'gsat_anomaly_K', 'model', 'scenario', 'ssp_family']
    """
    # Determine selection mode
    if run_id is not None:
        # Select by run_id
        model_pattern = f"|run_{run_id}"
        var_pattern = "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"

        temp_data = magicc_df[
            (magicc_df['Model'].str.contains(model_pattern, na=False, regex=False)) &
            (magicc_df['Variable'].str.contains(var_pattern, na=False, regex=False))
        ]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for run_id {run_id}")
    else:
        # Select by percentile
        if percentile is None:
            percentile = 50.0
        percentile_str = f"{percentile}th Percentile" if percentile != 50.0 else "50.0th Percentile"
        var_pattern = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{percentile_str}"

        temp_data = magicc_df[magicc_df['Variable'] == var_pattern]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for percentile {percentile}")

    temp_row = temp_data.iloc[0]

    # Extract year columns (strings like '1990', '2000', etc.)
    year_cols = [col for col in magicc_df.columns
                 if isinstance(col, str) and col.isdigit()]

    temps = {int(year): temp_row[year]
             for year in year_cols
             if pd.notna(temp_row[year])}

    temp_df = pd.DataFrame({
        'year': list(temps.keys()),
        'gsat_anomaly_K': list(temps.values())
    })

    # Extract metadata
    scenario_name = temp_row['Scenario']
    model_name = temp_row['Model']

    # Determine SSP family
    scenario_lower = scenario_name.lower()
    if 'ssp1' in scenario_lower:
        ssp_family = 'SSP1'
    elif 'ssp2' in scenario_lower:
        ssp_family = 'SSP2'
    elif 'ssp3' in scenario_lower:
        ssp_family = 'SSP3'
    elif 'ssp4' in scenario_lower:
        ssp_family = 'SSP4'
    elif 'ssp5' in scenario_lower:
        ssp_family = 'SSP5'
    else:
        ssp_family = 'SSP2'  # default

    temp_df['model'] = model_name
    temp_df['scenario'] = scenario_name
    temp_df['ssp_family'] = ssp_family

    return temp_df


def extract_all_run_ids(magicc_df: pd.DataFrame) -> list[int]:
    """Extract all available run_ids from MAGICC DataFrame.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)

    Returns:
        Sorted list of run_ids
    """
    gsat_data = magicc_df[
        magicc_df['Variable'].str.contains(
            'AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3',
            na=False,
            regex=False
        )
    ]

    def parse_run_id(model_str):
        if '|run_' in model_str:
            try:
                return int(model_str.split('|run_')[1].split('|')[0])
            except (IndexError, ValueError):
                return None
        return None

    run_ids = [parse_run_id(m) for m in gsat_data['Model'].unique()]
    run_ids = [r for r in run_ids if r is not None]

    return sorted(run_ids)


def batch_rime_predictions(magicc_df: pd.DataFrame,
                           run_ids: list[int],
                           dataset_path: Path,
                           basin_mapping: pd.DataFrame,
                           variable: str) -> dict[int, pd.DataFrame]:
    """Run RIME predictions on multiple runs using vectorized GMT lookups.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame
        variable: Variable name (qtot_mean or qr)

    Returns:
        Dictionary mapping run_id -> MESSAGE format DataFrame (217 rows × year columns)
    """
    # Extract GMT timeseries for all runs
    gmt_timeseries = []
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_timeseries.append(temp_df['gsat_anomaly_K'].values)
        if years is None:
            years = temp_df['year'].values

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array(gmt_timeseries)
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Call predict_from_gmt for each unique GMT value (cached dataset)
    # Results: (n_runs * n_years) × n_basins
    predictions_flat = np.array([
        predict_from_gmt(gmt, str(dataset_path), variable)
        for gmt in gmt_flat
    ])

    # Reshape back to 3D: (n_runs × n_years × n_basins)
    n_basins = predictions_flat.shape[1]
    predictions_3d = predictions_flat.reshape(n_runs, n_years, n_basins)

    # Convert each run to MESSAGE format DataFrame
    results = {}
    for i, run_id in enumerate(run_ids):
        # predictions_3d[i]: (n_years × n_basins)
        df = pd.DataFrame(
            predictions_3d[i].T,  # Transpose to (n_basins × n_years)
            columns=years
        )
        results[run_id] = df

    return results


def compute_expectation(predictions: dict[int, pd.DataFrame],
                        run_ids: np.ndarray,
                        weights: Optional[np.ndarray] = None) -> pd.DataFrame:
    """Compute expectation across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        run_ids: Array of run IDs
        weights: Optional array of importance weights (must sum to ~1.0).
                 If None, uses uniform weights (unweighted mean).

    Returns:
        DataFrame with (weighted) mean predictions (MESSAGE format)
    """
    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    first_pred = predictions[run_ids[0]]
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(first_pred.columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id].values

    # Compute (weighted) mean
    if weights is not None:
        mean = np.average(values_3d, axis=0, weights=weights)
    else:
        mean = np.mean(values_3d, axis=0)

    # Convert back to DataFrame
    result = pd.DataFrame(
        mean,
        index=first_pred.index,
        columns=first_pred.columns
    )

    return result


def compute_rime_cvar(predictions: dict[int, pd.DataFrame],
                      weights: np.ndarray,
                      run_ids: np.ndarray,
                      cvar_levels: list[float] = [10, 50, 90]) -> dict[str, pd.DataFrame]:
    """Compute weighted CVaR across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        weights: Array of importance weights (must sum to ~1.0)
        run_ids: Array of run IDs corresponding to weights
        cvar_levels: List of CVaR percentiles (default: [10, 50, 90])

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', 'cvar_90'
        Each value is a DataFrame (MESSAGE format: n_basins × year columns)
    """
    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    first_pred = predictions[run_ids[0]]
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(first_pred.columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id].values

    # Get basin indices and year columns for DataFrame output
    basin_ids = list(first_pred.index)
    year_columns = list(first_pred.columns)

    # Compute weighted CVaR
    cvar_results = compute_weighted_cvar(
        values_3d,
        weights,
        cvar_levels,
        basin_ids=basin_ids,
        year_columns=year_columns
    )

    return cvar_results
