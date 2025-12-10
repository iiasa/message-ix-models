"""RIME processing with CVaR calculation.

Core functions for:
- Extracting temperature timeseries from MAGICC output
- Running vectorized RIME predictions across multiple runs
- Computing expectations and CVaR risk metrics

IMPORTANT: RIME Emulator GMT Range Limitations
----------------------------------------------
All RIME emulators (annual and seasonal) have empirical support for GMT range: 0.6°C to 7.4°C

Analysis of MAGICC output shows ~1% of GMT observations fall below 0.6°C, primarily
in early years (1995-2002) where some runs have GMT as low as 0.34°C. These values
are out of support and the emulators fail to interpolate outside this range.

Mitigation strategy:
- Clip GMT values below threshold to [0.6°C, 0.9°C] for annual emulators
- Clip GMT values below threshold to [0.8°C, 1.2°C] for seasonal emulators
  (seasonal emulators have 87% NaN coverage for many basins at 0.6-0.7°C)
- Add skewed noise using beta(2,5) distribution to avoid artifacts at boundary
- This ensures predictions remain within the emulators' support domain

By starting predictions at 2025, we avoid most problematic early years where
GMT < 0.6°C occurs.
"""

import numpy as np
import pandas as pd
import xarray as xr
from functools import lru_cache
from pathlib import Path
from typing import Optional, Literal, Dict

from message_ix_models.util import package_data_path
from .cvar import compute_cvar

# START YEAR FOR RIME PREDICTIONS
# Set to 2025 to avoid early years with GMT < 0.6°C (minimum GMT with empirical support)
START_YEAR = 2025

# RIME datasets directory
RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")


# ==============================================================================
# Core RIME Prediction (from rime_functions.py)
# ==============================================================================


@lru_cache(maxsize=8)
def _load_rime_dataset(dataset_path: str) -> xr.Dataset:
    """Cached dataset loading."""
    return xr.open_dataset(dataset_path)


def predict_from_gmt(
    gmt: float,
    dataset_path: str,
    variable: str,
    sel: Optional[Dict[str, any]] = None
) -> any:
    """Predict from GMT value with optional dimension selection.

    Args:
        gmt: Global mean temperature (float, C above pre-industrial)
        dataset_path: Path to RIME NetCDF dataset
        variable: Variable name (e.g., 'qtot_mean', 'qr', 'EI_ac_m2')
        sel: Optional dict of dimension selections (e.g., {'region': 'R12_AFR', 'arch': 'SFH'})
             Applied before gwl interpolation to reduce dimensionality

    Returns:
        Array of predictions. Shape depends on variable and selections.
    """
    ds = _load_rime_dataset(dataset_path)
    data = ds[variable]

    if sel is not None:
        for dim, value in sel.items():
            if dim in data.dims:
                data = data.sel({dim: value})

    predictions = data.sel(gwl=gmt, method='nearest').values
    return predictions


# ==============================================================================
# Variable Type Classification
# ==============================================================================


def _get_variable_type(variable: str) -> Literal["basin", "regional", "building"]:
    """Classify RIME variable by dimensionality.

    Args:
        variable: Variable name (e.g., 'qtot_mean', 'capacity_factor', 'EI_cool')

    Returns:
        'basin': Basin-level hydrology (qtot_mean, qr, local_temp)
        'regional': Regional aggregates (capacity_factor)
        'building': Building energy intensity (EI_cool, EI_heat)
    """
    if variable in ("qtot_mean", "qr", "local_temp"):
        return "basin"
    elif variable == "capacity_factor":
        return "regional"
    elif variable.startswith("EI_"):
        return "building"
    else:
        raise ValueError(
            f"Unknown variable type: {variable}. "
            f"Expected: qtot_mean, qr, local_temp, capacity_factor, EI_cool, EI_heat"
        )


def split_basin_macroregion(
    rime_predictions: np.ndarray, basin_mapping: Optional[pd.DataFrame] = None
) -> np.ndarray:
    """Expand RIME predictions (157 basins) to MESSAGE format (217 rows) by area-weighted splitting.

    RIME emulators predict at basin level (157 unique basins), but MESSAGE R12 representation
    has 217 rows because some basins span multiple macroregions. This function splits basin
    predictions proportionally by the area of each basin-region fragment.

    Args:
        rime_predictions: RIME predictions array from predict_from_gmt
            Shape: (157, n_timesteps) for annual
                   (157, n_timesteps, n_seasons) for seasonal
        basin_mapping: Basin mapping DataFrame (R12 basins only, reset index)
            Must have columns: ['BASIN_ID', 'area_km2']
            If None, loads and filters all_basins.csv to R12

    Returns:
        message_predictions: Expanded predictions array
            Shape: (217, n_timesteps) for annual
                   (217, n_timesteps, n_seasons) for seasonal
            NOTE: Basins 0, 141, 154 have missing RIME data (filled with NaN)

    Examples:
        # RIME predicts 100 km³/yr for basin spanning 3 regions
        # Basin appears in AFR (area=1000), WEU (area=500), MEA (area=500)
        # Total area = 2000 km²
        # AFR gets: 100 * (1000/2000) = 50 km³/yr
        # WEU gets: 100 * (500/2000) = 25 km³/yr
        # MEA gets: 100 * (500/2000) = 25 km³/yr
    """
    import xarray as xr

    # Load basin mapping
    if basin_mapping is None:
        basin_mapping = load_basin_mapping()

    # Load RIME region IDs from dataset
    dataset_path = (
        RIME_DATASETS_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    )
    ds = xr.open_dataset(dataset_path)
    rime_region_ids = ds.region.values  # [1, 2, 3, ..., 162] with gaps, 157 total

    # Create BASIN_ID → RIME array index mapping
    basin_id_to_rime_idx = {
        int(region_id): i for i, region_id in enumerate(rime_region_ids)
    }

    # Handle seasonal (157, n_timesteps, n_seasons) or annual (157, n_timesteps)
    is_seasonal = rime_predictions.ndim == 3
    if is_seasonal:
        n_rime, n_timesteps, n_seasons = rime_predictions.shape
        message_predictions = np.full((217, n_timesteps, n_seasons), np.nan)
    else:
        n_rime, n_timesteps = rime_predictions.shape
        message_predictions = np.full((217, n_timesteps), np.nan)

    # Compute total area for each BASIN_ID (for basins spanning multiple regions)
    basin_total_areas = basin_mapping.groupby("BASIN_ID")["area_km2"].sum()

    # Map each MESSAGE row to RIME prediction, split by area fraction
    for i, row in basin_mapping.iterrows():
        basin_id = row["BASIN_ID"]
        area_km2 = row["area_km2"]

        if basin_id in basin_id_to_rime_idx:
            rime_idx = basin_id_to_rime_idx[basin_id]
            total_area = basin_total_areas[basin_id]
            area_fraction = area_km2 / total_area

            if is_seasonal:
                message_predictions[i, :, :] = (
                    rime_predictions[rime_idx, :, :] * area_fraction
                )
            else:
                message_predictions[i, :] = (
                    rime_predictions[rime_idx, :] * area_fraction
                )

    return message_predictions


def _clip_gmt(gmt_array: np.ndarray, temporal_res: str, seed: int = 42) -> np.ndarray:
    """Clip GMT values below RIME emulator minimum with skewed noise.

    RIME emulators have limited support at low GMT values:
    - Annual: 0.6°C to 7.4°C
    - Seasonal: 0.8°C to 7.4°C (0.6-0.7°C has 87% NaN for many basins)

    Values below minimum are clipped and offset with skewed noise via beta(2,5)
    to avoid boundary artifacts.

    Args:
        gmt_array: GMT values, shape (n_years,) or (n_runs, n_years)
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        seed: Random seed for reproducibility

    Returns:
        Clipped GMT array (same shape as input)
    """
    gmt_clipped = np.asarray(gmt_array).copy()
    original_shape = gmt_clipped.shape
    gmt_flat = gmt_clipped.flatten()

    GMT_MIN = 0.8 if temporal_res == "seasonal2step" else 0.6
    GMT_MAX_NOISE = 1.2 if temporal_res == "seasonal2step" else 0.9

    low_gmt_mask = gmt_flat < GMT_MIN
    n_low = np.sum(low_gmt_mask)

    if n_low > 0:
        rng = np.random.default_rng(seed)
        noise = rng.beta(2, 5, size=n_low) * GMT_MAX_NOISE
        gmt_flat[low_gmt_mask] = GMT_MIN + noise

    return gmt_flat.reshape(original_shape)


def get_rime_dataset_path(
    variable: str, temporal_res: str = "annual", hydro_model: str = "CWatM"
) -> Path:
    """Get dataset path for a RIME variable.

    Args:
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor', 'EI_cool', 'EI_heat')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        hydro_model: Hydrological model ('CWatM' or 'H08')

    Returns:
        Path to RIME dataset file

    Raises:
        NotImplementedError: If capacity_factor or EI requested with seasonal resolution
        FileNotFoundError: If dataset file doesn't exist
    """
    # Handle building energy intensity variables
    if variable.startswith("EI_"):
        if temporal_res == "seasonal2step":
            raise NotImplementedError(
                "Building energy intensity only supports annual temporal resolution"
            )
        mode = variable.split("_")[1]  # Extract 'cool' or 'heat'
        dataset_path = RIME_DATASETS_DIR / f"region_EI_{mode}_gwl_binned.nc"

    elif variable == "capacity_factor":
        if temporal_res == "seasonal2step":
            raise NotImplementedError(
                "Capacity factor only supports annual temporal resolution"
            )
        dataset_path = RIME_DATASETS_DIR / "r12_capacity_gwl_ensemble.nc"
    else:
        # Variable mapping for basin-level variables
        var_map = {"local_temp": "temp_mean_anomaly"}
        rime_var = var_map.get(variable, variable)

        # Determine window - use window11 as default (smoothed emulators)
        # Exception: temp_mean_anomaly annual uses window0
        if rime_var == "temp_mean_anomaly" and temporal_res == "annual":
            window = "0"
        else:
            window = "11"

        dataset_filename = (
            f"rime_regionarray_{rime_var}_{hydro_model}_{temporal_res}_window{window}.nc"
        )
        dataset_path = RIME_DATASETS_DIR / dataset_filename

    if not dataset_path.exists():
        raise FileNotFoundError(
            f"RIME dataset not found: {dataset_path}\n"
            f"Available variables: qtot_mean, qr, local_temp, capacity_factor, EI_cool, EI_heat\n"
            f"Available temporal resolutions: annual, seasonal2step (not for capacity_factor/EI)\n"
            f"Available hydro models: CWatM, H08, MIROC-INTEG-LAND, WaterGAP2-2e"
        )

    return dataset_path


def predict_rime(
    gmt_array,
    variable: str,
    temporal_res: str = "annual",
    percentile: Optional[str] = None,
    sel: Optional[dict] = None,
    hydro_model: str = "CWatM",
    *,
    cvar_levels: Optional[list[float]] = None,
    cvar_method: str = "coherent",
):
    """Predict RIME variable from GMT array with automatic dataset loading.

    Handles single trajectories (1D) or ensembles (2D). For 2D input, computes
    E[RIME(GMT_i)] - the expectation of RIME predictions across ensemble members.
    Optionally computes CVaR risk metrics for 2D input.

    Args:
        gmt_array: GMT values (°C). Shape (n_years,) for single trajectory,
                   or (n_runs, n_years) for ensemble.
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor', 'EI_cool', 'EI_heat')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        percentile: Optional percentile suffix ('p10', 'p50', 'p90') for uncertainty bounds
        sel: Optional dimension selection for building variables.
        hydro_model: Hydrological model ('CWatM' or 'H08') for basin variables
        cvar_levels: Optional CVaR percentiles (e.g., [10, 50, 90]). Only for 2D input.
        cvar_method: CVaR method ('coherent' or 'pointwise'). Only used if cvar_levels provided.

    Returns:
        1D input: ndarray (or tuple for seasonal)
        2D input without cvar_levels: ndarray expectation (or tuple for seasonal)
        2D input with cvar_levels: dict with keys 'expectation', 'cvar_10', etc.
            - annual: each value is ndarray (n_basins, n_years)
            - seasonal: each value is tuple (dry, wet) of ndarrays

    Examples:
        # Single trajectory
        predictions = predict_rime(gmt_1d, 'qtot_mean')

        # Ensemble expectation
        expected = predict_rime(gmt_2d, 'qtot_mean')

        # Ensemble with CVaR
        results = predict_rime(gmt_2d, 'qtot_mean', cvar_levels=[10, 50, 90])
        # results['expectation'], results['cvar_10'], results['cvar_50'], results['cvar_90']
    """
    gmt_array = np.asarray(gmt_array)

    if gmt_array.ndim == 1:
        # Single trajectory: direct prediction (CVaR not applicable)
        if cvar_levels is not None:
            raise ValueError("cvar_levels only supported for 2D (ensemble) input")
        return _predict_rime_single(
            gmt_array, variable, temporal_res, percentile, sel, hydro_model
        )

    elif gmt_array.ndim == 2:
        # Ensemble mode
        n_runs, n_years = gmt_array.shape
        gmt_clipped = _clip_gmt(gmt_array, temporal_res)

        # Predict for each run
        predictions = [
            _predict_rime_single(
                gmt_clipped[i], variable, temporal_res, percentile, sel, hydro_model
            )
            for i in range(n_runs)
        ]

        if temporal_res == "seasonal2step":
            # predictions is list of (dry, wet) tuples
            dry_stack = np.stack([p[0] for p in predictions], axis=0)  # (n_runs, n_basins, n_years)
            wet_stack = np.stack([p[1] for p in predictions], axis=0)

            if cvar_levels is None:
                # Return expectation only
                return (np.mean(dry_stack, axis=0), np.mean(wet_stack, axis=0))
            else:
                # Compute CVaR for both dry and wet
                return _compute_ensemble_stats_seasonal(
                    dry_stack, wet_stack, cvar_levels, cvar_method
                )
        else:
            # Annual: stack predictions
            pred_stack = np.stack(predictions, axis=0)  # (n_runs, n_basins, n_years)

            if cvar_levels is None:
                # Return expectation only
                return np.mean(pred_stack, axis=0)
            else:
                # Compute CVaR
                return _compute_ensemble_stats(pred_stack, cvar_levels, cvar_method)

    else:
        raise ValueError(f"gmt_array must be 1D or 2D, got shape {gmt_array.shape}")


def _compute_ensemble_stats(
    pred_stack: np.ndarray,
    cvar_levels: list[float],
    cvar_method: str,
) -> dict[str, np.ndarray]:
    """Compute expectation and CVaR from stacked predictions.

    Args:
        pred_stack: (n_runs, n_basins, n_years)
        cvar_levels: CVaR percentiles
        cvar_method: 'coherent' or 'pointwise'

    Returns:
        Dict with 'expectation' and 'cvar_X' keys, each ndarray (n_basins, n_years)
    """
    from .cvar import compute_cvar

    # compute_cvar returns DataFrames, we extract values
    cvar_results = compute_cvar(pred_stack, cvar_levels, method=cvar_method)

    # Convert DataFrames to ndarrays
    return {key: df.values for key, df in cvar_results.items()}


def _compute_ensemble_stats_seasonal(
    dry_stack: np.ndarray,
    wet_stack: np.ndarray,
    cvar_levels: list[float],
    cvar_method: str,
) -> dict[str, tuple]:
    """Compute expectation and CVaR for seasonal (dry, wet) data.

    Returns:
        Dict with 'expectation' and 'cvar_X' keys, each tuple (dry_arr, wet_arr)
    """
    dry_stats = _compute_ensemble_stats(dry_stack, cvar_levels, cvar_method)
    wet_stats = _compute_ensemble_stats(wet_stack, cvar_levels, cvar_method)

    # Combine into tuples
    results = {}
    for key in dry_stats:
        results[key] = (dry_stats[key], wet_stats[key])
    return results


def _predict_rime_single(
    gmt_array: np.ndarray,
    variable: str,
    temporal_res: str,
    percentile: Optional[str],
    sel: Optional[dict],
    hydro_model: str,
):
    """Single-trajectory RIME prediction (internal helper)."""
    dataset_path = get_rime_dataset_path(variable, temporal_res, hydro_model)

    # Handle building energy intensity variables
    if variable.startswith("EI_"):
        mode = variable.split("_")[1]
        if mode == "cool":
            var_name = "EI_ac_m2" if percentile is None else f"EI_ac_m2_{percentile}"
        else:
            var_name = "EI_h_m2" if percentile is None else f"EI_h_m2_{percentile}"
        return predict_from_gmt(gmt_array, str(dataset_path), var_name, sel=sel)

    # Regional variables (capacity_factor)
    elif variable == "capacity_factor":
        predict_var = variable if percentile is None else f"{variable}_{percentile}"
        return predict_from_gmt(gmt_array, str(dataset_path), predict_var)

    # Basin-level variables
    var_map = {"local_temp": "temp_mean_anomaly"}
    rime_var = var_map.get(variable, variable)

    if temporal_res == "seasonal2step":
        var_dry = f"{rime_var}_dry" if percentile is None else f"{rime_var}_dry_{percentile}"
        pred_dry = predict_from_gmt(gmt_array, str(dataset_path), var_dry)
        pred_dry_expanded = split_basin_macroregion(pred_dry)

        var_wet = f"{rime_var}_wet" if percentile is None else f"{rime_var}_wet_{percentile}"
        pred_wet = predict_from_gmt(gmt_array, str(dataset_path), var_wet)
        pred_wet_expanded = split_basin_macroregion(pred_wet)

        return (pred_dry_expanded, pred_wet_expanded)
    else:
        predict_var = rime_var if percentile is None else f"{rime_var}_{percentile}"
        rime_predictions = predict_from_gmt(gmt_array, str(dataset_path), predict_var)
        return split_basin_macroregion(rime_predictions)


def load_basin_mapping() -> pd.DataFrame:
    """Load R12 basin mapping with MESSAGE basin codes.

    Returns:
        DataFrame with 217 rows (R12 basins) and columns including:
        ['BASIN_ID', 'NAME', 'BASIN', 'REGION', 'BCU_name', 'area_km2', 'model_region', 'basin_code']

        The 'basin_code' column contains MESSAGE format codes (e.g., 'B107|AFR').

    Examples:
        mapping = load_basin_mapping()
        basin_codes = mapping['basin_code'].tolist()  # ['B0|AFR', 'B0|EEU', ...]
        amazon_area = mapping[mapping['BASIN_ID'] == 100]['area_km2'].sum()
    """
    basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
    basin_df = pd.read_csv(basin_file)
    basin_df = basin_df[basin_df["model_region"] == "R12"].copy()
    basin_df = basin_df.reset_index(drop=True)
    basin_df["basin_code"] = "B" + basin_df["BASIN_ID"].astype(str) + "|" + basin_df["REGION"]
    return basin_df


def aggregate_basins_to_regions(
    basin_predictions: np.ndarray,
    variable: str,
    basin_mapping: Optional[pd.DataFrame] = None,
) -> np.ndarray:
    """Aggregate basin-level predictions to R12 regional level.

    Args:
        basin_predictions: Basin-level predictions array
            Shape: (217, n_timesteps) for annual
                   (217, n_timesteps, n_seasons) for seasonal
        variable: Variable name to determine aggregation method
            - 'qtot_mean', 'qr': sum across basins (hydrology volumes)
            - 'local_temp': mean across basins (temperature)
        basin_mapping: Basin mapping DataFrame (R12 basins only, reset index)
            If None, loads and filters all_basins.csv to R12

    Returns:
        region_predictions: Regional predictions array
            Shape: (12, n_timesteps) for annual (12 R12 regions)
                   (12, n_timesteps, n_seasons) for seasonal
            Regions ordered alphabetically

    Examples:
        # Aggregate basin predictions to regions
        basin_preds = predict_rime(gmt, 'qtot_mean', temporal_res='seasonal2step')
        region_preds = aggregate_basins_to_regions(basin_preds, 'qtot_mean')
    """
    # Load basin mapping
    if basin_mapping is None:
        basin_mapping = load_basin_mapping()

    # Determine aggregation function
    var_map = {"local_temp": "temp_mean_anomaly"}
    mapped_var = var_map.get(variable, variable)

    if mapped_var == "temp_mean_anomaly":
        agg_func = np.nanmean
    else:  # qtot_mean, qr
        agg_func = np.nansum

    # Get unique regions sorted alphabetically
    regions = sorted(basin_mapping["REGION"].unique())
    n_regions = len(regions)

    # Handle seasonal (217, n_timesteps, n_seasons) or annual (217, n_timesteps)
    is_seasonal = basin_predictions.ndim == 3
    if is_seasonal:
        n_basins, n_timesteps, n_seasons = basin_predictions.shape
        region_predictions = np.zeros((n_regions, n_timesteps, n_seasons))
    else:
        n_basins, n_timesteps = basin_predictions.shape
        region_predictions = np.zeros((n_regions, n_timesteps))

    # Aggregate each region
    for i, region in enumerate(regions):
        region_basins = basin_mapping[basin_mapping["REGION"] == region].index.tolist()

        if is_seasonal:
            region_data = basin_predictions[region_basins, :, :]
            region_predictions[i, :, :] = agg_func(region_data, axis=0)
        else:
            region_data = basin_predictions[region_basins, :]
            region_predictions[i, :] = agg_func(region_data, axis=0)

    return region_predictions


def extract_temperature_timeseries(
    magicc_df: pd.DataFrame,
    percentile: Optional[float] = None,
    run_id: Optional[int] = None,
) -> pd.DataFrame:
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
            (magicc_df["Model"].str.contains(model_pattern, na=False, regex=False))
            & (magicc_df["Variable"].str.contains(var_pattern, na=False, regex=False))
        ]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for run_id {run_id}")
    else:
        # Select by percentile
        if percentile is None:
            percentile = 50.0
        percentile_str = (
            f"{percentile}th Percentile" if percentile != 50.0 else "50.0th Percentile"
        )
        var_pattern = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{percentile_str}"

        temp_data = magicc_df[magicc_df["Variable"] == var_pattern]

        if len(temp_data) == 0:
            raise ValueError(f"No temperature data found for percentile {percentile}")

    temp_row = temp_data.iloc[0]

    # Extract year columns (strings like '1990', '2000', etc.)
    year_cols = [
        col for col in magicc_df.columns if isinstance(col, str) and col.isdigit()
    ]

    temps = {
        int(year): temp_row[year] for year in year_cols if pd.notna(temp_row[year])
    }

    temp_df = pd.DataFrame(
        {"year": list(temps.keys()), "gsat_anomaly_K": list(temps.values())}
    )

    # Extract metadata
    scenario_name = temp_row["Scenario"]
    model_name = temp_row["Model"]

    # Determine SSP family
    scenario_lower = scenario_name.lower()
    if "ssp1" in scenario_lower:
        ssp_family = "SSP1"
    elif "ssp2" in scenario_lower:
        ssp_family = "SSP2"
    elif "ssp3" in scenario_lower:
        ssp_family = "SSP3"
    elif "ssp4" in scenario_lower:
        ssp_family = "SSP4"
    elif "ssp5" in scenario_lower:
        ssp_family = "SSP5"
    else:
        ssp_family = "SSP2"  # default

    temp_df["model"] = model_name
    temp_df["scenario"] = scenario_name
    temp_df["ssp_family"] = ssp_family

    return temp_df


def extract_all_run_ids(magicc_df: pd.DataFrame) -> list[int]:
    """Extract all available run_ids from MAGICC DataFrame.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)

    Returns:
        Sorted list of run_ids
    """
    gsat_data = magicc_df[
        magicc_df["Variable"].str.contains(
            "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3",
            na=False,
            regex=False,
        )
    ]

    def parse_run_id(model_str):
        if "|run_" in model_str:
            try:
                return int(model_str.split("|run_")[1].split("|")[0])
            except (IndexError, ValueError):
                return None
        return None

    run_ids = [parse_run_id(m) for m in gsat_data["Model"].unique()]
    run_ids = [r for r in run_ids if r is not None]

    return sorted(run_ids)


def _get_gmt_ensemble(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
) -> tuple[dict[int, np.ndarray], np.ndarray]:
    """Extract GMT trajectories for all runs from MAGICC DataFrame.

    Internal helper that extracts raw GMT values without any clipping or processing.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)
        run_ids: List of run IDs to extract

    Returns:
        Tuple of (gmt_trajectories, years) where:
        - gmt_trajectories: Dict mapping run_id -> GMT array (n_years,)
        - years: Array of year labels
    """
    gmt_trajectories = {}
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_trajectories[run_id] = temp_df["gsat_anomaly_K"].values
        if years is None:
            years = temp_df["year"].values

    return gmt_trajectories, years


def get_gmt_expectation(
    magicc_df: pd.DataFrame,
    run_ids: Optional[list[int]] = None,
    n_runs: Optional[int] = None,
    years: Optional[list[int]] = None,
) -> pd.DataFrame:
    """Compute expected GMT from MAGICC ensemble.

    Public API for computing expected (mean) GMT across MAGICC ensemble members.
    Use this instead of extracting individual run temperatures.

    Args:
        magicc_df: MAGICC output DataFrame (IAMC format)
        run_ids: Specific run IDs to use. If None, extracts all available.
        n_runs: Limit to first N runs (applied after run_ids selection).
                Useful for baseline (100 runs) vs budget scenarios (600 runs).
        years: Filter to specific years (e.g., MESSAGE_YEARS). If None, returns all years.

    Returns:
        DataFrame with columns ['year', 'gmt'] containing expected GMT per year.

    Example:
        >>> magicc_df = pd.read_excel('magicc_output.xlsx')
        >>> gmt_expected = get_gmt_expectation(magicc_df, n_runs=100)
        >>> gmt_expected[gmt_expected['year'] == 2050]['gmt'].values[0]
        1.834
    """
    # Get run_ids if not provided
    if run_ids is None:
        run_ids = extract_all_run_ids(magicc_df)

    # Limit runs if specified
    if n_runs is not None:
        run_ids = run_ids[:n_runs]

    # Extract GMT ensemble
    gmt_trajectories, all_years = _get_gmt_ensemble(magicc_df, run_ids)

    # Stack into array and compute mean
    gmt_array = np.array([gmt_trajectories[rid] for rid in run_ids])
    gmt_expected = np.mean(gmt_array, axis=0)

    # Build result DataFrame
    result = pd.DataFrame({"year": all_years, "gmt": gmt_expected})

    # Filter years if specified
    if years is not None:
        result = result[result["year"].isin(years)].reset_index(drop=True)

    return result
