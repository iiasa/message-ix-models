"""RIME processing with importance weighting and CVaR calculation.

Core functions for:
- Extracting temperature timeseries from MAGICC output
- Running vectorized RIME predictions across multiple runs
- Computing weighted expectations and CVaR risk metrics

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
from functools import lru_cache
from pathlib import Path
from typing import Optional
from scipy.stats import norm

from message_ix_models.util import package_data_path
from .weighted_cvar import compute_weighted_cvar
from .utils import fit_dist

# START YEAR FOR RIME PREDICTIONS
# Set to 2025 to avoid early years with GMT < 0.6°C (minimum GMT with empirical support)
START_YEAR = 2025

# RIME datasets directory
RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")


def _separate_metadata_and_years(df: pd.DataFrame) -> tuple[list, pd.DataFrame]:
    """Separate year columns from metadata columns in RIME output DataFrames.

    Year columns are integers (e.g., 2020, 2021, ..., 2100).
    Metadata columns are everything else (BASIN_ID, NAME, REGION, etc.).

    Args:
        df: DataFrame with mixed year and metadata columns

    Returns:
        (year_columns, metadata_df): List of year column names and DataFrame of metadata columns
    """
    year_columns = [col for col in df.columns if isinstance(col, (int, np.integer))]
    metadata_cols = [col for col in df.columns if col not in year_columns]
    metadata_df = df[metadata_cols].copy() if metadata_cols else pd.DataFrame(index=df.index)
    return year_columns, metadata_df


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
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy()
        basin_mapping = basin_mapping.reset_index(drop=True)

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


def get_rime_dataset_path(variable: str, temporal_res: str = "annual") -> Path:
    """Get dataset path for a RIME variable.

    Args:
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')

    Returns:
        Path to RIME dataset file

    Raises:
        NotImplementedError: If capacity_factor requested with seasonal resolution
        FileNotFoundError: If dataset file doesn't exist
    """
    if variable == "capacity_factor":
        if temporal_res == "seasonal2step":
            raise NotImplementedError(
                "Capacity factor only supports annual temporal resolution"
            )
        dataset_path = RIME_DATASETS_DIR / "stage3_gwl_binned.nc"
    else:
        # Variable mapping for basin-level variables
        var_map = {"local_temp": "temp_mean_anomaly"}
        rime_var = var_map.get(variable, variable)

        # Determine window based on variable and temporal resolution
        if rime_var == "temp_mean_anomaly" and temporal_res == "annual":
            window = "0"
        else:
            window = "11"  # Using window11 for smoothed emulators

        dataset_filename = (
            f"rime_regionarray_{rime_var}_CWatM_{temporal_res}_window{window}.nc"
        )
        dataset_path = RIME_DATASETS_DIR / dataset_filename

    if not dataset_path.exists():
        raise FileNotFoundError(
            f"RIME dataset not found: {dataset_path}\n"
            f"Available variables: qtot_mean, qr, local_temp, capacity_factor\n"
            f"Available temporal resolutions: annual, seasonal2step"
        )

    return dataset_path


def predict_rime(
    gmt_array,
    variable: str,
    temporal_res: str = "annual",
    percentile: Optional[str] = None,
):
    """Predict RIME variable from GMT array with automatic dataset loading.

    User-friendly wrapper around predict_from_gmt that handles:
    - Variable name mapping (e.g., 'local_temp' → 'temp_mean_anomaly')
    - Automatic dataset selection based on variable and temporal resolution
    - Correct window selection (window11 for smoothed emulators, window0 for temp_mean_anomaly)
    - Expansion from 157 RIME basins to 217 MESSAGE rows (area-weighted splitting)
    - Regional aggregates (R12) for capacity_factor

    Args:
        gmt_array: Array of GMT values (°C)
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        percentile: Optional percentile suffix ('p10', 'p50', 'p90') for uncertainty bounds

    Returns:
        For basin-level variables (qtot_mean, qr, local_temp):
            - annual: array (217, n_timesteps)
            - seasonal: tuple (dry, wet) where each is (217, n_timesteps)
            NOTE: Basins 0, 141, 154 have missing RIME data (filled with NaN)
        For regional variables (capacity_factor):
            - annual: array (12, n_timesteps) for R12 regions
            - seasonal: not supported

    Examples:
        # Predict total runoff from GMT timeseries
        predictions = predict_rime(gmt_array, 'qtot_mean', temporal_res='seasonal2step')

        # Get 90th percentile predictions for groundwater recharge
        predictions_p90 = predict_rime(gmt_array, 'qr', percentile='p90')

        # Predict local temperature anomaly (annual only)
        temp_predictions = predict_rime(gmt_array, 'local_temp')

        # Predict thermoelectric capacity factor (R12 regional)
        capacity_predictions = predict_rime(gmt_array, 'capacity_factor', percentile='p50')
    """
    # Lazy import to avoid loading RIME at module import time
    from rime.rime_functions import predict_from_gmt

    # Get dataset path using shared helper
    dataset_path = get_rime_dataset_path(variable, temporal_res)

    # For regional variables (capacity_factor), return directly without basin expansion
    if variable == "capacity_factor":
        predict_var = variable if percentile is None else f"{variable}_{percentile}"
        return predict_from_gmt(gmt_array, str(dataset_path), predict_var)

    # Variable mapping for basin-level variables
    var_map = {"local_temp": "temp_mean_anomaly"}
    rime_var = var_map.get(variable, variable)

    # For seasonal, predict dry and wet separately and return as tuple
    if temporal_res == "seasonal2step":
        # Predict dry season
        var_dry = (
            f"{rime_var}_dry" if percentile is None else f"{rime_var}_dry_{percentile}"
        )
        pred_dry = predict_from_gmt(gmt_array, str(dataset_path), var_dry)
        pred_dry_expanded = split_basin_macroregion(pred_dry)

        # Predict wet season
        var_wet = (
            f"{rime_var}_wet" if percentile is None else f"{rime_var}_wet_{percentile}"
        )
        pred_wet = predict_from_gmt(gmt_array, str(dataset_path), var_wet)
        pred_wet_expanded = split_basin_macroregion(pred_wet)

        return (pred_dry_expanded, pred_wet_expanded)
    else:
        # Annual: single prediction
        predict_var = rime_var if percentile is None else f"{rime_var}_{percentile}"
        rime_predictions = predict_from_gmt(gmt_array, str(dataset_path), predict_var)
        return split_basin_macroregion(rime_predictions)


def load_country_to_region_mapping() -> pd.DataFrame:
    """Load country ISO3 to R12 region mapping.

    Returns:
        DataFrame with columns: ['ISO3', 'UN_Code', 'Shik_code', 'Country_Name', 'Status', 'R11', 'R12']

    Examples:
        # Get R12 region for a country
        mapping = load_country_to_region_mapping()
        usa_region = mapping[mapping['ISO3'] == 'USA']['R12'].iloc[0]  # 'NAM'

        # Get all countries in a region
        lam_countries = mapping[mapping['R12'] == 'LAM']['Country_Name'].tolist()
    """
    mapping_path = package_data_path("water", "demands", "country_region_map_key.csv")
    return pd.read_csv(mapping_path)


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
    # Load basin mapping using same logic as plotting.py
    if basin_mapping is None:
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy()
        basin_mapping = basin_mapping.reset_index(drop=True)

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


def batch_rime_predictions(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str,
) -> dict[int, pd.DataFrame]:
    """Run RIME predictions on multiple runs using vectorized GMT lookups.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame (unused for regional variables)
        variable: Variable name (qtot_mean, qr, capacity_factor)

    Returns:
        Dictionary mapping run_id -> DataFrame with predictions
        - Basin-level variables (qtot_mean, qr): 157 rows × year columns
        - Regional variables (capacity_factor): 12 rows × year columns
    """
    # Lazy import to avoid loading RIME at module import time
    from rime.rime_functions import predict_from_gmt

    # Extract GMT timeseries for all runs
    gmt_timeseries = []
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_timeseries.append(temp_df["gsat_anomaly_K"].values)
        if years is None:
            years = temp_df["year"].values

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array(gmt_timeseries)
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Call predict_from_gmt for each unique GMT value (cached dataset)
    # Results: (n_runs * n_years) × n_regions where n_regions = 157 basins or 12 R12 regions
    predictions_flat = np.array(
        [predict_from_gmt(gmt, str(dataset_path), variable) for gmt in gmt_flat]
    )

    # Reshape back to 3D: (n_runs × n_years × n_regions)
    n_regions = predictions_flat.shape[1]
    predictions_3d = predictions_flat.reshape(n_runs, n_years, n_regions)

    # Convert each run to MESSAGE format DataFrame
    results = {}
    for i, run_id in enumerate(run_ids):
        # predictions_3d[i]: (n_years × n_basins)
        df = pd.DataFrame(
            predictions_3d[i].T,  # Transpose to (n_basins × n_years)
            columns=years,
        )
        results[run_id] = df

    return results


def _extract_percentile_predictions(
    gmt_flat: np.ndarray, dataset_path: str, var_prefix: str, percentiles: list[int]
) -> dict[int, np.ndarray]:
    """Helper to extract predictions for multiple percentiles.

    Args:
        gmt_flat: Flattened GMT values (1D array)
        dataset_path: Path to RIME dataset NetCDF file
        var_prefix: Variable prefix (e.g., "qtot_mean", "qtot_mean_dry", "qr_wet")
        percentiles: List of percentiles to extract (e.g., [10, 50, 90])

    Returns:
        Dict mapping percentile -> predictions array (n_gmt, n_basins)
    """
    # Lazy import to avoid loading RIME at module import time
    from rime.rime_functions import predict_from_gmt

    results = {}
    for p in percentiles:
        predictions = np.array(
            [
                predict_from_gmt(gmt, dataset_path, f"{var_prefix}_p{p}")
                for gmt in gmt_flat
            ]
        )
        results[p] = predictions
    return results


def batch_rime_predictions_with_percentiles(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str,
    suban: bool = False,
) -> tuple[dict[int, pd.DataFrame], dict[int, pd.DataFrame], dict[int, pd.DataFrame]]:
    """Run RIME predictions extracting p10, p50, p90 for each GMT value.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame (unused for regional variables)
        variable: Base variable name (qtot_mean, qr, capacity_factor)
        suban: If True, use seasonal (dry/wet) resolution; if False, use annual
               NOTE: Not supported for capacity_factor

    Returns:
        Tuple of three dictionaries (predictions_p10, predictions_p50, predictions_p90),
        each mapping run_id -> DataFrame with predictions.
        - Basin-level variables: 157 rows × year columns (217 after expansion)
        - Regional variables (capacity_factor): 12 rows × year columns
        For seasonal mode, columns are interleaved: [year_dry, year_wet, ...].
    """
    # Validate parameters
    if variable == "capacity_factor" and suban:
        raise NotImplementedError(
            "Capacity factor only supports annual temporal resolution"
        )

    # Extract GMT timeseries for all runs
    gmt_timeseries = []
    years = None

    for run_id in run_ids:
        temp_df = extract_temperature_timeseries(magicc_df, run_id=run_id)
        gmt_timeseries.append(temp_df["gsat_anomaly_K"].values)
        if years is None:
            years = temp_df["year"].values

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array(gmt_timeseries)
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Clip GMT values below RIME minimum with skewed noise
    # Annual emulators: 0.6°C to 7.4°C (complete coverage)
    # Seasonal emulators: 0.8°C to 7.4°C (0.6-0.7°C has 87% NaN for many basins)
    GMT_MIN = 0.8 if suban else 0.6
    GMT_MAX_NOISE = 1.2 if suban else 0.9  # Maximum noise to add

    low_gmt_mask = gmt_flat < GMT_MIN
    n_low = np.sum(low_gmt_mask)

    if n_low > 0:
        # Generate skewed noise using beta distribution (skewed toward 0)
        # beta(2, 5) has mode around 0.2, skewed left
        rng = np.random.default_rng(42)  # Fixed seed for reproducibility
        noise = rng.beta(2, 5, size=n_low) * GMT_MAX_NOISE

        # Clip low values to GMT_MIN + skewed noise
        gmt_flat[low_gmt_mask] = GMT_MIN + noise

        print(
            f"Warning: {n_low}/{len(gmt_flat)} GMT values below {GMT_MIN}°C were clipped to [{GMT_MIN}, {GMT_MIN + GMT_MAX_NOISE}]°C with skewed noise"
        )
        print(f"  Original range: {gmt_array.min():.3f}°C to {gmt_array.max():.3f}°C")
        print(f"  Clipped range:  {gmt_flat.min():.3f}°C to {gmt_flat.max():.3f}°C")

    # Extract percentiles
    percentiles = [10, 50, 90]

    if suban:
        # Seasonal mode: extract dry and wet separately, then interleave
        dry_preds = _extract_percentile_predictions(
            gmt_flat, str(dataset_path), f"{variable}_dry", percentiles
        )
        wet_preds = _extract_percentile_predictions(
            gmt_flat, str(dataset_path), f"{variable}_wet", percentiles
        )

        # Interleave dry and wet: [year0_dry, year0_wet, year1_dry, year1_wet, ...]
        n_regions = dry_preds[10].shape[1]
        predictions_p10_flat = np.empty((len(gmt_flat) * 2, n_regions))
        predictions_p50_flat = np.empty((len(gmt_flat) * 2, n_regions))
        predictions_p90_flat = np.empty((len(gmt_flat) * 2, n_regions))

        predictions_p10_flat[0::2] = dry_preds[10]
        predictions_p10_flat[1::2] = wet_preds[10]
        predictions_p50_flat[0::2] = dry_preds[50]
        predictions_p50_flat[1::2] = wet_preds[50]
        predictions_p90_flat[0::2] = dry_preds[90]
        predictions_p90_flat[1::2] = wet_preds[90]

        n_timesteps = n_years * 2
    else:
        # Annual mode: single extraction
        preds = _extract_percentile_predictions(
            gmt_flat, str(dataset_path), variable, percentiles
        )
        predictions_p10_flat = preds[10]
        predictions_p50_flat = preds[50]
        predictions_p90_flat = preds[90]
        n_timesteps = n_years

    # ASSERTION: Validate prediction ranges
    min_allowed = -100 if variable.startswith("qr") else 0
    for pred, name in [
        (predictions_p10_flat, "p10"),
        (predictions_p50_flat, "p50"),
        (predictions_p90_flat, "p90"),
    ]:
        if not np.all(pred >= min_allowed):
            # Find where NaN or invalid values occur
            invalid_mask = ~(pred >= min_allowed)  # NaN or < min_allowed
            invalid_indices = np.where(invalid_mask)

            # Map back to (run, timestep, region) indices
            n_regions = pred.shape[1]
            for flat_idx in range(len(invalid_indices[0])):
                row = invalid_indices[0][flat_idx]
                col = invalid_indices[1][flat_idx]

                region_id = col
                pred_value = pred[row, col]

                # For seasonal mode, row corresponds to interleaved dry/wet timesteps
                # For annual mode, row directly maps to flattened (run, year) indices
                if suban:
                    # row spans (n_runs * n_years * 2) because of interleaving
                    # But gmt_flat only has (n_runs * n_years) values
                    # row = run*n_years*2 + timestep where timestep is in [0, n_years*2)
                    gmt_idx = (
                        row // 2
                    )  # Maps interleaved index back to annual GMT index
                    season = "dry" if row % 2 == 0 else "wet"
                    run_id = gmt_idx // n_years
                    year_idx = gmt_idx % n_years
                    gmt_value = gmt_flat[gmt_idx]
                    print(
                        f"  Invalid {name} at run={run_ids[run_id]}, year_idx={year_idx}, season={season}, region={region_id}: "
                        f"GMT={gmt_value:.3f}, prediction={pred_value}"
                    )
                else:
                    # Annual mode: row directly indexes gmt_flat
                    run_id = row // n_years
                    year_idx = row % n_years
                    gmt_value = gmt_flat[row]
                    print(
                        f"  Invalid {name} at run={run_ids[run_id]}, year_idx={year_idx}, region={region_id}: "
                        f"GMT={gmt_value:.3f}, prediction={pred_value}"
                    )

                # Only print first 10 violations
                if flat_idx >= 9:
                    n_total = len(invalid_indices[0])
                    print(f"  ... ({n_total} total violations)")
                    break

            assert False, (
                f"{name} values exceed allowed threshold: min={pred.min()}, allowed>={min_allowed}"
            )

    # Reshape all to 3D: (n_runs × n_timesteps × n_regions)
    n_rime_basins = predictions_p10_flat.shape[1]  # 157 RIME basins
    predictions_p10_3d = predictions_p10_flat.reshape(n_runs, n_timesteps, n_rime_basins)
    predictions_p50_3d = predictions_p50_flat.reshape(n_runs, n_timesteps, n_rime_basins)
    predictions_p90_3d = predictions_p90_flat.reshape(n_runs, n_timesteps, n_rime_basins)

    # ASSERTION: Verify percentile ordering (p10 ≤ p50 ≤ p90)
    if not np.all(predictions_p10_3d <= predictions_p50_3d):
        n_violations = np.sum(predictions_p10_3d > predictions_p50_3d)
        print(f"WARNING: {n_violations} cases where p10 > p50")
    if not np.all(predictions_p50_3d <= predictions_p90_3d):
        n_violations = np.sum(predictions_p50_3d > predictions_p90_3d)
        print(f"WARNING: {n_violations} cases where p50 > p90")

    # Expand from 157 RIME basins to 217 MESSAGE rows (basin-region fragments)
    # Apply split_basin_macroregion to each run
    n_message_rows = 217
    predictions_p10_expanded = np.full((n_runs, n_timesteps, n_message_rows), np.nan)
    predictions_p50_expanded = np.full((n_runs, n_timesteps, n_message_rows), np.nan)
    predictions_p90_expanded = np.full((n_runs, n_timesteps, n_message_rows), np.nan)

    for i in range(n_runs):
        # Transpose from (n_timesteps, n_rime_basins) to (n_rime_basins, n_timesteps) for split_basin_macroregion
        predictions_p10_expanded[i, :, :] = split_basin_macroregion(
            predictions_p10_3d[i].T, basin_mapping
        ).T
        predictions_p50_expanded[i, :, :] = split_basin_macroregion(
            predictions_p50_3d[i].T, basin_mapping
        ).T
        predictions_p90_expanded[i, :, :] = split_basin_macroregion(
            predictions_p90_3d[i].T, basin_mapping
        ).T

    # Replace original arrays with expanded versions
    predictions_p10_3d = predictions_p10_expanded
    predictions_p50_3d = predictions_p50_expanded
    predictions_p90_3d = predictions_p90_expanded
    n_regions = n_message_rows  # Update for downstream code

    # Load basin mapping for metadata
    if basin_mapping is None:
        from message_ix_models.util import package_data_path
        basin_file = package_data_path("water", "infrastructure", "all_basins.csv")
        basin_df = pd.read_csv(basin_file)
        basin_mapping = basin_df[basin_df["model_region"] == "R12"].copy()
        basin_mapping = basin_mapping.reset_index(drop=True)

    # Extract basin metadata
    metadata_cols = ['BASIN_ID', 'NAME', 'BASIN', 'REGION', 'BCU_name', 'area_km2']
    basin_metadata = basin_mapping[metadata_cols].copy()

    # Convert to MESSAGE format DataFrames with metadata
    results_p10 = {}
    results_p50 = {}
    results_p90 = {}

    # Create column labels
    if suban:
        columns = []
        for year in years:
            columns.append(f"{year}_dry")
            columns.append(f"{year}_wet")
    else:
        columns = years

    for i, run_id in enumerate(run_ids):
        # Transpose to (n_regions × n_timesteps)
        data_p10 = pd.DataFrame(predictions_p10_3d[i].T, columns=columns)
        data_p50 = pd.DataFrame(predictions_p50_3d[i].T, columns=columns)
        data_p90 = pd.DataFrame(predictions_p90_3d[i].T, columns=columns)

        # Add metadata columns
        results_p10[run_id] = pd.concat([basin_metadata, data_p10], axis=1)
        results_p50[run_id] = pd.concat([basin_metadata, data_p50], axis=1)
        results_p90[run_id] = pd.concat([basin_metadata, data_p90], axis=1)

    return results_p10, results_p50, results_p90


def expand_predictions_with_emulator_uncertainty(
    predictions_p10: dict[int, pd.DataFrame],
    predictions_p50: dict[int, pd.DataFrame],
    predictions_p90: dict[int, pd.DataFrame],
    run_ids: list[int],
    weights: np.ndarray,
    n_samples: int = 5,
    seed: int = 42,
) -> tuple[dict[int, pd.DataFrame], np.ndarray, np.ndarray]:
    """Expand predictions with emulator uncertainty using RIME-style shuffled sampling.

    NOTE: We adapt RIME's shuffling logic rather than calling make_quantilemap_prediction()
    directly because:
    1. We already have p10/p50/p90 percentiles from predict_from_gmt (no need to reload NetCDF)
    2. Our CVaR calculation uses DataFrame format aligned with MESSAGE basins
    3. RIME's quantilemap operates on full xarray datasets (different data structure)
    4. This approach allows easy comparison of time-coherent vs shuffled strategies

    RIME's philosophy (see rimeX/preproc/quantilemaps.py):
    - Emulator uncertainty is ALEATORY (random year-to-year scatter), not EPISTEMIC (systematic bias)
    - Shuffles emulator quantiles randomly across years to break time-coherence
    - CVaR represents "X% chance in any given year", not "persistently bad trajectory"

    Implementation:
    - For each MAGICC run, generate K pseudo-runs by sampling random emulator quantiles per year
    - Shuffling ensures no persistent "pessimistic emulator" artifacts in CVaR tails

    Args:
        predictions_p10: P10 predictions (run_id -> DataFrame)
        predictions_p50: P50 predictions (run_id -> DataFrame)
        predictions_p90: P90 predictions (run_id -> DataFrame)
        run_ids: List of run IDs
        weights: Original importance weights (length N)
        n_samples: Number of pseudo-runs per MAGICC run (default: 5)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        Tuple of:
        - expanded_predictions: Dict mapping pseudo_run_id -> DataFrame
        - expanded_run_ids: Array of pseudo-run IDs (sequential integers)
        - expanded_weights: Array of weights for N×K pseudo-runs (each = original_weight / K)
    """
    rng = np.random.default_rng(seed)
    K = n_samples
    N = len(run_ids)

    # Get dimensions and separate metadata from data
    first_pred = predictions_p50[run_ids[0]]
    year_columns, basin_metadata = _separate_metadata_and_years(first_pred)

    n_basins = len(first_pred)
    n_years = len(year_columns)

    # Initialize expanded predictions dict
    expanded_predictions = {}
    pseudo_run_id = 0

    # Process each run
    for i, run_id in enumerate(run_ids):
        # Get p10, p50, p90 arrays for this run (n_basins × n_years), excluding metadata
        p10_array = predictions_p10[run_id][year_columns].values
        p50_array = predictions_p50[run_id][year_columns].values
        p90_array = predictions_p90[run_id][year_columns].values

        # Vectorized fit: fit all (basin, year) distributions at once
        # fit_dist automatically dispatches to vectorized version when inputs are arrays
        vec_dist = fit_dist(
            [p50_array, p10_array, p90_array],
            quantiles=[0.5, 0.05, 0.95],
            dist_name="lognorm",
        )

        # RIME-style shuffling: sample independent random quantiles for each (basin, year, sample)
        # Shape: (n_samples, n_basins, n_years)
        random_quantiles = rng.uniform(0, 1, size=(K, n_basins, n_years))

        # Sample from fitted distributions using per-year random quantiles
        # This breaks time-coherence: each year gets independent emulator draw
        for k in range(K):
            # Sample shape: (n_basins, n_years) using quantiles of shape (n_basins, n_years)
            samples = vec_dist.ppf_per_year(random_quantiles[k, :, :])

            # Clip negatives to zero: three-parameter lognormal with negative location parameter
            # can produce negative samples when extrapolating to extreme quantiles (<< p05).
            # This occurs when sampling q < 0.5% from right-skewed distributions with loc < 0.
            # Since runoff is physically non-negative, clip to zero.
            # Affects ~0.08% of samples in extreme CVaR tails.
            samples = np.maximum(samples, 0.0)

            # Create DataFrame with year data
            data_df = pd.DataFrame(samples, columns=year_columns)

            # Add metadata columns
            pseudo_run_df = pd.concat([basin_metadata.reset_index(drop=True), data_df], axis=1)

            # Store with sequential integer key
            expanded_predictions[pseudo_run_id] = pseudo_run_df
            pseudo_run_id += 1

    # Create array of pseudo-run IDs (0, 1, 2, ..., N×K-1)
    expanded_run_ids = np.arange(N * K)

    # Expand weights: each original weight is split equally among K samples
    expanded_weights = np.repeat(weights / K, K)

    return expanded_predictions, expanded_run_ids, expanded_weights


def compute_expectation(
    predictions: dict[int, pd.DataFrame],
    run_ids: np.ndarray,
    weights: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Compute expectation across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        run_ids: Array of run IDs
        weights: Optional array of importance weights (must sum to ~1.0).
                 If None, uses uniform weights (unweighted mean).

    Returns:
        DataFrame with (weighted) mean predictions (MESSAGE format with metadata)
    """
    # Separate metadata from data
    first_pred = predictions[run_ids[0]]
    year_columns, basin_metadata = _separate_metadata_and_years(first_pred)

    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(year_columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id][year_columns].values

    # Compute (weighted) mean
    if weights is not None:
        mean = np.average(values_3d, axis=0, weights=weights)
    else:
        mean = np.mean(values_3d, axis=0)

    # Convert back to DataFrame with metadata
    result_data = pd.DataFrame(mean, columns=year_columns)
    result = pd.concat([basin_metadata.reset_index(drop=True), result_data], axis=1)

    # ASSERTION: Check for negative values in expectation (year columns only)
    if np.any(result_data.values < 0):
        n_negative = np.sum(result_data.values < 0)
        min_value = result_data.values.min()
        print(
            f"WARNING: Expectation contains {n_negative}/{result_data.size} negative values, "
            f"min={min_value:.6f}"
        )

    return result


def compute_rime_cvar(
    predictions: dict[int, pd.DataFrame],
    weights: np.ndarray,
    run_ids: np.ndarray,
    cvar_levels: list[float] = [10, 50, 90],
    method: str = "pointwise",
) -> dict[str, pd.DataFrame]:
    """Compute weighted CVaR across RIME predictions.

    Args:
        predictions: Dictionary mapping run_id -> MESSAGE format DataFrame
        weights: Array of importance weights (must sum to ~1.0)
        run_ids: Array of run IDs corresponding to weights
        cvar_levels: List of CVaR percentiles (default: [10, 50, 90])
        method: CVaR computation method (default: "pointwise")
            - "pointwise": Independent CVaR at each timestep (maximally pessimistic)
            - "coherent": Trajectory-based CVaR (temporally coherent, realizable paths)

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', 'cvar_90'
        Each value is a DataFrame (MESSAGE format: n_basins × year columns + metadata)
    """
    # Separate metadata from year columns
    first_pred = predictions[run_ids[0]]
    year_columns, basin_metadata = _separate_metadata_and_years(first_pred)

    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(year_columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions[run_id][year_columns].values

    # Get basin indices for DataFrame output
    basin_ids = list(first_pred.index)

    # Compute weighted CVaR
    cvar_results_raw = compute_weighted_cvar(
        values_3d, weights, cvar_levels, basin_ids=basin_ids, year_columns=year_columns, method=method
    )

    # Add metadata back to each result DataFrame
    cvar_results = {}
    for key, result_df in cvar_results_raw.items():
        result_with_metadata = pd.concat([basin_metadata.reset_index(drop=True), result_df.reset_index(drop=True)], axis=1)
        cvar_results[key] = result_with_metadata

    # ASSERTION: Check for negative values in CVaR results
    for key, result_df in cvar_results.items():
        # Check only year columns for negative values
        year_data = result_df[year_columns]
        if np.any(year_data.values < 0):
            n_negative = np.sum(year_data.values < 0)
            min_value = year_data.values.min()
            print(
                f"WARNING: CVaR '{key}' contains {n_negative}/{year_data.size} negative values, "
                f"min={min_value:.6f}"
            )

    return cvar_results
