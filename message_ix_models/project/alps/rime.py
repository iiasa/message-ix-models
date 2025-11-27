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
from typing import Optional, Literal, Union
from scipy.stats import norm
from dataclasses import dataclass, field

from message_ix_models.util import package_data_path
from .weighted_cvar import compute_weighted_cvar
from .utils import fit_dist

# START YEAR FOR RIME PREDICTIONS
# Set to 2025 to avoid early years with GMT < 0.6°C (minimum GMT with empirical support)
START_YEAR = 2025

# RIME datasets directory
RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")


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


# ==============================================================================
# Lazy Ensemble Storage
# ==============================================================================


@dataclass
class _RimeEnsemble:
    """Internal lazy ensemble storage for efficient RIME predictions.

    Stores GMT trajectories + metadata instead of materialized predictions.
    Predictions are reconstructed on-demand via predict_from_gmt lookups.

    Memory efficiency: Stores ~288K values instead of 10-260M values (180× reduction).

    Attributes:
        gmt_trajectories: Dict mapping run_id → GMT array (n_years,)
        variable: RIME variable name ('qtot_mean', 'EI_cool', etc.)
        dataset_path: Path to RIME NetCDF dataset
        years: Year labels for GMT trajectories
        percentile_sampling: Optional dict mapping run_id → percentile choices per year.
                           For emulator uncertainty: array of [10, 50, 90] indices.
                           Shape: (n_years,) per run.
        suban: Whether predictions use seasonal resolution
        basin_mapping: Optional basin mapping DataFrame (for basin variables)
    """
    gmt_trajectories: dict[int, np.ndarray]
    variable: str
    dataset_path: Path
    years: np.ndarray
    percentile_sampling: Optional[dict[int, np.ndarray]] = None
    suban: bool = False
    basin_mapping: Optional[pd.DataFrame] = None

    def _evaluate_single_run(
        self,
        run_id: int,
        sel: Optional[dict] = None
    ) -> np.ndarray:
        """Evaluate predictions for a single run.

        Args:
            run_id: Run identifier
            sel: Optional dimension selection for building variables

        Returns:
            Predictions array. Shape depends on variable type and sel parameter.
        """
        from .rime_functions import predict_from_gmt

        gmt = self.gmt_trajectories[run_id]

        # Pattern match on (seasonal mode, emulator uncertainty)
        match (self.suban, self.percentile_sampling is not None):
            case (True, True):
                # Seasonal + emulator uncertainty: sample percentiles, interleave dry/wet
                percentile_indices = self.percentile_sampling[run_id]
                dry_preds = []
                wet_preds = []

                for year_idx, gmt_val in enumerate(gmt):
                    p = percentile_indices[year_idx]
                    dry_pred = predict_from_gmt(gmt_val, str(self.dataset_path), f"{self.variable}_dry_p{p}", sel=sel)
                    wet_pred = predict_from_gmt(gmt_val, str(self.dataset_path), f"{self.variable}_wet_p{p}", sel=sel)
                    dry_preds.append(dry_pred)
                    wet_preds.append(wet_pred)

                # Convert to arrays
                dry_array = np.array(dry_preds)  # (n_years, n_basins) or (n_years,)
                wet_array = np.array(wet_preds)

                if dry_array.ndim == 1:
                    # Regional: (n_years,)
                    interleaved = np.empty(len(gmt) * 2)
                    interleaved[0::2] = dry_array
                    interleaved[1::2] = wet_array
                else:
                    # Basin: (n_years, n_basins) → transpose to (n_basins, n_years*2)
                    n_years, n_basins = dry_array.shape
                    interleaved = np.empty((n_basins, n_years * 2))
                    interleaved[:, 0::2] = dry_array.T  # Transpose to (n_basins, n_years)
                    interleaved[:, 1::2] = wet_array.T

                return interleaved

            case (True, False):
                # Seasonal without emulator uncertainty: interleave dry/wet
                dry_pred = predict_from_gmt(gmt, str(self.dataset_path), f"{self.variable}_dry", sel=sel)
                wet_pred = predict_from_gmt(gmt, str(self.dataset_path), f"{self.variable}_wet", sel=sel)

                if dry_pred.ndim == 1:
                    # Regional: (n_years,)
                    interleaved = np.empty(len(gmt) * 2)
                    interleaved[0::2] = dry_pred
                    interleaved[1::2] = wet_pred
                else:
                    # Basin: (n_basins, n_years) → (n_basins, n_years*2)
                    n_basins, n_years = dry_pred.shape
                    interleaved = np.empty((n_basins, n_years * 2))
                    interleaved[:, 0::2] = dry_pred
                    interleaved[:, 1::2] = wet_pred

                return interleaved

            case (False, True):
                # Annual + emulator uncertainty: sample percentiles per year
                percentile_indices = self.percentile_sampling[run_id]
                predictions = []

                for year_idx, gmt_val in enumerate(gmt):
                    p = percentile_indices[year_idx]
                    pred = predict_from_gmt(gmt_val, str(self.dataset_path), f"{self.variable}_p{p}", sel=sel)
                    predictions.append(pred)

                pred_array = np.array(predictions)  # (n_years, n_basins) or (n_years,)
                # Transpose for basin data to match expected shape (n_basins, n_years)
                return pred_array.T if pred_array.ndim == 2 else pred_array

            case (False, False):
                # Annual without emulator uncertainty: single prediction
                return predict_from_gmt(gmt, str(self.dataset_path), self.variable, sel=sel)

    def evaluate(
        self,
        run_ids: Optional[list[int]] = None,
        sel: Optional[dict] = None,
        as_dataframe: bool = False
    ) -> dict[int, np.ndarray] | dict[int, pd.DataFrame]:
        """Evaluate ensemble predictions on-demand.

        Args:
            run_ids: Optional subset of runs to evaluate. If None, evaluates all.
            sel: Optional dimension selection for building variables
                 (e.g., {'region': 'NAM', 'arch': 'mfh_s1'})
            as_dataframe: If True, return DataFrames with metadata (basin variables only)

        Returns:
            Dict mapping run_id → predictions (array or DataFrame)
        """
        if run_ids is None:
            run_ids = list(self.gmt_trajectories.keys())

        predictions = {}
        for run_id in run_ids:
            pred = self._evaluate_single_run(run_id, sel=sel)

            # For basin variables with DataFrame output
            if as_dataframe and _get_variable_type(self.variable) == "basin":
                # Apply basin expansion if needed
                if pred.ndim == 2 and pred.shape[0] == 157:
                    pred_expanded = split_basin_macroregion(pred, self.basin_mapping)
                else:
                    pred_expanded = pred

                # Create DataFrame with metadata
                pred_df = self._array_to_dataframe(pred_expanded)
                predictions[run_id] = pred_df
            else:
                predictions[run_id] = pred

        return predictions

    def _array_to_dataframe(self, pred_array: np.ndarray) -> pd.DataFrame:
        """Convert prediction array to DataFrame with metadata (basin variables)."""
        if self.basin_mapping is None:
            self.basin_mapping = load_basin_mapping()

        # Extract metadata
        metadata_cols = ['BASIN_ID', 'NAME', 'BASIN', 'REGION', 'BCU_name', 'area_km2']
        basin_metadata = self.basin_mapping[metadata_cols].copy()

        # Create year columns
        if self.suban:
            columns = []
            for year in self.years:
                columns.extend([f"{year}_dry", f"{year}_wet"])
        else:
            columns = self.years

        # Create DataFrame
        data_df = pd.DataFrame(pred_array, columns=columns)
        return pd.concat([basin_metadata, data_df], axis=1)


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
        dataset_path = RIME_DATASETS_DIR / "stage3_gwl_binned.nc"
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
):
    """Predict RIME variable from GMT array with automatic dataset loading.

    User-friendly wrapper around predict_from_gmt that handles:
    - Variable name mapping (e.g., 'local_temp' → 'temp_mean_anomaly')
    - Automatic dataset selection based on variable and temporal resolution
    - Correct window selection (window11 for smoothed emulators, window0 for temp_mean_anomaly)
    - Expansion from 157 RIME basins to 217 MESSAGE rows (area-weighted splitting)
    - Regional aggregates (R12) for capacity_factor and building variables
    - Dimension selection for building energy intensity variables

    Args:
        gmt_array: Array of GMT values (°C)
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor', 'EI_cool', 'EI_heat')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        percentile: Optional percentile suffix ('p10', 'p50', 'p90') for uncertainty bounds
        sel: Optional dimension selection for building variables.
             E.g., {'region': 'NAM', 'arch': 'mfh_s1', 'urt': 'urban'}
             Reduces dimensionality by selecting specific values.
        hydro_model: Hydrological model ('CWatM' or 'H08') for basin variables

    Returns:
        For basin-level variables (qtot_mean, qr, local_temp):
            - annual: array (217, n_timesteps)
            - seasonal: tuple (dry, wet) where each is (217, n_timesteps)
            NOTE: Basins 0, 141, 154 have missing RIME data (filled with NaN)
        For regional variables (capacity_factor):
            - annual: array (12, n_timesteps) for R12 regions
            - seasonal: not supported
        For building variables (EI_cool, EI_heat):
            - Without sel: array (12, 10, 3, n_timesteps) for (region, arch, urt, time)
            - With sel: reduced array based on selections

    Examples:
        # Predict total runoff from GMT timeseries
        predictions = predict_rime(gmt_array, 'qtot_mean', temporal_res='seasonal2step')

        # Get 90th percentile predictions for groundwater recharge
        predictions_p90 = predict_rime(gmt_array, 'qr', percentile='p90')

        # Predict thermoelectric capacity factor (R12 regional)
        capacity_predictions = predict_rime(gmt_array, 'capacity_factor', percentile='p50')

        # Predict cooling EI for all building types
        ei_all = predict_rime(gmt_array, 'EI_cool', percentile='p50')  # (12, 10, 3, n_years)

        # Predict cooling EI for specific building type
        ei_sfh_urban_nam = predict_rime(
            gmt_array, 'EI_cool', percentile='p50',
            sel={'region': 'NAM', 'arch': 'mfh_s1', 'urt': 'urban'}
        )  # (n_years,) scalar per timestep
    """
    # Lazy import to avoid loading RIME at module import time
    from .rime_functions import predict_from_gmt

    # Get dataset path using shared helper
    dataset_path = get_rime_dataset_path(variable, temporal_res, hydro_model)

    # Handle building energy intensity variables
    if variable.startswith("EI_"):
        # Extract mode (cool or heat)
        mode = variable.split("_")[1]
        # Build variable name with percentile if provided
        if mode == "cool":
            var_name = "EI_ac_m2" if percentile is None else f"EI_ac_m2_{percentile}"
        else:  # heat
            var_name = "EI_h_m2" if percentile is None else f"EI_h_m2_{percentile}"

        return predict_from_gmt(gmt_array, str(dataset_path), var_name, sel=sel)

    # For regional variables (capacity_factor), return directly without basin expansion
    elif variable == "capacity_factor":
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


def batch_rime_predictions(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str,
    suban: bool = False,
) -> dict[int, pd.DataFrame]:
    """Run RIME predictions on multiple runs (eager evaluation).

    Wrapper that creates lazy ensemble and immediately evaluates it for backward compatibility.
    Use batch_rime_predictions_with_percentiles() directly for lazy evaluation.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame
        variable: Variable name (qtot_mean, qr, capacity_factor)
        suban: If True, use seasonal (dry/wet) resolution; if False, use annual

    Returns:
        Dictionary mapping run_id -> DataFrame with predictions (MESSAGE format with metadata)
    """
    ensemble = batch_rime_predictions_with_percentiles(
        magicc_df, run_ids, dataset_path, basin_mapping, variable, suban=suban
    )
    return ensemble.evaluate(as_dataframe=True)


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
    from .rime_functions import predict_from_gmt

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
) -> _RimeEnsemble:
    """Run RIME predictions with lazy evaluation for memory efficiency.

    Stores GMT trajectories instead of materialized predictions (180× memory reduction).
    Predictions are reconstructed on-demand when calling ensemble.evaluate().

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame (for basin variables)
        variable: Base variable name (qtot_mean, qr, capacity_factor, EI_cool, EI_heat)
        suban: If True, use seasonal (dry/wet) resolution; if False, use annual
               NOTE: Not supported for capacity_factor or EI variables

    Returns:
        _RimeEnsemble object with lazy evaluation.
        Call ensemble.evaluate(as_dataframe=True) to materialize predictions as DataFrames.
        Call ensemble.evaluate(as_dataframe=False) to get arrays only.
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

    # Reshape GMT back to original structure (remove flatten)
    gmt_array = gmt_flat.reshape(n_runs, n_years)

    # Create GMT trajectories dict
    gmt_trajectories = {run_id: gmt_array[i] for i, run_id in enumerate(run_ids)}

    # Return lazy ensemble (no materialization!)
    return _RimeEnsemble(
        gmt_trajectories=gmt_trajectories,
        variable=variable,
        dataset_path=dataset_path,
        years=years,
        percentile_sampling=None,  # No sampling yet (added by expand_predictions_with_emulator_uncertainty)
        suban=suban,
        basin_mapping=basin_mapping,
    )


# ==============================================================================
# Legacy Helper (Deprecated - will be removed)
# ==============================================================================


def _extract_percentile_predictions_DEPRECATED(
    gmt_flat: np.ndarray, dataset_path: str, var_prefix: str, percentiles: list[int]
) -> dict[int, np.ndarray]:
    """DEPRECATED: This function materializes predictions eagerly.

    Kept temporarily for reference. Use _RimeEnsemble.evaluate() instead.
    """
    # Lazy import to avoid loading RIME at module import time
    from .rime_functions import predict_from_gmt

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


# Skip old implementation - now handled by _RimeEnsemble


def expand_predictions_with_emulator_uncertainty(
    ensemble: _RimeEnsemble,
    weights: np.ndarray,
    n_samples: int = 5,
    seed: int = 42,
) -> tuple[_RimeEnsemble, np.ndarray]:
    """Expand ensemble with emulator uncertainty using stratified percentile sampling.

    RIME's philosophy (see rimeX/preproc/quantilemaps.py):
    - Emulator uncertainty is ALEATORY (random year-to-year scatter), not EPISTEMIC (systematic bias)
    - Shuffles emulator quantiles randomly across years to break time-coherence
    - CVaR represents "X% chance in any given year", not "persistently bad trajectory"

    Implementation:
    - For each MAGICC run, generate K pseudo-runs by sampling random percentile choices per year
    - Uses stratified sampling: roughly equal mix of p10/p50/p90 across samples
    - Stores only the sampling schedule (which percentile to use per year), not materialized predictions
    - Memory efficiency: N×K×T percentile indices vs N×K×T×B prediction values (B/1 reduction)

    Args:
        ensemble: Base RIME ensemble (N runs, no emulator uncertainty)
        weights: Original importance weights (length N)
        n_samples: Number of pseudo-runs per MAGICC run (default: 5)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        Tuple of:
        - expanded_ensemble: New ensemble with N×K pseudo-runs and percentile sampling schedule
        - expanded_weights: Array of weights for N×K pseudo-runs (each = original_weight / K)
    """
    rng = np.random.default_rng(seed)
    K = n_samples
    run_ids = list(ensemble.gmt_trajectories.keys())
    N = len(run_ids)
    n_years = len(ensemble.years)

    # Create expanded GMT trajectories and percentile sampling schedule
    expanded_gmt = {}
    percentile_sampling = {}
    pseudo_run_id = 0

    # Percentile choices for stratified sampling
    percentiles = np.array([10, 50, 90])

    # Process each run
    for run_id in run_ids:
        gmt = ensemble.gmt_trajectories[run_id]

        # Generate K pseudo-runs with shuffled percentile choices
        for k in range(K):
            # Stratified sampling: shuffle percentiles independently per year
            # Shape: (n_years,) with values from {10, 50, 90}
            percentile_choices = rng.choice(percentiles, size=n_years)

            # Store GMT and percentile schedule
            expanded_gmt[pseudo_run_id] = gmt.copy()
            percentile_sampling[pseudo_run_id] = percentile_choices
            pseudo_run_id += 1

    # Expand weights: each original weight is split equally among K samples
    expanded_weights = np.repeat(weights / K, K)

    # Create new ensemble with expanded structure
    expanded_ensemble = _RimeEnsemble(
        gmt_trajectories=expanded_gmt,
        variable=ensemble.variable,
        dataset_path=ensemble.dataset_path,
        years=ensemble.years,
        percentile_sampling=percentile_sampling,
        suban=ensemble.suban,
        basin_mapping=ensemble.basin_mapping,
    )

    return expanded_ensemble, expanded_weights


def compute_expectation(
    predictions: Union[_RimeEnsemble, dict[int, pd.DataFrame]],
    run_ids: Optional[np.ndarray] = None,
    weights: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Compute expectation across RIME predictions.

    Args:
        predictions: Either _RimeEnsemble (lazy) or dict mapping run_id -> DataFrame (eager)
        run_ids: Array of run IDs (required if predictions is dict, ignored if _RimeEnsemble)
        weights: Optional array of importance weights (must sum to ~1.0).
                 If None, uses uniform weights (unweighted mean).

    Returns:
        DataFrame with (weighted) mean predictions (MESSAGE format with metadata)
    """
    # Handle input type
    if isinstance(predictions, _RimeEnsemble):
        # Lazy evaluation: evaluate ensemble on-demand
        predictions_dict = predictions.evaluate(as_dataframe=True)
        run_ids = np.array(list(predictions_dict.keys()))
    else:
        # Eager mode: use provided dict
        predictions_dict = predictions
        if run_ids is None:
            raise ValueError("run_ids required when predictions is dict")

    # Separate metadata from data
    first_pred = predictions_dict[run_ids[0]]
    year_columns, basin_metadata = _separate_metadata_and_years(first_pred)

    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(year_columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions_dict[run_id][year_columns].values

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
    predictions: Union[_RimeEnsemble, dict[int, pd.DataFrame]],
    weights: np.ndarray,
    run_ids: Optional[np.ndarray] = None,
    cvar_levels: list[float] = [10, 50, 90],
    method: str = "pointwise",
) -> dict[str, pd.DataFrame]:
    """Compute weighted CVaR across RIME predictions.

    Args:
        predictions: Either _RimeEnsemble (lazy) or dict mapping run_id -> DataFrame (eager)
        weights: Array of importance weights (must sum to ~1.0)
        run_ids: Array of run IDs (required if predictions is dict, ignored if _RimeEnsemble)
        cvar_levels: List of CVaR percentiles (default: [10, 50, 90])
        method: CVaR computation method (default: "pointwise")
            - "pointwise": Independent CVaR at each timestep (maximally pessimistic)
            - "coherent": Trajectory-based CVaR (temporally coherent, realizable paths)

    Returns:
        Dictionary with keys 'expectation', 'cvar_10', 'cvar_50', 'cvar_90'
        Each value is a DataFrame (MESSAGE format: n_basins × year columns + metadata)
    """
    # Handle input type
    if isinstance(predictions, _RimeEnsemble):
        # Lazy evaluation: evaluate ensemble on-demand
        predictions_dict = predictions.evaluate(as_dataframe=True)
        run_ids = np.array(list(predictions_dict.keys()))
    else:
        # Eager mode: use provided dict
        predictions_dict = predictions
        if run_ids is None:
            raise ValueError("run_ids required when predictions is dict")

    # Separate metadata from year columns
    first_pred = predictions_dict[run_ids[0]]
    year_columns, basin_metadata = _separate_metadata_and_years(first_pred)

    # Stack predictions into 3D array (n_runs × n_basins × n_years)
    n_runs = len(run_ids)
    n_basins = len(first_pred)
    n_years = len(year_columns)

    values_3d = np.zeros((n_runs, n_basins, n_years))
    for i, run_id in enumerate(run_ids):
        values_3d[i, :, :] = predictions_dict[run_id][year_columns].values

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
