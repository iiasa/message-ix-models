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
from typing import Optional, Literal, Union, Dict
from scipy.stats import norm
from dataclasses import dataclass, field

from message_ix_models.util import package_data_path
from .constants import R12_REGIONS
from .cvar import compute_cvar
from .utils import fit_dist

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
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        basin_mapping: Optional basin mapping DataFrame (for basin variables)
    """
    gmt_trajectories: dict[int, np.ndarray]
    variable: str
    dataset_path: Path
    years: np.ndarray
    percentile_sampling: Optional[dict[int, np.ndarray]] = None
    temporal_res: str = "annual"
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
        gmt = self.gmt_trajectories[run_id]

        # Pattern match on (seasonal mode, emulator uncertainty)
        match (self.temporal_res == "seasonal2step", self.percentile_sampling is not None):
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
            as_dataframe: If True, return DataFrames with metadata (basin and regional)

        Returns:
            Dict mapping run_id → predictions (array or DataFrame)
        """
        if run_ids is None:
            run_ids = list(self.gmt_trajectories.keys())

        var_type = _get_variable_type(self.variable)
        predictions = {}
        for run_id in run_ids:
            pred = self._evaluate_single_run(run_id, sel=sel)

            if as_dataframe:
                if var_type == "basin":
                    # Apply basin expansion if needed
                    if pred.ndim == 2 and pred.shape[0] == 157:
                        pred_expanded = split_basin_macroregion(pred, self.basin_mapping)
                    else:
                        pred_expanded = pred
                    pred_df = self._array_to_dataframe(pred_expanded)
                elif var_type == "regional":
                    pred_df = self._regional_array_to_dataframe(pred)
                else:
                    # Building variables - return as-is for now
                    predictions[run_id] = pred
                    continue
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
        if self.temporal_res == "seasonal2step":
            columns = []
            for year in self.years:
                columns.extend([f"{year}_dry", f"{year}_wet"])
        else:
            columns = self.years

        # Create DataFrame
        data_df = pd.DataFrame(pred_array, columns=columns)
        return pd.concat([basin_metadata, data_df], axis=1)

    def _regional_array_to_dataframe(self, pred_array: np.ndarray) -> pd.DataFrame:
        """Convert prediction array to DataFrame with metadata (regional variables).

        Args:
            pred_array: Regional predictions array, shape (12, n_years) or (n_years,)

        Returns:
            DataFrame with 'region' column + year columns
        """
        # Handle 1D array (single region selection) vs 2D (all regions)
        if pred_array.ndim == 1:
            # Single timeseries - reshape to (1, n_years)
            pred_array = pred_array.reshape(1, -1)

        # Create year columns
        if self.temporal_res == "seasonal2step":
            columns = []
            for year in self.years:
                columns.extend([f"{year}_dry", f"{year}_wet"])
        else:
            columns = list(self.years)

        # Create DataFrame
        data_df = pd.DataFrame(pred_array, columns=columns)

        # Add region metadata
        if len(data_df) == 12:
            data_df.insert(0, 'region', R12_REGIONS)
        else:
            # Subset of regions - use index
            data_df.insert(0, 'region', [f'region_{i}' for i in range(len(data_df))])

        return data_df


def _separate_metadata_and_years(df: pd.DataFrame) -> tuple[list, pd.DataFrame]:
    """Separate year columns from metadata columns in RIME output DataFrames.

    Year columns are either:
    - Integers (e.g., 2020, 2021, ..., 2100) for annual data
    - Strings matching '{year}_{season}' pattern (e.g., '2020_dry', '2020_wet') for seasonal

    Metadata columns are everything else (BASIN_ID, NAME, REGION, etc.).

    Args:
        df: DataFrame with mixed year and metadata columns

    Returns:
        (year_columns, metadata_df): List of year column names and DataFrame of metadata columns
    """
    def is_year_column(col):
        if isinstance(col, (int, np.integer)):
            return True
        if isinstance(col, str) and '_' in col:
            # Check if it matches '{year}_{season}' pattern
            parts = col.split('_')
            if len(parts) == 2 and parts[0].isdigit() and parts[1] in ('dry', 'wet'):
                return True
        return False

    year_columns = [col for col in df.columns if is_year_column(col)]
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
):
    """Predict RIME variable from GMT array with automatic dataset loading.

    Handles single trajectories (1D) or ensembles (2D). For 2D input, computes
    E[RIME(GMT_i)] - the expectation of RIME predictions across ensemble members.

    Args:
        gmt_array: GMT values (°C). Shape (n_years,) for single trajectory,
                   or (n_runs, n_years) for ensemble (returns expectation).
        variable: Target variable ('qtot_mean', 'qr', 'local_temp', 'capacity_factor', 'EI_cool', 'EI_heat')
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')
        percentile: Optional percentile suffix ('p10', 'p50', 'p90') for uncertainty bounds
        sel: Optional dimension selection for building variables.
        hydro_model: Hydrological model ('CWatM' or 'H08') for basin variables

    Returns:
        For basin-level variables (qtot_mean, qr, local_temp):
            - annual: array (217, n_timesteps)
            - seasonal: tuple (dry, wet) where each is (217, n_timesteps)
        For regional variables (capacity_factor):
            - annual: array (12, n_timesteps) for R12 regions
        For building variables (EI_cool, EI_heat):
            - Without sel: array (12, 10, 3, n_timesteps)
            - With sel: reduced array based on selections

    Examples:
        # Single trajectory
        predictions = predict_rime(gmt_1d, 'qtot_mean')

        # Ensemble expectation E[RIME(GMT_i)]
        gmt_ensemble = np.random.randn(100, 76) + 2.0  # (n_runs, n_years)
        expected = predict_rime(gmt_ensemble, 'qtot_mean')  # Same shape as single trajectory
    """
    gmt_array = np.asarray(gmt_array)

    if gmt_array.ndim == 1:
        # Single trajectory: direct prediction
        return _predict_rime_single(
            gmt_array, variable, temporal_res, percentile, sel, hydro_model
        )

    elif gmt_array.ndim == 2:
        # Ensemble: compute E[RIME(GMT_i)]
        n_runs, n_years = gmt_array.shape
        gmt_clipped = _clip_gmt(gmt_array, temporal_res)

        # Predict for each run
        predictions = [
            _predict_rime_single(
                gmt_clipped[i], variable, temporal_res, percentile, sel, hydro_model
            )
            for i in range(n_runs)
        ]

        # Stack and compute mean
        if temporal_res == "seasonal2step":
            # predictions is list of (dry, wet) tuples
            dry_stack = np.stack([p[0] for p in predictions], axis=0)
            wet_stack = np.stack([p[1] for p in predictions], axis=0)
            return (np.mean(dry_stack, axis=0), np.mean(wet_stack, axis=0))
        else:
            return np.mean(np.stack(predictions, axis=0), axis=0)

    else:
        raise ValueError(f"gmt_array must be 1D or 2D, got shape {gmt_array.shape}")


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


def batch_rime_predictions(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str,
    temporal_res: str = "annual",
) -> dict[int, pd.DataFrame]:
    """Run RIME predictions on multiple runs (eager evaluation).

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame
        variable: Variable name (qtot_mean, qr, capacity_factor)
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')

    Returns:
        Dictionary mapping run_id -> DataFrame with predictions (MESSAGE format with metadata)
    """
    ensemble = create_rime_ensemble(
        magicc_df, run_ids, dataset_path, basin_mapping, variable, temporal_res=temporal_res
    )
    return ensemble.evaluate(as_dataframe=True)


def create_rime_ensemble(
    magicc_df: pd.DataFrame,
    run_ids: list[int],
    dataset_path: Path,
    basin_mapping: pd.DataFrame,
    variable: str,
    temporal_res: str = "annual",
) -> _RimeEnsemble:
    """Create lazy RIME ensemble for memory-efficient processing.

    Stores GMT trajectories instead of materialized predictions (180x memory reduction).
    Predictions are reconstructed on-demand when calling ensemble.evaluate().

    Used by batch_rime_predictions() internally, and by expand_predictions_with_emulator_uncertainty()
    for the emulator uncertainty workflow.

    Args:
        magicc_df: MAGICC output DataFrame
        run_ids: List of run IDs to process
        dataset_path: Path to RIME dataset NetCDF file
        basin_mapping: Basin mapping DataFrame (for basin variables)
        variable: Base variable name (qtot_mean, qr, capacity_factor, EI_cool, EI_heat)
        temporal_res: Temporal resolution ('annual' or 'seasonal2step')

    Returns:
        _RimeEnsemble object with lazy evaluation.
        Call ensemble.evaluate(as_dataframe=True) to materialize predictions.
    """
    # Validate parameters
    if variable == "capacity_factor" and temporal_res == "seasonal2step":
        raise NotImplementedError(
            "Capacity factor only supports annual temporal resolution"
        )

    # Extract GMT ensemble using shared helper
    gmt_trajectories, years = _get_gmt_ensemble(magicc_df, run_ids)

    # Stack into 2D array (n_runs × n_years)
    gmt_array = np.array([gmt_trajectories[rid] for rid in run_ids])
    n_runs, n_years = gmt_array.shape

    # Flatten to 1D for vectorized lookup
    gmt_flat = gmt_array.flatten()

    # Clip GMT values below RIME minimum with skewed noise
    # Annual emulators: 0.6°C to 7.4°C (complete coverage)
    # Seasonal emulators: 0.8°C to 7.4°C (0.6-0.7°C has 87% NaN for many basins)
    GMT_MIN = 0.8 if temporal_res == "seasonal2step" else 0.6
    GMT_MAX_NOISE = 1.2 if temporal_res == "seasonal2step" else 0.9  # Maximum noise to add

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
        temporal_res=temporal_res,
        basin_mapping=basin_mapping,
    )


def expand_predictions_with_emulator_uncertainty(
    ensemble: _RimeEnsemble,
    n_samples: int = 5,
    seed: int = 42,
) -> _RimeEnsemble:
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
        n_samples: Number of pseudo-runs per MAGICC run (default: 5)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        Expanded ensemble with N×K pseudo-runs and percentile sampling schedule
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

    # Create new ensemble with expanded structure
    expanded_ensemble = _RimeEnsemble(
        gmt_trajectories=expanded_gmt,
        variable=ensemble.variable,
        dataset_path=ensemble.dataset_path,
        years=ensemble.years,
        percentile_sampling=percentile_sampling,
        temporal_res=ensemble.temporal_res,
        basin_mapping=ensemble.basin_mapping,
    )

    return expanded_ensemble


def compute_expectation(
    predictions: Union[_RimeEnsemble, dict[int, pd.DataFrame]],
    run_ids: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Compute expectation across RIME predictions.

    Args:
        predictions: Either _RimeEnsemble (lazy) or dict mapping run_id -> DataFrame (eager)
        run_ids: Array of run IDs (required if predictions is dict, ignored if _RimeEnsemble)

    Returns:
        DataFrame with mean predictions (MESSAGE format with metadata)
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

    # Compute mean
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
    run_ids: Optional[np.ndarray] = None,
    cvar_levels: list[float] = [10, 50, 90],
    method: str = "pointwise",
) -> dict[str, pd.DataFrame]:
    """Compute CVaR across RIME predictions.

    Args:
        predictions: Either _RimeEnsemble (lazy) or dict mapping run_id -> DataFrame (eager)
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

    # Compute CVaR
    cvar_results_raw = compute_cvar(
        values_3d, cvar_levels, basin_ids=basin_ids, year_columns=year_columns, method=method
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
