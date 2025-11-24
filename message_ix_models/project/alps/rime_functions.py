"""
RIME prediction functions adapted from ~/RIME package.

Extended to support dimension selection for multi-dimensional datasets (e.g., EI with arch/urt).
"""

import xarray as xr
from functools import lru_cache
from typing import Optional, Dict


@lru_cache(maxsize=8)
def _load_dataset(dataset_path: str) -> xr.Dataset:
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
        gmt: Global mean temperature (float, °C above pre-industrial)
        dataset_path: Path to RIME NetCDF dataset
        variable: Variable name (e.g., 'qtot_mean', 'qr', 'EI_ac_m2')
        sel: Optional dict of dimension selections (e.g., {'region': 'R12_AFR', 'arch': 'SFH'})
             Applied before gwl interpolation to reduce dimensionality

    Returns:
        Array of predictions. Shape depends on:
        - No sel: All dimensions except gwl (e.g., (157,) for basins, (12, 10, 3) for EI)
        - With sel: Reduced dimensions after selection

    Examples:
        # Basin-level hydrology (2D: region × gwl)
        predict_from_gmt(1.5, "qtot_mean.nc", "qtot_mean")
        # Returns: (157,) array

        # Regional energy intensity (4D: region × arch × urt × gwl)
        predict_from_gmt(1.5, "EI_cool.nc", "EI_ac_m2")
        # Returns: (12, 10, 3) array

        # EI for specific building type (select region + arch)
        predict_from_gmt(1.5, "EI_cool.nc", "EI_ac_m2", sel={'region': 'R12_AFR', 'arch': 'SFH'})
        # Returns: (3,) array (just urt dimension)

        # EI for specific configuration (select all dimensions)
        predict_from_gmt(1.5, "EI_cool.nc", "EI_ac_m2",
                        sel={'region': 'R12_AFR', 'arch': 'SFH', 'urt': 'urban'})
        # Returns: scalar
    """
    ds = _load_dataset(dataset_path)

    # Get variable
    data = ds[variable]

    # Apply dimension selections if provided
    if sel is not None:
        for dim, value in sel.items():
            if dim in data.dims:
                data = data.sel({dim: value})

    # Interpolate at gwl (use 'nearest' to match original behavior)
    predictions = data.sel(gwl=gmt, method='nearest').values

    return predictions
