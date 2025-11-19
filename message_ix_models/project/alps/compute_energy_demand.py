"""
Pure function to compute aggregate energy demand from GWL-binned EI and STURM parameters.

Based on STURM energy demand formula (F03_energy_demand.R:46-47):
- Heating: E = EI_heat × (1 - en_sav_ren) × shr_acc_heat / eff_heat × shr_floor_heat × f_h
- Cooling: E = EI_cool × (1 - en_sav_ren) × shr_acc_cool / eff_cool × shr_floor_cool × f_c
"""

import numpy as np
import pandas as pd
import xarray as xr
from typing import Dict, Literal


def compute_energy_demand(
    ei: float,
    floor_m2: float,
    params: Dict[str, float],
    mode: Literal["heat", "cool"] = "cool",
) -> float:
    """
    Compute energy demand from energy intensity and STURM parameters.

    Parameters
    ----------
    ei : float
        Energy intensity (MJ/m²/yr) from CHILLED GWL bins
    floor_m2 : float
        Floor area (million m²) from STURM
    params : dict
        STURM parameters with keys:
        - en_sav_ren: Energy savings from renovation (0-1)
        - shr_acc: Access/penetration share (0-1)
        - eff: System efficiency (>0)
        - shr_floor: Share of floor served (0-1)
        - f_hours: Hours fraction (hours/24, 0-1)
    mode : str
        "heat" or "cool"

    Returns
    -------
    float
        Energy demand (EJ/yr) = floor × EI × params / 1e6
        (Divide by 1e6 to convert MJ to EJ and Mm² to m²)

    Formula
    -------
    E = floor_Mm2 × EI × (1 - en_sav_ren) × shr_acc / eff × shr_floor × f_hours / 1e6
    """
    # Extract parameters
    en_sav_ren = params.get("en_sav_ren", 0.0)
    shr_acc = params.get("shr_acc", 1.0)
    eff = params.get("eff", 1.0)
    shr_floor = params.get("shr_floor", 1.0)
    f_hours = params.get("f_hours", 1.0)

    # Compute energy demand
    energy_demand = (
        floor_m2  # Million m²
        * ei  # MJ/m²/yr
        * (1 - en_sav_ren)  # Renovation savings adjustment
        * shr_acc  # Access/penetration share
        / eff  # System efficiency
        * shr_floor  # Floor share served
        * f_hours  # Hours fraction
        / 1e6  # Convert MJ → EJ
    )

    return energy_demand


def aggregate_energy_demand(
    ei_data: xr.Dataset,
    floor_data: pd.DataFrame,
    params: Dict[str, float],
    mode: Literal["heat", "cool"] = "cool",
) -> pd.DataFrame:
    """
    Compute aggregate energy demand by region from GWL-binned EI and STURM data.

    Parameters
    ----------
    ei_data : xr.Dataset
        GWL-binned energy intensity from NetCDF
        Dimensions: [region, arch, urt, gwl]
        Variables: EI_ac_m2_mean, EI_ac_m2_p50, etc.
    floor_data : pd.DataFrame
        Floor area by region/year/vintage from STURM
        Columns: region, year, vintage, floor_Mm2, ...
    params : dict
        STURM parameters (see compute_energy_demand)
    mode : str
        "heat" or "cool"

    Returns
    -------
    pd.DataFrame
        Aggregate energy demand by region/gwl/year
        Columns: [region, gwl, year, vintage, E_demand_mean, E_demand_p50, ...]
    """
    ei_var = f"EI_ac_m2" if mode == "cool" else f"EI_h_m2"

    results = []

    # Iterate over regions
    for region in ei_data.region.values:
        # Iterate over archetypes (map to vintage)
        for arch in ei_data.arch.values:
            # Map arch to vintage (assume 1:1 for now)
            vintage = arch

            # Get floor area for this region/vintage
            floor_subset = floor_data[
                (floor_data["region"] == region) & (floor_data["vintage"] == vintage)
            ]

            if len(floor_subset) == 0:
                continue

            # Iterate over GWL bins
            for gwl in ei_data.gwl.values:
                # Get EI statistics for this combination
                # Use urt='total' for aggregation
                ei_mean = ei_data[ei_var].sel(
                    region=region, arch=arch, urt="total", gwl=gwl
                ).values.item()

                ei_p50 = ei_data[f"{ei_var}_p50"].sel(
                    region=region, arch=arch, urt="total", gwl=gwl
                ).values.item()

                # Skip if no data
                if np.isnan(ei_mean):
                    continue

                # Compute energy demand for each year in floor_subset
                for _, row in floor_subset.iterrows():
                    floor_m2 = row["floor_Mm2"]
                    year = row["year"]

                    # Compute for mean and p50
                    e_mean = compute_energy_demand(
                        ei=ei_mean, floor_m2=floor_m2, params=params, mode=mode
                    )

                    e_p50 = compute_energy_demand(
                        ei=ei_p50, floor_m2=floor_m2, params=params, mode=mode
                    )

                    results.append(
                        {
                            "region": region,
                            "vintage": vintage,
                            "gwl": gwl,
                            "year": year,
                            f"E_{mode}_mean_EJ": e_mean,
                            f"E_{mode}_p50_EJ": e_p50,
                        }
                    )

    df_results = pd.DataFrame(results)

    # Aggregate by region/gwl/year (sum across vintages)
    df_agg = (
        df_results.groupby(["region", "gwl", "year"])
        .agg(
            {
                f"E_{mode}_mean_EJ": "sum",
                f"E_{mode}_p50_EJ": "sum",
            }
        )
        .reset_index()
    )

    return df_agg


# Hardcoded default parameters (to be replaced with actual STURM values)
DEFAULT_PARAMS_COOL = {
    "en_sav_ren": 0.0,  # No renovation savings
    "shr_acc": 0.5,  # 50% AC penetration (placeholder)
    "eff": 2.5,  # AC COP (placeholder)
    "shr_floor": 1.0,  # 100% of floor cooled
    "f_hours": 0.5,  # 12 hours/day cooling (placeholder)
}

DEFAULT_PARAMS_HEAT = {
    "en_sav_ren": 0.0,  # No renovation savings
    "shr_acc": 1.0,  # 100% heating access
    "eff": 0.85,  # Heating efficiency (placeholder)
    "shr_floor": 1.0,  # 100% of floor heated
    "f_hours": 0.67,  # 16 hours/day heating (placeholder)
}


if __name__ == "__main__":
    # Test the pure function
    ei_cool = 100.0  # MJ/m²/yr
    floor = 1000.0  # Million m²

    e_cool = compute_energy_demand(
        ei=ei_cool, floor_m2=floor, params=DEFAULT_PARAMS_COOL, mode="cool"
    )

    print(f"Test computation:")
    print(f"  EI: {ei_cool} MJ/m²/yr")
    print(f"  Floor: {floor} Mm²")
    print(f"  Energy demand (cooling): {e_cool:.3f} EJ/yr")
    print(f"  Parameters: {DEFAULT_PARAMS_COOL}")
