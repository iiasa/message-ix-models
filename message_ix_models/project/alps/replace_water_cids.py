"""Replace water availability parameters with climate impact projections.

This module provides functions to swap water CID (Climate Impact Driver) parameters
in MESSAGE scenarios with RIME-based climate projections.

Key functionality:
- Transform RIME seasonal (dry/wet) predictions to MESSAGE timeslices (h1/h2)
- Handle basin-specific seasonality via regime shift bifurcation mapping
- Prepare demand and share parameters in MESSAGE format
- Atomic parameter replacement in scenarios
"""

from functools import lru_cache

import numpy as np
import pandas as pd
from message_ix import Scenario

from message_ix_models.project.alps.constants import MESSAGE_YEARS
from message_ix_models.util import package_data_path

# Default timeslice definitions (n_time=2)
DEFAULT_H1_MONTHS = {1, 2, 3, 4, 5, 6}
DEFAULT_H2_MONTHS = {7, 8, 9, 10, 11, 12}


@lru_cache(maxsize=1)
def load_bifurcation_mapping(expand_to_message: bool = True) -> pd.DataFrame:
    """Load basin-specific month→season mapping from regime shift analysis.

    Parameters
    ----------
    expand_to_message : bool
        If True (default), expand from 157 RIME basins to 217 MESSAGE basins
        by duplicating values for basins that span multiple regions.
        If False, return the raw 157-basin mapping.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - BASIN_ID: Basin identifier (1-157 for raw, repeated for expanded)
        - wet_months: Set of month numbers (1-12) classified as wet
        - dry_months: Set of month numbers classified as dry
        - r2_joint: Fit quality of regime shift classification
        If expanded, also includes: BCU_name, REGION, basin_code, area_km2

    Notes
    -----
    The mapping is derived from joint regime shift analysis of qtot and qr
    timeseries. Each basin has its own seasonal classification based on
    hydrological patterns, not a global definition.

    Bifurcation is an intensive variable (property of location), so values
    are duplicated (not area-weighted) when expanding to MESSAGE basins.
    """
    path = package_data_path(
        "alps", "rime_datasets", "joint_bifurcation_mapping_CWatM_2step.csv"
    )
    df = pd.read_csv(path)

    # Parse comma-separated month strings into sets of integers
    def parse_months(s):
        if pd.isna(s) or s == "":
            return set()
        return set(int(m) for m in str(s).split(","))

    df["wet_months"] = df["wet_months"].apply(parse_months)
    df["dry_months"] = df["dry_months"].apply(parse_months)

    if not expand_to_message:
        return df

    # Expand to 217 MESSAGE basins by joining on BASIN_ID
    from message_ix_models.project.alps.rime import load_basin_mapping

    basin_mapping = load_basin_mapping()  # 217 rows
    expanded = basin_mapping.merge(df, on="BASIN_ID", how="left")

    # For basins without bifurcation data, use default 50/50 split
    default_wet = {7, 8, 9, 10, 11, 12}
    default_dry = {1, 2, 3, 4, 5, 6}
    missing_mask = expanded["wet_months"].isna()
    if missing_mask.any():
        expanded.loc[missing_mask, "wet_months"] = expanded.loc[missing_mask].apply(
            lambda _: default_wet, axis=1
        )
        expanded.loc[missing_mask, "dry_months"] = expanded.loc[missing_mask].apply(
            lambda _: default_dry, axis=1
        )

    return expanded


def compute_season_to_timeslice_matrix(
    wet_months: set, dry_months: set, h1_months: set = None, h2_months: set = None
) -> np.ndarray:
    """Compute 2x2 transformation matrix from (dry, wet) to (h1, h2) for a single basin.

    RIME dry/wet values are ANNUALIZED RATES (km³/year), not seasonal volumes.
    This transformation converts annualized seasonal rates to annualized timeslice rates.

    Parameters
    ----------
    wet_months : set
        Set of month numbers (1-12) classified as wet for this basin
    dry_months : set
        Set of month numbers classified as dry for this basin
    h1_months : set, optional
        Months belonging to timeslice h1 (default: {1,2,3,4,5,6})
    h2_months : set, optional
        Months belonging to timeslice h2 (default: {7,8,9,10,11,12})

    Returns
    -------
    np.ndarray
        2x2 transformation matrix T where:
        [R_h1]       [R_dry]
        [R_h2] = T @ [R_wet]

        T[i,j] = months_of_season_j_in_timeslice_i / months_in_timeslice

        For n_time=2 (6-month timeslices):
        T[0,0] = |dry ∩ h1| / 6  (months of dry in h1, divided by timeslice length)
        T[0,1] = |wet ∩ h1| / 6
        T[1,0] = |dry ∩ h2| / 6
        T[1,1] = |wet ∩ h2| / 6

    Notes
    -----
    Volume conservation: annual_volume = R_dry × (n_dry/12) + R_wet × (n_wet/12)
                                       = R_h1 × 0.5 + R_h2 × 0.5

    The timeslice rates R_h1, R_h2 are annualized rates for each 6-month period.
    """
    if h1_months is None:
        h1_months = DEFAULT_H1_MONTHS
    if h2_months is None:
        h2_months = DEFAULT_H2_MONTHS

    # Timeslice length in months (for n_time=2, each timeslice is 6 months)
    n_timeslice_months = len(h1_months)  # = 6 for default

    # Compute overlap counts: how many months of each season fall in each timeslice
    dry_in_h1 = len(dry_months & h1_months)
    dry_in_h2 = len(dry_months & h2_months)
    wet_in_h1 = len(wet_months & h1_months)
    wet_in_h2 = len(wet_months & h2_months)

    # Build transformation matrix
    # Divide by timeslice length (6), NOT by season length
    # This correctly handles annualized rates from RIME
    T = np.array(
        [
            [dry_in_h1 / n_timeslice_months, wet_in_h1 / n_timeslice_months],
            [dry_in_h2 / n_timeslice_months, wet_in_h2 / n_timeslice_months],
        ]
    )

    return T


def transform_seasonal_to_timeslice(
    dry_df: pd.DataFrame, wet_df: pd.DataFrame, n_time: int = 2
) -> tuple:
    """Transform RIME seasonal (dry/wet) data to MESSAGE timeslice (h1/h2) format.

    Applies basin-specific linear transformation based on regime shift mapping.
    Each basin has its own transformation matrix determined by which months
    belong to dry vs wet seasons for that basin.

    Parameters
    ----------
    dry_df : pd.DataFrame
        RIME dry season predictions with columns: BASIN_ID, BCU_name, + year columns
        Values in km³/year
    wet_df : pd.DataFrame
        RIME wet season predictions with same structure
    n_time : int, optional
        Number of timeslices (default: 2 for h1/h2)

    Returns
    -------
    tuple of (h1_df, h2_df)
        DataFrames with same structure as input, values transformed to timeslice basis

    Notes
    -----
    The transformation preserves total annual VOLUME (not rates):
        V_h1 + V_h2 = V_dry + V_wet
    where V = R × duration. Rates R_h1 + R_h2 ≠ R_dry + R_wet in general.

    For basin b with transformation matrix T_b:
        [R_h1_b]       [R_dry_b]
        [R_h2_b] = T_b [R_wet_b]

    where T_b is computed from the basin's regime shift classification.
    """
    if n_time != 2:
        raise NotImplementedError(f"Only n_time=2 supported, got {n_time}")

    # Load bifurcation mapping
    bifurc = load_bifurcation_mapping()

    # Get year columns (integers)
    year_cols = [c for c in dry_df.columns if isinstance(c, (int, np.integer))]
    metadata_cols = [c for c in dry_df.columns if c not in year_cols]

    # Initialize output arrays
    n_basins = len(dry_df)
    n_years = len(year_cols)
    h1_values = np.zeros((n_basins, n_years))
    h2_values = np.zeros((n_basins, n_years))

    # Get dry and wet values as arrays
    dry_values = dry_df[year_cols].values  # (n_basins, n_years)
    wet_values = wet_df[year_cols].values

    # Apply transformation per basin
    for i in range(n_basins):
        # Get basin ID (RIME uses 1-indexed BASIN_ID)
        if "BASIN_ID" in dry_df.columns:
            basin_id = dry_df.iloc[i]["BASIN_ID"]
        else:
            # Fallback: use row index + 1
            basin_id = i + 1

        # Look up bifurcation mapping for this basin
        basin_row = bifurc[bifurc["BASIN_ID"] == basin_id]

        if len(basin_row) == 0:
            # Basin not in mapping - use identity (dry→h1, wet→h2)
            h1_values[i, :] = dry_values[i, :]
            h2_values[i, :] = wet_values[i, :]
        else:
            wet_months = basin_row.iloc[0]["wet_months"]
            dry_months = basin_row.iloc[0]["dry_months"]

            # Compute transformation matrix for this basin
            T = compute_season_to_timeslice_matrix(wet_months, dry_months)

            # Apply transformation: [h1, h2] = T @ [dry, wet]
            for j in range(n_years):
                seasonal = np.array([dry_values[i, j], wet_values[i, j]])
                timeslice = T @ seasonal
                h1_values[i, j] = timeslice[0]
                h2_values[i, j] = timeslice[1]

    # Construct output DataFrames
    h1_df = dry_df[metadata_cols].copy()
    h2_df = wet_df[metadata_cols].copy()

    for idx, col in enumerate(year_cols):
        h1_df[col] = h1_values[:, idx]
        h2_df[col] = h2_values[:, idx]

    return h1_df, h2_df


def _filter_with_fallback(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame,
    node_col: str,
    key_cols: list[str],
) -> pd.DataFrame:
    """Filter new values to existing basins, preserve original where RIME has NaN or missing."""
    existing_basins = set(old_df[node_col].unique())
    candidate = new_df[new_df[node_col].isin(existing_basins)].copy()

    valid = candidate[~candidate["value"].isna()]
    nan_rows = candidate[candidate["value"].isna()]

    nan_keys = nan_rows[key_cols]
    preserved = old_df.merge(nan_keys, on=key_cols, how="inner")

    basins_with_valid = set(valid[node_col].unique())
    missing_basins = existing_basins - basins_with_valid
    missing = (
        old_df[old_df[node_col].isin(missing_basins)]
        if missing_basins
        else pd.DataFrame()
    )

    return pd.concat([valid, preserved, missing], ignore_index=True)


def _sample_and_extend_years(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Sample at MESSAGE years and duplicate 2100 → 2110."""
    years = [y for y in MESSAGE_YEARS if y in df.columns and y <= 2100]
    sampled = df[["BCU_name"] + years].copy()
    if 2100 in sampled.columns:
        sampled[2110] = sampled[2100]
    return sampled, years + [2110]


def _to_demand_long(df: pd.DataFrame, commodity: str, time_val: str) -> pd.DataFrame:
    """Convert wide DataFrame to MESSAGE demand format."""
    years = [c for c in df.columns if isinstance(c, int)]
    long = df.melt(
        id_vars=["BCU_name"], value_vars=years, var_name="year", value_name="value"
    )
    result = pd.DataFrame(
        {
            "node": "B" + long["BCU_name"].astype(str),
            "commodity": commodity,
            "level": "water_avail_basin",
            "year": long["year"],
            "time": time_val,
            "value": -long["value"] * 1000,  # km³ → MCM, negate
            "unit": "MCM/year",
        }
    )
    # Clip positive values (depleting aquifers after negation)
    result.loc[result["value"] > 0, "value"] = 0.0
    return result


def _to_share_long(qtot: pd.DataFrame, qr: pd.DataFrame, time_val: str) -> pd.DataFrame:
    """Compute groundwater share and convert to MESSAGE format."""
    years = [c for c in qtot.columns if isinstance(c, int)]
    share_values = (qr[years] / (qtot[years] + qr[years]) * 0.95).clip(0, 1)
    share = pd.concat([qtot[["BCU_name"]], share_values], axis=1)
    long = share.melt(
        id_vars=["BCU_name"], value_vars=years, var_name="year", value_name="value"
    )
    return pd.DataFrame(
        {
            "shares": "share_low_lim_GWat",
            "node_share": "B" + long["BCU_name"].astype(str),
            "year_act": long["year"],
            "time": time_val,
            "value": long["value"],
            "unit": "-",
        }
    )


def prepare_water_cids(
    qtot,
    qr,
    scenario: Scenario,
    temporal_res: str = "annual",
    sw_from_residual: bool = False,
) -> tuple[
    tuple[pd.DataFrame, pd.DataFrame],
    tuple[pd.DataFrame, pd.DataFrame],
    tuple[pd.DataFrame, pd.DataFrame],
]:
    """Prepare all water CID parameters from RIME projections.

    Parameters
    ----------
    qtot : pd.DataFrame or tuple[pd.DataFrame, pd.DataFrame]
        Total runoff. Annual: single DataFrame. Seasonal: (dry, wet) tuple.
    qr : pd.DataFrame or tuple[pd.DataFrame, pd.DataFrame]
        Groundwater recharge. Same format as qtot.
    scenario : Scenario
        MESSAGE scenario for filtering to existing basins.
    temporal_res : str
        'annual' or 'seasonal' (default: 'annual')
    sw_from_residual : bool
        If True, compute surfacewater as qtot - qr. If False, use qtot directly.
        Default: False.

    Returns
    -------
    tuple of 3 tuples
        (sw_data, gw_data, share_data) where each is (new_filtered, old)
        ready for replace_water_availability.
    """
    if temporal_res == "annual":
        qtot_sampled, _ = _sample_and_extend_years(qtot)
        qr_sampled, _ = _sample_and_extend_years(qr)

        # Compute surfacewater source
        if sw_from_residual:
            sw_source = qtot_sampled.copy()
            year_cols = [c for c in sw_source.columns if isinstance(c, int)]
            sw_source[year_cols] = qtot_sampled[year_cols] - qr_sampled[year_cols]
        else:
            sw_source = qtot_sampled

        sw_new = _to_demand_long(sw_source, "surfacewater_basin", "year")
        gw_new = _to_demand_long(qr_sampled, "groundwater_basin", "year")
        share_new = _to_share_long(qtot_sampled, qr_sampled, "year")

    elif temporal_res == "seasonal":
        qtot_dry, qtot_wet = qtot
        qr_dry, qr_wet = qr

        # Transform dry/wet → h1/h2
        qtot_h1, qtot_h2 = transform_seasonal_to_timeslice(qtot_dry, qtot_wet, n_time=2)
        qr_h1, qr_h2 = transform_seasonal_to_timeslice(qr_dry, qr_wet, n_time=2)

        qtot_h1, _ = _sample_and_extend_years(qtot_h1)
        qtot_h2, _ = _sample_and_extend_years(qtot_h2)
        qr_h1, _ = _sample_and_extend_years(qr_h1)
        qr_h2, _ = _sample_and_extend_years(qr_h2)

        # Compute surfacewater source
        if sw_from_residual:
            year_cols = [c for c in qtot_h1.columns if isinstance(c, int)]
            sw_h1 = qtot_h1.copy()
            sw_h1[year_cols] = qtot_h1[year_cols] - qr_h1[year_cols]
            sw_h2 = qtot_h2.copy()
            sw_h2[year_cols] = qtot_h2[year_cols] - qr_h2[year_cols]
        else:
            sw_h1, sw_h2 = qtot_h1, qtot_h2

        sw_new = pd.concat(
            [
                _to_demand_long(sw_h1, "surfacewater_basin", "h1"),
                _to_demand_long(sw_h2, "surfacewater_basin", "h2"),
            ],
            ignore_index=True,
        )
        gw_new = pd.concat(
            [
                _to_demand_long(qr_h1, "groundwater_basin", "h1"),
                _to_demand_long(qr_h2, "groundwater_basin", "h2"),
            ],
            ignore_index=True,
        )
        share_new = pd.concat(
            [
                _to_share_long(qtot_h1, qr_h1, "h1"),
                _to_share_long(qtot_h2, qr_h2, "h2"),
            ],
            ignore_index=True,
        )
    else:
        raise ValueError(
            f"temporal_res must be 'annual' or 'seasonal', got {temporal_res}"
        )

    # Load old values and filter
    sw_old = scenario.par("demand", {"commodity": "surfacewater_basin"})
    gw_old = scenario.par("demand", {"commodity": "groundwater_basin"})
    share_old = scenario.par("share_commodity_lo", {"shares": "share_low_lim_GWat"})

    sw_filtered = _filter_with_fallback(
        sw_new, sw_old, "node", ["node", "year", "time"]
    )
    gw_filtered = _filter_with_fallback(
        gw_new, gw_old, "node", ["node", "year", "time"]
    )
    share_filtered = _filter_with_fallback(
        share_new, share_old, "node_share", ["node_share", "year_act", "time"]
    )

    return (sw_filtered, sw_old), (gw_filtered, gw_old), (share_filtered, share_old)


def replace_water_availability(
    scenario: Scenario,
    sw_data: tuple[pd.DataFrame, pd.DataFrame],
    gw_data: tuple[pd.DataFrame, pd.DataFrame],
    share_data: tuple[pd.DataFrame, pd.DataFrame],
    commit_message: str = "Replace water availability with RIME projections",
) -> Scenario:
    """Atomically replace water availability parameters.

    Parameters
    ----------
    scenario : Scenario
        MESSAGE scenario with nexus module built
    sw_data : tuple[pd.DataFrame, pd.DataFrame]
        (new, old) surfacewater demand from prepare_demand_parameter(..., scenario)
    gw_data : tuple[pd.DataFrame, pd.DataFrame]
        (new, old) groundwater demand from prepare_demand_parameter(..., scenario)
    share_data : tuple[pd.DataFrame, pd.DataFrame]
        (new, old) groundwater share from prepare_groundwater_share(..., scenario)
    commit_message : str
        Annotation for scenario commit

    Returns
    -------
    Scenario
        Modified scenario (committed)
    """
    sw_new, sw_old = sw_data
    gw_new, gw_old = gw_data
    share_new, share_old = share_data

    with scenario.transact(commit_message):
        scenario.remove_par("demand", sw_old)
        scenario.remove_par("demand", gw_old)
        scenario.remove_par("share_commodity_lo", share_old)
        scenario.add_par("demand", sw_new)
        scenario.add_par("demand", gw_new)
        scenario.add_par("share_commodity_lo", share_new)

    scenario.set_as_default()
    return scenario


# ==============================================================================
# Generic CID Replacement Helper
# ==============================================================================


def replace_parameter(
    scenario: Scenario,
    parameter: str,
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    commit_message: str,
    set_default: bool = True,
) -> Scenario:
    """Atomically replace parameter values using transact().

    Parameters
    ----------
    scenario : Scenario
        MESSAGE scenario to modify
    parameter : str
        Parameter name (e.g., 'demand', 'capacity_factor', 'share_commodity_lo')
    old_df : pd.DataFrame
        Existing parameter rows to remove
    new_df : pd.DataFrame
        New parameter rows to add
    commit_message : str
        Annotation for scenario commit
    set_default : bool
        Whether to mark scenario as default after commit (default: True)

    Returns
    -------
    Scenario
        Modified scenario (committed)
    """
    with scenario.transact(commit_message):
        scenario.remove_par(parameter, old_df)
        scenario.add_par(parameter, new_df)

    if set_default:
        scenario.set_as_default()

    return scenario
