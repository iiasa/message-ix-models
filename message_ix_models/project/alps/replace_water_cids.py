"""Replace water availability parameters with climate impact projections.

This module provides functions to swap water CID (Climate Impact Driver) parameters
in MESSAGE scenarios with RIME-based climate projections.

Key functionality:
- Transform RIME seasonal (dry/wet) predictions to MESSAGE timeslices (h1/h2)
- Handle basin-specific seasonality via regime shift bifurcation mapping
- Prepare demand and share parameters in MESSAGE format
- Atomic parameter replacement in scenarios
"""

from message_ix import Scenario
import pandas as pd
import numpy as np
from functools import lru_cache

from message_ix_models.util import package_data_path
from message_ix_models.project.alps.constants import MESSAGE_YEARS, R12_REGIONS

# Default timeslice definitions (n_time=2)
DEFAULT_H1_MONTHS = {1, 2, 3, 4, 5, 6}
DEFAULT_H2_MONTHS = {7, 8, 9, 10, 11, 12}


@lru_cache(maxsize=1)
def load_bifurcation_mapping() -> pd.DataFrame:
    """Load basin-specific month→season mapping from regime shift analysis.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - BASIN_ID: Basin identifier (1-157)
        - wet_months: Set of month numbers (1-12) classified as wet
        - dry_months: Set of month numbers classified as dry
        - r2_joint: Fit quality of regime shift classification

    Notes
    -----
    The mapping is derived from joint regime shift analysis of qtot and qr
    timeseries. Each basin has its own seasonal classification based on
    hydrological patterns, not a global definition.
    """
    path = package_data_path("alps", "rime_datasets", "joint_bifurcation_mapping_CWatM_2step.csv")
    df = pd.read_csv(path)

    # Parse comma-separated month strings into sets of integers
    def parse_months(s):
        if pd.isna(s) or s == '':
            return set()
        return set(int(m) for m in str(s).split(','))

    df['wet_months'] = df['wet_months'].apply(parse_months)
    df['dry_months'] = df['dry_months'].apply(parse_months)

    return df


def compute_season_to_timeslice_matrix(
    wet_months: set,
    dry_months: set,
    h1_months: set = None,
    h2_months: set = None
) -> np.ndarray:
    """Compute 2x2 transformation matrix from (dry, wet) to (h1, h2) for a single basin.

    The transformation assumes uniform distribution within each season (constant
    flow rate per month within dry/wet periods).

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
        [S_h1]       [S_dry]
        [S_h2] = T @ [S_wet]

        T[0,0] = |dry ∩ h1| / |dry|  (fraction of dry in h1)
        T[0,1] = |wet ∩ h1| / |wet|  (fraction of wet in h1)
        T[1,0] = |dry ∩ h2| / |dry|  (fraction of dry in h2)
        T[1,1] = |wet ∩ h2| / |wet|  (fraction of wet in h2)

    Notes
    -----
    Conservation property: sum of each column = 1, ensuring S_h1 + S_h2 = S_dry + S_wet
    """
    if h1_months is None:
        h1_months = DEFAULT_H1_MONTHS
    if h2_months is None:
        h2_months = DEFAULT_H2_MONTHS

    n_dry = len(dry_months)
    n_wet = len(wet_months)

    # Handle edge cases where a season has no months
    if n_dry == 0:
        n_dry = 1  # Avoid division by zero; dry contribution will be 0
    if n_wet == 0:
        n_wet = 1

    # Compute overlap counts
    dry_in_h1 = len(dry_months & h1_months)
    dry_in_h2 = len(dry_months & h2_months)
    wet_in_h1 = len(wet_months & h1_months)
    wet_in_h2 = len(wet_months & h2_months)

    # Build transformation matrix
    # Columns: [dry, wet], Rows: [h1, h2]
    T = np.array([
        [dry_in_h1 / n_dry, wet_in_h1 / n_wet],
        [dry_in_h2 / n_dry, wet_in_h2 / n_wet]
    ])

    return T


def transform_seasonal_to_timeslice(
    dry_df: pd.DataFrame,
    wet_df: pd.DataFrame,
    n_time: int = 2
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
    The transformation preserves total annual supply: h1 + h2 = dry + wet

    For basin b with transformation matrix T_b:
        [h1_b]       [dry_b]
        [h2_b] = T_b [wet_b]

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
        if 'BASIN_ID' in dry_df.columns:
            basin_id = dry_df.iloc[i]['BASIN_ID']
        else:
            # Fallback: use row index + 1
            basin_id = i + 1

        # Look up bifurcation mapping for this basin
        basin_row = bifurc[bifurc['BASIN_ID'] == basin_id]

        if len(basin_row) == 0:
            # Basin not in mapping - use identity (dry→h1, wet→h2)
            h1_values[i, :] = dry_values[i, :]
            h2_values[i, :] = wet_values[i, :]
        else:
            wet_months = basin_row.iloc[0]['wet_months']
            dry_months = basin_row.iloc[0]['dry_months']

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


def prepare_demand_parameter(
    rime_df,
    commodity: str,
    temporal_res: str = "annual"
) -> pd.DataFrame:
    """Convert RIME wide-format projections to MESSAGE demand parameter format.

    Parameters
    ----------
    rime_df : pd.DataFrame or tuple of pd.DataFrame
        RIME projections from batch_rime_predictions/compute_expectation with metadata.
        For annual: single DataFrame with metadata columns + year columns
        For seasonal: tuple (dry, wet) each with same format
        Metadata includes: BASIN_ID, NAME, BASIN, REGION, BCU_name, area_km2
        Units: km³/year (positive values)

    commodity : str
        MESSAGE commodity name: 'surfacewater_basin' or 'groundwater_basin'

    temporal_res : str, optional
        Temporal resolution: 'annual' or 'seasonal' (default: 'annual')

    Returns
    -------
    pd.DataFrame
        MESSAGE demand parameter with columns:
        [node, commodity, level, year, time, value, unit]
        - node: "B{BCU_name}" e.g., "B1|AFR", "B38|AFR"
        - time: 'year' for annual, 'h1'/'h2' for seasonal
        - value: negative MCM/year (km³ × 1000, negated)

    Notes
    -----
    For seasonal data, applies basin-specific transformation from RIME (dry/wet)
    to MESSAGE timeslices (h1/h2) using the bifurcation mapping. This accounts
    for the fact that each basin has its own seasonal pattern (regime shift).
    """
    if temporal_res == "annual":
        # Sample at MESSAGE years and add 2110 (duplicate 2100)
        years_to_sample = [y for y in MESSAGE_YEARS if y in rime_df.columns and y <= 2100]
        df_sampled = rime_df[['BCU_name'] + years_to_sample].copy()

        # Duplicate 2100 → 2110
        if 2100 in df_sampled.columns:
            df_sampled[2110] = df_sampled[2100]

        # Melt to long format
        df_long = df_sampled.melt(
            id_vars=['BCU_name'],
            var_name='year',
            value_name='value'
        )

        # Transform to MESSAGE format
        result = pd.DataFrame({
            'node': 'B' + df_long['BCU_name'].astype(str),
            'commodity': commodity,
            'level': 'water_avail_basin',
            'year': df_long['year'],
            'time': 'year',
            'value': -df_long['value'] * 1000,  # km³ → MCM, negate
            'unit': 'MCM/year'
        })

    elif temporal_res == "seasonal":
        # Expect tuple (dry, wet) from RIME
        dry_df, wet_df = rime_df

        # Transform from RIME seasonal (dry/wet) to MESSAGE timeslices (h1/h2)
        # This applies basin-specific transformation based on regime shift mapping
        h1_df, h2_df = transform_seasonal_to_timeslice(dry_df, wet_df, n_time=2)

        # Sample at MESSAGE years
        years_to_sample = [y for y in MESSAGE_YEARS if y in h1_df.columns and y <= 2100]

        # Process h1 timeslice
        h1_sampled = h1_df[['BCU_name'] + years_to_sample].copy()
        if 2100 in h1_sampled.columns:
            h1_sampled[2110] = h1_sampled[2100]

        h1_long = h1_sampled.melt(
            id_vars=['BCU_name'], var_name='year', value_name='value'
        )
        h1_result = pd.DataFrame({
            'node': 'B' + h1_long['BCU_name'].astype(str),
            'commodity': commodity,
            'level': 'water_avail_basin',
            'year': h1_long['year'],
            'time': 'h1',
            'value': -h1_long['value'] * 1000,  # km³ → MCM, negate
            'unit': 'MCM/year'
        })

        # Process h2 timeslice
        h2_sampled = h2_df[['BCU_name'] + years_to_sample].copy()
        if 2100 in h2_sampled.columns:
            h2_sampled[2110] = h2_sampled[2100]

        h2_long = h2_sampled.melt(
            id_vars=['BCU_name'], var_name='year', value_name='value'
        )
        h2_result = pd.DataFrame({
            'node': 'B' + h2_long['BCU_name'].astype(str),
            'commodity': commodity,
            'level': 'water_avail_basin',
            'year': h2_long['year'],
            'time': 'h2',
            'value': -h2_long['value'] * 1000,
            'unit': 'MCM/year'
        })

        # Concatenate h1 and h2
        result = pd.concat([h1_result, h2_result], ignore_index=True)
    else:
        raise ValueError(f"temporal_res must be 'annual' or 'seasonal', got {temporal_res}")

    return result


def prepare_groundwater_share(
    qtot_df,
    qr_df,
    temporal_res: str = "annual"
) -> pd.DataFrame:
    """Compute groundwater share constraint from RIME projections.

    Parameters
    ----------
    qtot_df : pd.DataFrame or tuple of pd.DataFrame
        Total runoff RIME projections with metadata (includes BCU_name)

    qr_df : pd.DataFrame or tuple of pd.DataFrame
        Groundwater recharge RIME projections with metadata (includes BCU_name)

    temporal_res : str, optional
        Temporal resolution: 'annual' or 'seasonal' (default: 'annual')

    Returns
    -------
    pd.DataFrame
        MESSAGE share_commodity_lo parameter with columns:
        [shares, node_share, year_act, time, value, unit]
        - time: 'year' for annual, 'h1'/'h2' for seasonal
        - value: qr / (qtot + qr) × 0.95, clipped to [0, 1]

    Notes
    -----
    For seasonal data, first transforms from RIME (dry/wet) to MESSAGE timeslices
    (h1/h2) using basin-specific regime shift mapping, then computes share per
    timeslice.
    """
    if temporal_res == "annual":
        # Sample at MESSAGE years
        years_to_sample = [y for y in MESSAGE_YEARS if y in qtot_df.columns and y <= 2100]
        qtot_sampled = qtot_df[['BCU_name'] + years_to_sample].copy()
        qr_sampled = qr_df[['BCU_name'] + years_to_sample].copy()

        # Duplicate 2100 → 2110
        if 2100 in years_to_sample:
            qtot_sampled[2110] = qtot_sampled[2100]
            qr_sampled[2110] = qr_sampled[2100]

        # Compute share: qr / (qtot + qr) × 0.95
        bcu_col = qtot_sampled[['BCU_name']]
        share_values = (qr_sampled[years_to_sample + [2110]] / (qtot_sampled[years_to_sample + [2110]] + qr_sampled[years_to_sample + [2110]]) * 0.95).clip(0, 1)
        share = pd.concat([bcu_col, share_values], axis=1)

        # Melt to long format
        share_long = share.melt(
            id_vars=['BCU_name'], var_name='year', value_name='value'
        )

        result = pd.DataFrame({
            'shares': 'share_low_lim_GWat',
            'node_share': 'B' + share_long['BCU_name'].astype(str),
            'year_act': share_long['year'],
            'time': 'year',
            'value': share_long['value'],
            'unit': '-'
        })

    elif temporal_res == "seasonal":
        # Expect tuples (dry, wet) from RIME
        qtot_dry, qtot_wet = qtot_df
        qr_dry, qr_wet = qr_df

        # Transform from RIME seasonal (dry/wet) to MESSAGE timeslices (h1/h2)
        # Apply same transformation to both qtot and qr
        qtot_h1, qtot_h2 = transform_seasonal_to_timeslice(qtot_dry, qtot_wet, n_time=2)
        qr_h1, qr_h2 = transform_seasonal_to_timeslice(qr_dry, qr_wet, n_time=2)

        years_to_sample = [y for y in MESSAGE_YEARS if y in qtot_h1.columns and y <= 2100]

        # h1 timeslice
        qtot_h1_sampled = qtot_h1[['BCU_name'] + years_to_sample].copy()
        qr_h1_sampled = qr_h1[['BCU_name'] + years_to_sample].copy()
        if 2100 in years_to_sample:
            qtot_h1_sampled[2110] = qtot_h1_sampled[2100]
            qr_h1_sampled[2110] = qr_h1_sampled[2100]

        bcu_h1 = qtot_h1_sampled[['BCU_name']]
        share_h1_values = (qr_h1_sampled[years_to_sample + [2110]] / (qtot_h1_sampled[years_to_sample + [2110]] + qr_h1_sampled[years_to_sample + [2110]]) * 0.95).clip(0, 1)
        share_h1 = pd.concat([bcu_h1, share_h1_values], axis=1)

        h1_long = share_h1.melt(
            id_vars=['BCU_name'], var_name='year', value_name='value'
        )
        h1_result = pd.DataFrame({
            'shares': 'share_low_lim_GWat',
            'node_share': 'B' + h1_long['BCU_name'].astype(str),
            'year_act': h1_long['year'],
            'time': 'h1',
            'value': h1_long['value'],
            'unit': '-'
        })

        # h2 timeslice
        qtot_h2_sampled = qtot_h2[['BCU_name'] + years_to_sample].copy()
        qr_h2_sampled = qr_h2[['BCU_name'] + years_to_sample].copy()
        if 2100 in years_to_sample:
            qtot_h2_sampled[2110] = qtot_h2_sampled[2100]
            qr_h2_sampled[2110] = qr_h2_sampled[2100]

        bcu_h2 = qtot_h2_sampled[['BCU_name']]
        share_h2_values = (qr_h2_sampled[years_to_sample + [2110]] / (qtot_h2_sampled[years_to_sample + [2110]] + qr_h2_sampled[years_to_sample + [2110]]) * 0.95).clip(0, 1)
        share_h2 = pd.concat([bcu_h2, share_h2_values], axis=1)

        h2_long = share_h2.melt(
            id_vars=['BCU_name'], var_name='year', value_name='value'
        )
        h2_result = pd.DataFrame({
            'shares': 'share_low_lim_GWat',
            'node_share': 'B' + h2_long['BCU_name'].astype(str),
            'year_act': h2_long['year'],
            'time': 'h2',
            'value': h2_long['value'],
            'unit': '-'
        })

        result = pd.concat([h1_result, h2_result], ignore_index=True)
    else:
        raise ValueError(f"temporal_res must be 'annual' or 'seasonal', got {temporal_res}")

    return result


def replace_water_availability(
    scenario: Scenario,
    sw_demand_new: pd.DataFrame,  # Surfacewater demand (MESSAGE format)
    gw_demand_new: pd.DataFrame,  # Groundwater demand (MESSAGE format)
    gw_share_new: pd.DataFrame,   # Groundwater share constraint (MESSAGE format)
    commit_message: str = "Replace water availability with RIME projections",
) -> Scenario:
    """Replace water availability parameters with new CID projections.

    This function removes existing water availability constraints and replaces them
    with new climate-driven projections. It operates on scenarios that already have
    the nexus module built.

    Parameters
    ----------
    scenario : message_ix.Scenario
        MESSAGE scenario with nexus module built. Must contain:
        - demand parameter with commodity='surfacewater_basin'
        - demand parameter with commodity='groundwater_basin'
        - share_commodity_lo parameter with shares='share_low_lim_GWat'

    sw_demand_new : pd.DataFrame
        New surfacewater_basin demand parameter in MESSAGE format.
        Required columns: node, commodity, level, year, time, value, unit

    gw_demand_new : pd.DataFrame
        New groundwater_basin demand parameter in MESSAGE format.
        Required columns: node, commodity, level, year, time, value, unit

    gw_share_new : pd.DataFrame
        New share_commodity_lo parameter for groundwater constraint.
        Required columns: shares, node_share, year_act, time, value, unit

    commit_message : str, optional
        Annotation for scenario commit (default: "Replace water availability with RIME projections")

    Returns
    -------
    message_ix.Scenario
        Modified scenario with updated water availability parameters (committed)

    Notes
    -----
    - Input DataFrames must be in MESSAGE parameter format (see documentation)
    - Uses scenario.transact() for atomic checkout/commit
    - Assumes inputs are validated and ready to add
    - All three parameters (surfacewater, groundwater, share) are updated together

    Examples
    --------
    >>> from ixmp import Platform
    >>> from message_ix import Scenario
    >>> mp = Platform('ixmp_dev')
    >>> scen = Scenario(mp, 'model', 'scenario')
    >>>
    >>> # Prepare new CID DataFrames (from RIME projections)
    >>> sw_new = prepare_surfacewater_demand(qtot_rime)
    >>> gw_new = prepare_groundwater_demand(qr_rime)
    >>> share_new = compute_groundwater_share(qtot_rime, qr_rime)
    >>>
    >>> # Replace water availability
    >>> scen_updated = replace_water_availability(
    ...     scen, sw_new, gw_new, share_new,
    ...     commit_message="Update with SSP2 RCP4.5 RIME projections"
    ... )
    """
    # Get old parameters to remove
    old_sw = scenario.par('demand', {'commodity': 'surfacewater_basin'})
    old_gw = scenario.par('demand', {'commodity': 'groundwater_basin'})
    old_share = scenario.par('share_commodity_lo', {'shares': 'share_low_lim_GWat'})

    # Filter new parameters to match existing scenario structure
    # Strategy: Use RIME where valid, preserve original values where RIME has NaN
    existing_basins = set(old_sw['node'].unique())

    # Filter to existing basins only
    sw_candidate = sw_demand_new[sw_demand_new['node'].isin(existing_basins)].copy()
    gw_candidate = gw_demand_new[gw_demand_new['node'].isin(existing_basins)].copy()
    share_candidate = gw_share_new[gw_share_new['node_share'].isin(existing_basins)].copy()

    # Separate valid RIME values from NaN
    sw_valid = sw_candidate[~sw_candidate['value'].isna()]
    sw_nan = sw_candidate[sw_candidate['value'].isna()]

    gw_valid = gw_candidate[~gw_candidate['value'].isna()]
    gw_nan = gw_candidate[gw_candidate['value'].isna()]

    share_valid = share_candidate[~share_candidate['value'].isna()]
    share_nan = share_candidate[share_candidate['value'].isna()]

    # For NaN basin-year combinations, preserve original values if they exist
    # Match on: node, year, time for demand; node_share, year_act, time for share
    sw_nan_keys = sw_nan[['node', 'year', 'time']]
    sw_preserved = old_sw.merge(sw_nan_keys, on=['node', 'year', 'time'], how='inner')

    print(f"DEBUG: sw_nan has {len(sw_nan)} NaN rows, unique basins: {sw_nan['node'].nunique()}")
    print(f"DEBUG: sw_preserved has {len(sw_preserved)} preserved rows from merge")
    if len(sw_preserved) > 0:
        print(f"DEBUG: Preserved basins: {sorted(sw_preserved['node'].unique())}")

    gw_nan_keys = gw_nan[['node', 'year', 'time']]
    gw_preserved = old_gw.merge(gw_nan_keys, on=['node', 'year', 'time'], how='inner')

    share_nan_keys = share_nan[['node_share', 'year_act', 'time']]
    share_preserved = old_share.merge(share_nan_keys, on=['node_share', 'year_act', 'time'], how='inner')

    # Also preserve basins that are completely missing from RIME (not just NaN)
    basins_with_valid_rime = set(sw_valid['node'].unique())
    missing_basins = existing_basins - basins_with_valid_rime

    if missing_basins:
        sw_missing = old_sw[old_sw['node'].isin(missing_basins)]
        gw_missing = old_gw[old_gw['node'].isin(missing_basins)]
        share_missing = old_share[old_share['node_share'].isin(missing_basins)]
        print(f"DEBUG: {len(missing_basins)} basins missing from RIME, preserving {len(sw_missing)} original rows")
        print(f"DEBUG: Missing basins: {sorted(missing_basins)}")
    else:
        sw_missing = pd.DataFrame()
        gw_missing = pd.DataFrame()
        share_missing = pd.DataFrame()

    # Combine valid RIME values with preserved original values
    sw_filtered = pd.concat([sw_valid, sw_preserved, sw_missing], ignore_index=True)
    gw_filtered = pd.concat([gw_valid, gw_preserved, gw_missing], ignore_index=True)
    gw_share_filtered = pd.concat([share_valid, share_preserved, share_missing], ignore_index=True)

    # Report filtering statistics
    n_basins_old = len(existing_basins)
    n_basins_new = len(set(sw_demand_new['node'].unique()))
    n_rime_valid = len(sw_valid)
    n_preserved = len(sw_preserved)
    n_dropped = len(sw_candidate) - len(sw_filtered)

    if n_dropped > 0 or n_preserved > 0:
        print(f"Note: Filtered {len(sw_candidate) - n_rime_valid} rows (non-existent basins or NaN)")
        print(f"  Scenario basins: {n_basins_old}, RIME basins: {n_basins_new}")
        print(f"  RIME values: {n_rime_valid}, Preserved original: {n_preserved}, Dropped: {n_dropped}")

    # Replace parameters atomically
    with scenario.transact(commit_message):
        # Remove old water availability constraints
        scenario.remove_par('demand', old_sw)
        scenario.remove_par('demand', old_gw)
        scenario.remove_par('share_commodity_lo', old_share)

        # Add new RIME-based projections (filtered)
        scenario.add_par('demand', sw_filtered)
        scenario.add_par('demand', gw_filtered)
        scenario.add_par('share_commodity_lo', gw_share_filtered)

    scenario.set_as_default()
    return scenario


# ==============================================================================
# Cooling Capacity Factor Functions (Regional CID)
# ==============================================================================


def prepare_capacity_factor_parameter(
    rime_df: pd.DataFrame,
    scenario: "Scenario",
) -> pd.DataFrame:
    """Convert RIME regional capacity_factor predictions to MESSAGE parameter format.

    Parameters
    ----------
    rime_df : pd.DataFrame
        RIME capacity_factor predictions with columns: region + year columns (2025-2100)
        Shape: 12 rows (R12 regions) x N year columns
        Values: capacity factor (0-1)

    scenario : Scenario
        MESSAGE scenario to get existing capacity_factor structure from

    Returns
    -------
    pd.DataFrame
        MESSAGE capacity_factor parameter with columns:
        [node_loc, technology, year_vtg, year_act, time, value, unit]
        Only includes freshwater cooling technologies (contains 'fresh')
    """
    # Get existing capacity_factor for freshwater cooling techs
    existing_cf = scenario.par('capacity_factor')
    fresh_cf = existing_cf[existing_cf['technology'].str.contains('fresh', na=False)].copy()

    if len(fresh_cf) == 0:
        raise ValueError("No freshwater cooling technologies found in scenario capacity_factor")

    # Get year columns from RIME predictions
    year_cols = [c for c in rime_df.columns if isinstance(c, (int, np.integer))]

    # Build region -> RIME value lookup per year
    # RIME df has 'region' column with short codes (AFR, CHN, etc.)
    rime_lookup = {}
    for _, row in rime_df.iterrows():
        region = row.get('region', row.name) if 'region' in rime_df.columns else R12_REGIONS[row.name]
        for year in year_cols:
            rime_lookup[(region, year)] = row[year]

    # Create new capacity_factor DataFrame
    new_cf = fresh_cf.copy()

    # Map node_loc (R12_AFR) to RIME region (AFR) and apply RIME values
    def get_rime_value(row):
        # Extract region code from node_loc (e.g., R12_AFR -> AFR)
        node = row['node_loc']
        if node.startswith('R12_'):
            region = node[4:]
        else:
            region = node

        year = row['year_act']

        # Find closest year in RIME data
        if year in year_cols:
            key = (region, year)
        else:
            # Find nearest year
            nearest = min(year_cols, key=lambda y: abs(y - year))
            key = (region, nearest)

        return rime_lookup.get(key, row['value'])

    new_cf['value'] = new_cf.apply(get_rime_value, axis=1)

    return new_cf


def replace_cooling_capacity_factor(
    scenario: "Scenario",
    cf_new: pd.DataFrame,
    commit_message: str = "Replace cooling capacity_factor with RIME projections",
) -> "Scenario":
    """Replace freshwater cooling capacity_factor with RIME climate projections.

    Parameters
    ----------
    scenario : Scenario
        MESSAGE scenario with cooling technologies

    cf_new : pd.DataFrame
        New capacity_factor parameter for freshwater cooling techs
        Required columns: node_loc, technology, year_vtg, year_act, time, value, unit

    commit_message : str
        Annotation for scenario commit

    Returns
    -------
    Scenario
        Modified scenario with updated capacity_factor (committed)
    """
    # Get existing freshwater cooling capacity_factor to remove
    existing_cf = scenario.par('capacity_factor')
    fresh_cf_old = existing_cf[existing_cf['technology'].str.contains('fresh', na=False)].copy()

    print(f"   Replacing {len(fresh_cf_old)} freshwater cooling capacity_factor rows")
    print(f"   Technologies: {fresh_cf_old['technology'].nunique()} unique")
    print(f"   Regions: {fresh_cf_old['node_loc'].nunique()} unique")

    # Replace atomically
    with scenario.transact(commit_message):
        scenario.remove_par('capacity_factor', fresh_cf_old)
        scenario.add_par('capacity_factor', cf_new)

    scenario.set_as_default()
    return scenario
