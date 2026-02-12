"""Year resampling for model timesteps.

Climate data (MAGICC, RIME) typically has annual resolution (2020-2100).
MESSAGE uses non-uniform timesteps (5-year steps with gaps). This module
resamples annual data to match model year grids.

No equivalent exists in the codebase. The genno ``interpolate`` operator
fills gaps in existing model data — it does not resample external climate
timeseries. ``tools/costs/`` derives years from codelists but does not
resample annual data.
"""

import logging

import pandas as pd

log = logging.getLogger(__name__)


def sample_to_model_years(
    df: pd.DataFrame,
    id_cols: list[str],
    model_years: list[int],
    method: str = "point",
) -> pd.DataFrame:
    """Resample annual data to model timesteps.

    Parameters
    ----------
    df
        Wide DataFrame with annual year columns (integers) and ID columns.
        Year columns must be integer-typed.
    id_cols
        Non-year columns to preserve (e.g. ``["BCU_name"]``, ``["region"]``).
    model_years
        Target years (e.g. ``[2020, 2025, ..., 2100, 2110]``).
        Years beyond input range are forward-filled from the last input year.
    method
        ``"point"`` — take value at model year.
        ``"average"`` — average over the preceding period
        (e.g. mean of 2026-2030 for timestep 2030).

    Returns
    -------
    pd.DataFrame
        DataFrame with *model_years* as columns plus *id_cols*.
    """
    # Identify input year range
    all_cols = df.columns.tolist()
    year_cols = sorted([c for c in all_cols if isinstance(c, int) and c not in id_cols])

    if not year_cols:
        raise ValueError("No integer year columns found in DataFrame")

    max_input_year = max(year_cols)

    # Separate model years within and beyond input range
    years_within = [y for y in model_years if y <= max_input_year]
    years_beyond = [y for y in model_years if y > max_input_year]

    if method == "point":
        # Select columns at model year positions
        missing = [y for y in years_within if y not in year_cols]
        if missing:
            raise ValueError(
                f"Model years {missing} not found in input columns. "
                f"Input range: {min(year_cols)}-{max_input_year}"
            )
        result = df[id_cols + years_within].copy()

    elif method == "average":
        result = df[id_cols].copy()
        for i, y in enumerate(years_within):
            start = years_within[i - 1] + 1 if i > 0 else y
            period_years = [yr for yr in range(start, y + 1) if yr in year_cols]
            if period_years:
                result[y] = df[period_years].mean(axis=1)
            elif y in year_cols:
                result[y] = df[y]
            else:
                raise ValueError(f"No input data for averaging period ending at {y}")
    else:
        raise ValueError(f"method must be 'point' or 'average', got {method}")

    # Forward-fill beyond input range
    if years_beyond and years_within:
        last_available = years_within[-1]
        for y in years_beyond:
            result[y] = result[last_available]

    return result


# Placed here rather than in a separate module because the function is
# trivial and there is no existing util/node.py to host it.  Re-exported
# from __init__.py so callers don't depend on this placement.
def extract_region_code(node: str) -> str:
    """Extract short region code from MESSAGE node name.

    Parameters
    ----------
    node
        MESSAGE node name (e.g. ``"R12_AFR"`` or ``"AFR"``).

    Returns
    -------
    str
        Short code (e.g. ``"AFR"``).
    """
    return node[4:] if node.startswith("R12_") else node
