"""Year resampling for model timesteps.

Climate data (MAGICC, RIME) typically has annual resolution (2020-2100).
MESSAGE uses non-uniform timesteps (5-year steps with gaps). This module
resamples annual data to match model year grids.

"""

import logging
from typing import Literal

import pandas as pd

log = logging.getLogger(__name__)


def _resample_point(
    df: pd.DataFrame,
    id_cols: list[str],
    year_cols: list[int],
    targets: list[int],
    *,
    min_input_year: int,
    max_input_year: int,
) -> pd.DataFrame:
    missing = [y for y in targets if y not in year_cols]
    if missing:
        raise ValueError(
            f"Model years {missing} not found in input columns. "
            f"Input range: {min_input_year}-{max_input_year}"
        )
    return df[id_cols + targets].copy()


def _resample_average(
    df: pd.DataFrame,
    id_cols: list[str],
    year_cols: list[int],
    targets: list[int],
) -> pd.DataFrame:
    result = df[id_cols].copy()
    for i, y in enumerate(targets):
        start = targets[i - 1] + 1 if i > 0 else y
        period = [yr for yr in range(start, y + 1) if yr in year_cols]
        if period:
            result[y] = df[period].mean(axis=1)
        elif y in year_cols:
            result[y] = df[y]
        else:
            raise ValueError(f"No input data for averaging period ending at {y}")
    return result


def _resample_long_form(
    df: pd.DataFrame,
    id_cols: list[str],
    value_cols: list[str],
    model_years: list[int],
    method: Literal["point", "average", "interpolate"],
    *,
    extrapolate_below: bool,
) -> pd.DataFrame:
    """Long-form adapter: pivot per value column, resample, melt back, merge."""
    parts: list[pd.DataFrame] = []
    for col in value_cols:
        wide = df.pivot(index=id_cols, columns="year", values=col).reset_index()
        wide.columns.name = None
        sampled = sample_to_model_years(
            wide,
            id_cols,
            model_years,
            method,
            extrapolate_below=extrapolate_below,
        )
        long = sampled.melt(id_vars=id_cols, var_name="year", value_name=col)
        long["year"] = long["year"].astype(int)
        parts.append(long)
    result = parts[0]
    for part in parts[1:]:
        result = result.merge(part, on=id_cols + ["year"])
    return result


def _resample_interpolate(
    df: pd.DataFrame,
    id_cols: list[str],
    year_cols: list[int],
    targets: list[int],
    *,
    min_input_year: int,
    extrapolate_below: bool,
) -> pd.DataFrame:
    target_in = (
        targets if extrapolate_below else [y for y in targets if y >= min_input_year]
    )
    if not target_in:
        return df[id_cols].copy()

    target_cols = sorted(set(year_cols) | set(target_in))
    wide = df.set_index(id_cols)[year_cols].reindex(columns=target_cols)
    wide = wide.interpolate(axis=1, method="index")
    if extrapolate_below:
        wide = wide.bfill(axis=1)
    return wide[target_in].reset_index()


def sample_to_model_years(
    df: pd.DataFrame,
    id_cols: list[str],
    model_years: list[int],
    method: Literal["point", "average", "interpolate"] = "point",
    *,
    extrapolate_below: bool = False,
    value_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Resample data on a year axis to model timesteps.

    Parameters
    ----------
    df
        Wide DataFrame with integer year columns and ID columns. When
        *value_cols* is given, *df* is treated as long form instead:
        ID columns plus a ``year`` integer column plus each name in
        *value_cols*.
    id_cols
        Non-year columns to preserve (e.g. ``["BCU_name"]``, ``["region"]``).
    model_years
        Target years (e.g. ``[2020, 2025, ..., 2100, 2110]``).
        Years beyond input range are forward-filled from the last input year.
    method
        ``"point"`` — take value at model year; missing years raise.
        ``"average"`` — mean over the preceding period (e.g. 2026–2030 for 2030).
        ``"interpolate"`` — linear interpolation between input years using the
        year column as a numeric x-axis. Years below the first input year are
        dropped unless ``extrapolate_below=True``.
    extrapolate_below
        Only applies to ``method="interpolate"``. If True, target years below
        the first input year are back-filled from the first input row. Default
        False — drop them.
    value_cols
        When given, switches to long-form input/output: *df* must have a
        ``year`` integer column plus each name in *value_cols*. Output is
        long-form on the same columns.

    Returns
    -------
    pd.DataFrame
        Wide layout (default): ID columns plus *model_years* (filtered to
        the supported range) as integer columns. Long layout (when
        *value_cols* is given): ID columns plus ``year`` plus *value_cols*.
    """
    if value_cols is not None:
        return _resample_long_form(
            df,
            id_cols,
            value_cols,
            model_years,
            method,
            extrapolate_below=extrapolate_below,
        )

    year_cols = sorted(c for c in df.columns if isinstance(c, int) and c not in id_cols)
    if not year_cols:
        raise ValueError("No integer year columns found in DataFrame")

    min_input_year = year_cols[0]
    max_input_year = year_cols[-1]
    years_within = [y for y in model_years if y <= max_input_year]
    years_beyond = [y for y in model_years if y > max_input_year]

    match method:
        case "point":
            result = _resample_point(
                df,
                id_cols,
                year_cols,
                years_within,
                min_input_year=min_input_year,
                max_input_year=max_input_year,
            )
        case "average":
            result = _resample_average(df, id_cols, year_cols, years_within)
        case "interpolate":
            result = _resample_interpolate(
                df,
                id_cols,
                year_cols,
                years_within,
                min_input_year=min_input_year,
                extrapolate_below=extrapolate_below,
            )
        case _:
            raise ValueError(
                f"method must be 'point', 'average', or 'interpolate'; got {method!r}"
            )

    if years_beyond:
        result_year_cols = [
            c for c in result.columns if isinstance(c, int) and c not in id_cols
        ]
        if result_year_cols:
            last_available = max(result_year_cols)
            for y in years_beyond:
                result[y] = result[last_available]

    return result
