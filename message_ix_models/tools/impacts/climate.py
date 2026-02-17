"""GMT array extraction from wide DataFrames.

Callers provide a DataFrame with ID columns and year columns. Two functions
extract the year columns as a structured array:

- :func:`gmt_ensemble` — all rows as ``(n_rows, n_years)``
- :func:`gmt_expectation` — nanmean across rows as ``(n_years,)``

Both return a :class:`GmtArray` NamedTuple bundling values with year labels.
"""

from collections.abc import Sequence
from typing import NamedTuple

import numpy as np
import pandas as pd


class GmtArray(NamedTuple):
    """GMT values with year labels.

    Attributes
    ----------
    values
        Shape ``(n_runs, n_years)`` for ensemble or ``(n_years,)`` for
        expectation.
    years
        Year labels, shape ``(n_years,)``.
    """

    values: np.ndarray
    years: np.ndarray


def _year_columns(df: pd.DataFrame, id_cols: Sequence[str]) -> list:
    """Identify year columns: all columns not in *id_cols* that are numeric.

    Accepts both ``int`` columns (``2020``) and ``str``-of-digits
    (``"2020"``). Returns original column labels sorted by integer value.

    Raises
    ------
    ValueError
        If no year columns are found.
    """
    id_set = set(id_cols)
    cols = sorted(
        (
            c
            for c in df.columns
            if c not in id_set
            and (isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit()))
        ),
        key=lambda c: int(c),
    )
    if not cols:
        raise ValueError(
            f"No year columns found. ID columns: {list(id_cols)}; "
            f"all columns: {list(df.columns)}"
        )
    return cols


def gmt_ensemble(df: pd.DataFrame, id_cols: Sequence[str]) -> GmtArray:
    """Extract wide DataFrame rows as a 2D array.

    Parameters
    ----------
    df
        Wide DataFrame with ID columns and year columns (int or str-digits).
    id_cols
        Non-year columns (e.g. ``["Model", "Scenario", "Variable"]``).

    Returns
    -------
    GmtArray
        ``.values`` shape ``(n_rows, n_years)``, ``.years`` shape
        ``(n_years,)``.
    """
    cols = _year_columns(df, id_cols)
    values = df[cols].values.astype(float)
    years = np.array([int(c) for c in cols])
    return GmtArray(values, years)


def gmt_expectation(df: pd.DataFrame, id_cols: Sequence[str]) -> GmtArray:
    """Nanmean of :func:`gmt_ensemble` along axis 0.

    Returns
    -------
    GmtArray
        ``.values`` shape ``(n_years,)``.
    """
    ens = gmt_ensemble(df, id_cols)
    return GmtArray(np.nanmean(ens.values, axis=0), ens.years)
