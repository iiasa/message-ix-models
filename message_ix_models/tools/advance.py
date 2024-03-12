"""Handle data from the ADVANCE project."""

import logging
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import pandas as pd
import pint
from genno import Quantity

from message_ix_models.project.advance.data import LOCATION, NAME
from message_ix_models.util import (
    cached,
    local_data_path,
    maybe_query,
    private_data_path,
)

log = logging.getLogger(__name__)

#: Standard dimensions for data produced as snapshots from the IIASA ENE Program
#: “WorkDB”.
DIMS = ["model", "scenario", "region", "variable", "unit", "year"]


@cached
def get_advance_data(query: Optional[str] = None) -> pd.Series:
    """Return data from the ADVANCE Work Package 2 data snapshot at :data:`LOCATION`.

    .. deprecated:: 2023.11
       Use :class:`.ADVANCE` through :func:`.exo_data.prepare_computer` instead.

    Parameters
    ----------
    query : str, optional
        Passed to :meth:`pandas.DataFrame.query` to limit the returned values.

    Returns
    -------
    pandas.Series
        with a :class:`pandas.MultiIndex` having the levels :data:`.DIMS`.
    """
    try:
        path = private_data_path(*LOCATION)
    except TypeError:
        path = local_data_path(*LOCATION)

    return _read_workdb_snapshot(path, NAME).pipe(maybe_query, query)


def advance_data(variable: str, query: Optional[str] = None) -> Quantity:
    """Return a single ADVANCE data `variable` as a :class:`genno.Quantity`.

    .. deprecated:: 2023.11
       Use :class:`.ADVANCE` through :func:`.exo_data.prepare_computer` instead.

    Parameters
    ----------
    query : str, optional
        Passed to :func:`get_advance_data`.

    Returns
    -------
    genno.Quantity
        with the dimensions :data:`.DIMS` and name `variable`. If the units of the data
        for `variable` are consistent and parseable by :mod:`pint`, the returned
        Quantity has these units; otherwise units are discarded and the returned
        Quantity is dimensionless.
    """
    data = (
        get_advance_data(query)
        .rename("value")
        .xs(variable, level="variable")
        .reset_index("unit")
    )
    if len(data.unit.unique()) > 1:  # pragma: no cover
        log.info(f"Non-unique units for {variable!r}; discarded")
        units = ""
    else:
        units = data.unit.iloc[0]

    result = Quantity(data["value"], name=variable)

    try:
        result.units = units
    except pint.errors.PintError as e:  # pragma: no cover
        log.info(f'"{e}" when parsing {units!r}; discarded')

    return result


@cached
def _read_workdb_snapshot(path: Path, name: str) -> pd.Series:
    """Read the data file.

    The expected format is a ZIP archive at `path` containing a member at `name` in CSV
    format, with columns corresponding to :data:`DIMS`, except for “year”, which is
    stored as column headers (‘wide’ format). (This corresponds to an older version of
    the “IAMC format,” without more recent additions intended to represent sub-annual
    time resolution using a separate column.)

    .. deprecated:: 2023.11
       Use :func:`.iamc_like_data_for_query` instead.
    """
    with ZipFile(path) as zf:  # Open the ZIP archive
        with zf.open(name) as f:  # Open a particular member
            # - Read data using upper case column names, then convert to lower-case.
            # - Drop null rows.
            # - Stack the “year” dimension (‘long’ format), creating a pd.Series.
            # - Apply the index names.
            return (
                pd.read_csv(f, index_col=list(map(str.upper, DIMS[:-1])))
                .rename(columns=lambda c: int(c))
                .dropna(how="all")
                .stack()
                .rename_axis(DIMS)
            )
