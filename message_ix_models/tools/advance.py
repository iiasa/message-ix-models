"""Handle data from the ADVANCE project."""
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pandas as pd
import pint
from genno import Quantity

from message_ix_models.util import (
    cached,
    local_data_path,
    maybe_query,
    private_data_path,
)

log = logging.getLogger(__name__)

#: Expected location of the ADVANCE WP2 data snapshot.
LOCATION = "advance", "advance_compare_20171018-134445.csv.zip"

#: Name of the data file within the archive.
NAME = "advance_compare_20171018-134445.csv"

#: Standard dimensions for data produced as snapshots from the IIASA ENE Program
#: “WorkDB”.
#:
#: .. todo:: Move to a common location for use with other snapshots in the same format.
DIMS = ["model", "scenario", "region", "variable", "unit", "year"]


@cached
def get_advance_data(query: Optional[str] = None) -> pd.Series:
    """Return data from the ADVANCE Work Package 2 data snapshot at :data:`LOCATION`.

    Parameters
    ----------
    query : str, *optional*
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

    Parameters
    ----------
    query : str, *optional*
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

    .. todo:: Move to a general location for use with other files in the same format.
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


def _fuzz_data(size=1e2, include: List[Tuple[str, str]] = []):
    """Select a subset of the data for use in testing.

    Parameters
    ----------
    size : numeric
        Number of rows to include.
    include : sequence of 2-tuple (str, str)
        (variable name, unit) to include. The data will be partly duplicated to ensure
        the given variable name(s) are included.
    """
    size = int(size)
    rng = np.random.default_rng()

    # - Select `size` rows at random from the full data set.
    # - Use their index for a new pd.Series with random data.
    # - Convert to pd.DataFrame with upper-case column names
    # - Drop duplicated indices
    # - Return to original wide format.
    columns = list(map(str.upper, DIMS))
    dfs = [
        pd.Series(
            rng.random(size), index=get_advance_data().sample(size).index, name="value"
        )
        .rename_axis(columns)
        .reset_index()
        .drop_duplicates(subset=columns)
        .pivot(index=columns[:-1], columns="YEAR", values="value")
    ]

    # Duplicate data for (variable, unit) pairs required per `include`
    for variable, unit in include:
        dfs.append(
            dfs[0]
            .query(f"VARIABLE != {variable!r}")
            .assign(VARIABLE=variable, UNIT=unit)
        )

    # Path for output archive

    # For ordinary testing, output to a temporary directory
    target = local_data_path("test", *LOCATION)
    # To update/overwrite the data file in the repo, uncomment this line
    # target = package_data_path("test", *LOCATION)

    target.parent.mkdir(exist_ok=True, parents=True)

    # Concatenate data, write to the target member of the target
    with ZipFile(target, "w", ZIP_DEFLATED) as zf:
        with zf.open(NAME, "w") as f:
            pd.concat(dfs).to_csv(f, index=False)
