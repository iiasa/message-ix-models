"""Simulated solution data for testing :mod:`~message_ix_models.report`."""

import logging
from collections import ChainMap, defaultdict
from collections.abc import Mapping
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

import pandas as pd
from dask.core import quote
from genno import Key, KeyExistsError, Quantity
from message_ix import Reporter
from pandas.api.types import is_scalar

from message_ix_models import ScenarioInfo
from message_ix_models.util import minimum_version
from message_ix_models.util._logging import mark_time, silence_log
from message_ix_models.util.ixmp import rename_dims

if TYPE_CHECKING:
    from message_ix.models import Item

__all__ = [
    "add_simulated_solution",
    "data_from_file",
    "simulate_qty",
    "to_simulate",
]

log = logging.getLogger(__name__)


def dims_of(info: "Item") -> Dict[str, str]:
    """Return a mapping from the full index names to short dimension IDs of `info`."""
    return {d: rename_dims().get(d, d) for d in (info.dims or info.coords or [])}


@minimum_version("message_ix 3.7.0.post0")
@lru_cache(1)
def to_simulate():
    """Return items to be included in a simulated solution."""
    from message_ix.models import MACRO, MESSAGE

    # Items to included in a simulated solution: MESSAGE sets and parameters; some
    # variables
    result = deepcopy(MESSAGE.items)
    # MACRO variables
    result.update({k: MACRO.items[k] for k in ("GDP", "MERtoPPP")})

    return result


def simulate_qty(
    name: str, dims: List[str], item_data: Union[dict, pd.DataFrame]
) -> Quantity:
    """Return simulated data for item `name`.

    Parameters
    ----------
    dims :
        Dimensions of the resulting quantity.
    item_data :
        Optional data for the quantity.
    """
    if isinstance(item_data, dict):
        # NB this is code lightly modified from make_df

        # Default values for every column
        data: Mapping = ChainMap(item_data, defaultdict(lambda: None))

        # Arguments for pd.DataFrame constructor
        args: Dict[str, Any] = dict(data={})

        # Flag if all values in `data` are scalars
        all_scalar = True

        for column in dims + ["value"]:
            # Update flag
            all_scalar &= is_scalar(data[column])
            # Store data
            args["data"][column] = data[column]

        if all_scalar:
            # All values are scalars, so the constructor requires an index to be passed
            # explicitly.
            args["index"] = [0]

        df = pd.DataFrame(**args)
    else:
        # Provided complete data frame
        df = item_data.rename(columns=rename_dims())

    # Data must be entirely empty, or complete
    assert not df.isna().any().any() or df.isna().all().all(), data
    assert not df.duplicated().any(), f"Duplicate data for simulated {repr(name)}"

    return Quantity(df.set_index(dims)["value"] if len(dims) else df, name=name)


def data_from_file(path: Path, *, name: str, dims: Sequence[str]) -> Quantity:
    """Read simulated solution data for item `name` from `path`.

    For variables and equations (`name` in upper case), the file **must** have columns
    corresponding to `dims` followed by "Val", "Marginal", "Upper", and "Scale". The
    "Val" column is returned.

    For parameters, the file **must** have columns corresponding to `dims` followed by
    "value" and "unit". The "value" column is returned.
    """
    if name.isupper():
        # Construct a list of the columns
        # NB Must assign the dimensions directly; they cannot be read from the file, as
        #    the column headers are the internal GAMS set names (e.g. "year_all")
        #    instead of the index names from message_ix.
        cols = list(dims) + ["Val", "Marginal", "Lower", "Upper", "Scale"]

        return Quantity(
            pd.read_csv(path, engine="pyarrow")
            .set_axis(cols, axis=1)
            .set_index(cols[:-5])["Val"],
            name=name,
        )
    else:
        cols = list(dims) + ["value", "unit"]
        tmp = (
            pd.read_csv(path, engine="pyarrow")
            # Drop a leading index column that appears in some files
            # TODO Adjust .snapshot.unpack() to avoid generating this column; update
            # data; then remove this call
            .drop(columns="", errors="ignore")
            .set_axis(cols, axis=1)
            .set_index(cols[:-2])
        )
        # TODO pass units if they are unique
        return Quantity(tmp["value"], name=name)


@minimum_version("message_ix 3.6")
def add_simulated_solution(
    rep: Reporter,
    info: ScenarioInfo,
    data: Optional[Dict] = None,
    path: Optional[Path] = None,
):
    """Add a simulated model solution to `rep`.

    Parameters
    ----------
    data : dict or pandas.DataFrame, *optional*
        If given, a mapping from MESSAGE item (set, parameter, or variable) names to
        inputs that are passed to :func:`simulate_qty`.
    path : Path, *optional*
        If given, a path to a directory containing one or more files with names like
        :file:`ACT.csv.gz`. These files are taken as containing "simulated" model
        solution data for the MESSAGE variable with the same name. See
        :func:`data_from_file`.
    """
    from ixmp.backend import ItemType

    rep.configure(
        rename_dims=dict(
            node_rel="nr",
            year_rel="yr",
        ),
    )

    mark_time()
    N = len(rep.graph)

    # Ensure "scenario" is present in the graph
    rep.graph.setdefault("scenario", None)

    # Add simulated data
    data = data or dict()
    for name, item_info in to_simulate().items():
        dims = list(dims_of(item_info).values())
        key = Key(name, dims)

        # Add a task to load data from a file in `path`, if it exists
        try:
            assert path is not None
            p = path.joinpath(name).with_suffix(".csv.gz")
            assert p.exists()
        except AssertionError:
            pass  # No `path` or no such file
        else:
            # Add data from file
            rep.add(key, data_from_file, p, name=name, dims=key.dims, sums=True)
            continue

        if item_info.type == ItemType.SET:
            # Add the set elements from `info`
            rep.add(rename_dims().get(name, name), quote(info.set[name]))
        elif item_info.type in (ItemType.PAR, ItemType.VAR):
            # Retrieve an existing key for `name`
            try:
                full_key = rep.full_key(name)
            except KeyError:
                full_key = None  # Not present in `rep`

            # Simulate data for name
            item_data = data.get(name)

            if full_key and not item_data:
                # Don't overwrite existing task with empty data
                continue

            # Add a task to simulate data for this quantity
            # NB data.get() can return None, but simulate_qty() needs item_data to not
            # be None
            rep.add(
                key,
                simulate_qty,
                name=name,
                dims=dims,
                item_data=item_data,
                sums=True,
            )

    log.info(f"{len(rep.graph) - N} keys")
    N = len(rep.graph)
    mark_time()

    # Prepare the base MESSAGEix computations
    with silence_log("genno", logging.CRITICAL):
        try:
            rep.add_tasks()
        except KeyExistsError:
            pass  # `rep` was produced with Reporter.from_scenario()

    log.info(f"{len(rep.graph)} total keys")
    mark_time()
