"""Simulated solution data for testing :mod:`~message_ix_models.report`."""
import logging
from collections import ChainMap, defaultdict
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union

import pandas as pd
from dask.core import quote
from genno import configure
from ixmp.reporting import RENAME_DIMS
from message_ix.models import MESSAGE_ITEMS, item
from message_ix.reporting import Key, KeyExistsError, Quantity, Reporter
from pandas.api.types import is_scalar

from message_ix_models import ScenarioInfo
from message_ix_models.util._logging import mark_time, silence_log

__all__ = [
    "SIMULATE_ITEMS",
    "add_simulated_solution",
    "data_from_file",
    "simulate_qty",
]

log = logging.getLogger(__name__)

# Copied and expanded from message_ix.models.MESSAGE_ITEMS, where these entries are
# commented because of JDBCBackend limitations.
# TODO read from that location once possible
MESSAGE_VARS = {
    # Activity
    "ACT": item("var", "nl t yv ya m h"),
    # "ACTIVITY_BOUND_ALL_MODES_LO": item("var", ""),
    # "ACTIVITY_BOUND_ALL_MODES_UP": item("var", ""),
    # "ACTIVITY_BOUND_LO": item("var", ""),
    # "ACTIVITY_BOUND_UP": item("var", ""),
    # "ACTIVITY_BY_RATING": item("var", ""),
    # "ACTIVITY_CONSTRAINT_LO": item("var", ""),
    # "ACTIVITY_CONSTRAINT_UP": item("var", ""),
    # "ACTIVITY_RATING_TOTAL": item("var", ""),
    # "ACTIVITY_SOFT_CONSTRAINT_LO": item("var", ""),
    # "ACTIVITY_SOFT_CONSTRAINT_UP": item("var", ""),
    # "ACT_LO": item("var", ""),
    # "ACT_RATING": item("var", ""),
    # "ACT_UP": item("var", ""),
    # "ADDON_ACTIVITY_LO": item("var", ""),
    # "ADDON_ACTIVITY_UP": item("var", ""),
    # Maintained capacity
    "CAP": item("var", "nl t yv ya"),
    # "CAPACITY_CONSTRAINT": item("var", ""),
    # "CAPACITY_MAINTENANCE": item("var", ""),
    # "CAPACITY_MAINTENANCE_HIST": item("var", ""),
    # "CAPACITY_MAINTENANCE_NEW": item("var", ""),
    # "CAP_FIRM": item("var", ""),
    # New capacity
    "CAP_NEW": item("var", "nl t yv"),
    # "CAP_NEW_LO": item("var", ""),
    # "CAP_NEW_UP": item("var", ""),
    # "COMMODITY_BALANCE_GT": item("var", ""),
    # "COMMODITY_BALANCE_LT": item("var", ""),
    # "COMMODITY_USE": item("var", ""),
    # "COMMODITY_USE_LEVEL": item("var", ""),
    # "COST_ACCOUNTING_NODAL": item("var", ""),
    # "COST_NODAL": item("var", ""),
    # "COST_NODAL_NET": item("var", ""),
    # "DEMAND": item("var", ""),
    # "DYNAMIC_LAND_SCEN_CONSTRAINT_LO": item("var", ""),
    # "DYNAMIC_LAND_SCEN_CONSTRAINT_UP": item("var", ""),
    # "DYNAMIC_LAND_TYPE_CONSTRAINT_LO": item("var", ""),
    # "DYNAMIC_LAND_TYPE_CONSTRAINT_UP": item("var", ""),
    # Emissions
    "EMISS": item("var", "n e type_tec y"),
    # "EMISSION_CONSTRAINT": item("var", ""),
    # "EMISSION_EQUIVALENCE": item("var", ""),
    # Extraction
    "EXT": item("var", "n c g y"),
    # "EXTRACTION_BOUND_UP": item("var", ""),
    # "EXTRACTION_EQUIVALENCE": item("var", ""),
    # "FIRM_CAPACITY_PROVISION": item("var", ""),
    # "GDP": item("var", ""),
    # Land scenario share
    "LAND": item("var", "n land_scenario y"),
    # "LAND_CONSTRAINT": item("var", ""),
    # "MIN_UTILIZATION_CONSTRAINT": item("var", ""),
    # "NEW_CAPACITY_BOUND_LO": item("var", ""),
    # "NEW_CAPACITY_BOUND_UP": item("var", ""),
    # "NEW_CAPACITY_CONSTRAINT_LO": item("var", ""),
    # "NEW_CAPACITY_CONSTRAINT_UP": item("var", ""),
    # "NEW_CAPACITY_SOFT_CONSTRAINT_LO": item("var", ""),
    # "NEW_CAPACITY_SOFT_CONSTRAINT_UP": item("var", ""),
    # Objective (scalar)
    "OBJ": dict(ix_type="var", idx_sets=[]),
    # "OBJECTIVE": item("var", ""),
    # "OPERATION_CONSTRAINT": item("var", ""),
    # Price of emissions
    "PRICE_COMMODITY": item("var", "n c l y h"),
    # Price of emissions
    "PRICE_EMISSION": item("var", "n e t y"),
    # Relation (lhs)
    "REL": item("var", "relation nr yr"),
    # "RELATION_CONSTRAINT_LO": item("var", ""),
    # "RELATION_CONSTRAINT_UP": item("var", ""),
    # "RELATION_EQUIVALENCE": item("var", ""),
    # "REN": item("var", ""),
    # "RENEWABLES_CAPACITY_REQUIREMENT": item("var", ""),
    # "RENEWABLES_EQUIVALENCE": item("var", ""),
    # "RENEWABLES_POTENTIAL_CONSTRAINT": item("var", ""),
    # "RESOURCE_CONSTRAINT": item("var", ""),
    # "RESOURCE_HORIZON": item("var", ""),
    # "SHARE_CONSTRAINT_COMMODITY_LO": item("var", ""),
    # "SHARE_CONSTRAINT_COMMODITY_UP": item("var", ""),
    # "SHARE_CONSTRAINT_MODE_LO": item("var", ""),
    # "SHARE_CONSTRAINT_MODE_UP": item("var", ""),
    # "SLACK_ACT_BOUND_LO": item("var", ""),
    # "SLACK_ACT_BOUND_UP": item("var", ""),
    # "SLACK_ACT_DYNAMIC_LO": item("var", ""),
    # "SLACK_ACT_DYNAMIC_UP": item("var", ""),
    # "SLACK_CAP_NEW_BOUND_LO": item("var", ""),
    # "SLACK_CAP_NEW_BOUND_UP": item("var", ""),
    # "SLACK_CAP_NEW_DYNAMIC_LO": item("var", ""),
    # "SLACK_CAP_NEW_DYNAMIC_UP": item("var", ""),
    # "SLACK_CAP_TOTAL_BOUND_LO": item("var", ""),
    # "SLACK_CAP_TOTAL_BOUND_UP": item("var", ""),
    # "SLACK_COMMODITY_EQUIVALENCE_LO": item("var", ""),
    # "SLACK_COMMODITY_EQUIVALENCE_UP": item("var", ""),
    # "SLACK_LAND_SCEN_LO": item("var", ""),
    # "SLACK_LAND_SCEN_UP": item("var", ""),
    # "SLACK_LAND_TYPE_LO": item("var", ""),
    # "SLACK_LAND_TYPE_UP": item("var", ""),
    # "SLACK_RELATION_BOUND_LO": item("var", ""),
    # "SLACK_RELATION_BOUND_UP": item("var", ""),
    # Stock
    "STOCK": item("var", "n c l y"),
    # "STOCK_CHG": item("var", ""),
    # "STOCKS_BALANCE": item("var", ""),
    # "STORAGE": item("var", ""),
    # "STORAGE_BALANCE": item("var", ""),
    # "STORAGE_BALANCE_INIT": item("var", ""),
    # "STORAGE_CHANGE": item("var", ""),
    # "STORAGE_CHARGE": item("var", ""),
    # "STORAGE_INPUT": item("var", ""),
    # "SYSTEM_FLEXIBILITY_CONSTRAINT": item("var", ""),
    # "SYSTEM_RELIABILITY_CONSTRAINT": item("var", ""),
    # "TOTAL_CAPACITY_BOUND_LO": item("var", ""),
    # "TOTAL_CAPACITY_BOUND_UP": item("var", ""),
}

# Items to included in a simulated solution: MESSAGE sets and parameters; some variables
SIMULATE_ITEMS = deepcopy(MESSAGE_ITEMS)
# Other MESSAGE variables
SIMULATE_ITEMS.update(MESSAGE_VARS)
# MACRO variables
SIMULATE_ITEMS.update(
    {
        "GDP": item("var", "n y"),
        "MERtoPPP": item("var", "n y"),
    }
)

configure(
    rename_dims=dict(
        node_rel="nr",
        year_rel="yr",
    ),
)


def dims_of(info: dict) -> Dict[str, str]:
    """Return a mapping from the full index names to short dimension IDs of `info`."""
    return {
        d: RENAME_DIMS.get(d, d)
        for d in (info.get("idx_names") or info.get("idx_sets") or [])
    }


def simulate_qty(
    name: str, info: dict, item_data: Union[dict, pd.DataFrame]
) -> Quantity:
    """Return simulated data for item `name`."""
    # Dimensions of the resulting quantity
    dims = list(dims_of(info).values())

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
        df = item_data.rename(columns=RENAME_DIMS)

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
            .set_axis(cols, axis=1)
            .set_index(cols[:-2])
        )
        # TODO pass units if they are unique
        return Quantity(tmp["value"], name=name)


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
    from importlib.metadata import version

    if version("message_ix") < "3.6":
        raise NotImplementedError(
            "Support for message_ix_models.report.sim.add_simulated_solution() with "
            "message_ix <= 3.5.0. Please upgrade to message_ix 3.6 or later."
        )

    mark_time()
    N = len(rep.graph)

    # Ensure "scenario" is present in the graph
    rep.graph.setdefault("scenario", None)

    # Add simulated data
    data = data or dict()
    for name, item_info in SIMULATE_ITEMS.items():
        key = Key(name, list(dims_of(item_info).values()))

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

        if item_info["ix_type"] == "set":
            # Add the set elements from `info`
            rep.add(RENAME_DIMS.get(name, name), quote(info.set[name]))
        elif item_info["ix_type"] in ("par", "var"):
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
            rep.add(
                key,
                simulate_qty,
                name=name,
                info=item_info,
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
