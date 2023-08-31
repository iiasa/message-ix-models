import logging
from collections import ChainMap, defaultdict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from dask.core import quote
from ixmp.reporting import RENAME_DIMS
from message_ix.models import MESSAGE_ITEMS
from message_ix.reporting import Key, KeyExistsError, Quantity, Reporter
from pandas.api.types import is_scalar

from message_ix_models import ScenarioInfo
from message_ix_models.util._logging import silence_log


# Shorthand for MESSAGE_VARS, below
def item(ix_type, idx_names):
    return dict(ix_type=ix_type, idx_names=tuple(idx_names.split()))


# Copied from message_ix.models.MESSAGE_ITEMS, where these entries are commented because
# of JDBCBackend limitations.
# TODO read from that location once possible
MESSAGE_VARS = {
    # Activity
    "ACT": item("var", "nl t yv ya m h"),
    # Maintained capacity
    "CAP": item("var", "nl t yv ya"),
    # New capacity
    "CAP_NEW": item("var", "nl t yv"),
    # Emissions
    "EMISS": item("var", "n e type_tec y"),
    # Extraction
    "EXT": item("var", "n c g y"),
    # Land scenario share
    "LAND": item("var", "n land_scenario y"),
    # Objective (scalar)
    "OBJ": dict(ix_type="var", idx_names=[]),
    # Price of emissions
    "PRICE_COMMODITY": item("var", "n c l y h"),
    # Price of emissions
    "PRICE_EMISSION": item("var", "n e t y"),
    # Relation (lhs)
    "REL": item("var", "relation nr yr"),
    # Stock
    "STOCK": item("var", "n c l y"),
}


def simulate_qty(name: str, item_info: dict, **data_kw: Any) -> Tuple[Key, Quantity]:
    """Return simulated data for item `name`."""
    # NB this is code lightly modified from make_df

    # Dimensions of the resulting quantity
    dims = list(
        map(
            lambda d: RENAME_DIMS.get(d, d),
            item_info.get("idx_names", []) or item_info.get("idx_sets", []),
        )
    )

    # Default values for every column
    data: Mapping = ChainMap(data_kw, defaultdict(lambda: None))

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
    # Data must be entirely empty, or complete
    assert not df.isna().any().any() or df.isna().all().all(), data
    assert not df.duplicated().any(), f"Duplicate data for simulated {repr(name)}"

    return Key(name, dims), Quantity(df.set_index(dims) if len(dims) else df)


def add_simulated_solution(
    rep: Reporter, info: ScenarioInfo, data: Optional[Dict] = None
):
    """Add a simulated model solution to `rep`, given `info` and `data`."""
    # Populate the sets (from `info`, maybe empty) and pars (empty)
    to_add = deepcopy(MESSAGE_ITEMS)
    # Populate variables
    to_add.update(MESSAGE_VARS)
    # Populate MACRO items
    to_add.update(
        {
            "GDP": item("var", "n y"),
            "MERtoPPP": item("var", "n y"),
        }
    )

    data = data or dict()

    for name, item_info in to_add.items():
        if item_info["ix_type"] == "set":
            # Add the set elements
            rep.add(RENAME_DIMS.get(name, name), quote(info.set[name]))
        elif item_info["ix_type"] in ("par", "var"):
            # Retrieve an existing key for `name`
            try:
                full_key = rep.full_key(name)
            except KeyError:
                full_key = None  # Not present in `rep`

            # Simulated data for name
            item_data = data.get(name, {})

            if full_key and not item_data:
                # Don't overwrite existing task with empty data
                continue

            # Store simulated data for this quantity
            key, qty = simulate_qty(name, item_info, **item_data)
            rep.add(key, qty, sums=True)
            # log.debug(f"{key}\n{qty}")

    # Prepare the base MESSAGEix computations
    with silence_log("genno", logging.CRITICAL):
        try:
            rep.add_tasks()
        except KeyExistsError:
            pass  # `rep` was produced with Reporter.from_scenario()
