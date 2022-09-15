from collections import defaultdict
from functools import lru_cache
from itertools import product
from typing import Tuple

import numpy as np
import pandas as pd
import xarray as xr
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import load_private_data
from sdmx.model import Code

# Configuration files
METADATA = [
    # Information about MESSAGE-water
    ("water", "config"),
    ("water", "set"),
    ("water", "technology"),
]


def read_config(context=None):
    """Read the water model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """

    context = context or Context.get_instance(0)

    # if context.nexus_set == 'nexus':
    if "water set" in context:
        # Already loaded
        return context

    # Load water configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_private_data(*_parts)

    return context


@lru_cache()
def map_add_on(rtype=Code):
    """Map addon & type_addon in ``sets.yaml``."""
    dims = ["add_on", "type_addon"]

    # Retrieve configuration
    context = read_config()

    # Assemble group information
    result = defaultdict(list)

    for indices in product(*[context["water set"][d]["add"] for d in dims]):
        # Create a new code by combining two
        result["code"].append(
            Code(
                id="".join(c.id for c in indices),
                name=", ".join(c.name for c in indices),
            )
        )

        # Tuple of the values along each dimension
        result["index"].append(tuple(c.id for c in indices))

    if rtype == "indexers":
        # Three tuples of members along each dimension
        indexers = zip(*result["index"])
        indexers = {
            d: xr.DataArray(list(i), dims="consumer_group")
            for d, i in zip(dims, indexers)
        }
        indexers["consumer_group"] = xr.DataArray(
            [c.id for c in result["code"]], dims="consumer_group",
        )
        return indexers
    elif rtype is Code:
        return sorted(result["code"], key=str)
    else:
        raise ValueError(rtype)


def add_commodity_and_level(df, default_level=None):
    # Add input commodity and level
    t_info = Context.get_instance()["water set"]["technology"]["add"]
    c_info = get_codes("commodity")

    @lru_cache()
    def t_cl(t):
        input = t_info[t_info.index(t)].anno["input"]
        # Commodity must be specified
        commodity = input["commodity"]
        # Use the default level for the commodity in the RES (per
        # commodity.yaml)
        level = (
            input.get("level", "water_supply")
            or c_info[c_info.index(commodity)].anno.get("level", None)
            or default_level
        )

        return commodity, level

    def func(row):
        row[["commodity", "level"]] = t_cl(row["technology"])
        return row

    return df.apply(func, axis=1)


def map_yv_ya_lt(periods: Tuple[int, ...], lt, ya=None) -> pd.DataFrame:
    """All meaningful combinations of (vintage year, active year) given `periods`.
    Parameters
    ----------
    labels : pandas.DataFrame
        Each column (dimension) corresponds to one in `df`. Each row represents one
        matched set of labels for those dimensions.
    lt : int, lifetime
    """
    # commented: currently unused, this does the same as the line below, using (start
    # period, final period, uniform ``duration_period). The intermediate periods are
    # inferred
    # _s = slice(periods_or_start, end + 1, dp)
    # data = np.mgrid[_s, _s]

    if not ya:
        ya = periods[0]
        print(("first active year set as"), ya)
    if not lt:
        raise ValueError("Add a fixed lifetime parameter 'lt'")

    # Create a mesh grid using numpy built-ins
    data = np.meshgrid(periods, periods, indexing="ij")
    # Take the upper-triangular portion (setting the rest to 0), reshape
    data = np.triu(data).reshape((2, -1))
    # Filter only non-zero pairs
    df = pd.DataFrame(
        filter(sum, zip(data[0, :], data[1, :])),
        columns=["year_vtg", "year_act"],
        dtype=np.int64,
    )
    df = df[df.year_act >= ya]
    df = df[(df.year_act - df.year_vtg) <= lt]
    return df
