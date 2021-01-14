from collections import defaultdict
from functools import lru_cache
from itertools import product

import pandas as pd
import xarray as xr

from message_data.model.transport.common import METADATA, SETTINGS
from message_data.tools import Code, as_codes, get_context, load_data, set_info


def read_config(context=None):
    """Read the transport model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """
    context = context or get_context(strict=True)

    if "transport set" in context:
        # Already loaded
        return context

    # Load transport configuration
    for parts in METADATA:
        context.load_config(*parts)

    # Merge technology.yaml with set.yaml
    context["transport set"]["technology"]["add"] = (
        context.pop("transport technology")
    )

    # Convert some values to codes
    for set_name, info in context["transport set"].items():
        try:
            info["add"] = as_codes(info["add"])
        except KeyError:
            pass

    # Load data files
    for key in context["transport config"]["data files"]:
        context.data[f"transport {key.replace('/', ' ')}"] = load_data(
            context, "transport", key, rtype=xr.DataArray,
        )

    return context


@lru_cache()
def consumer_groups(rtype=Code):
    """Iterate over consumer groups in ``sets.yaml``."""
    dims = ['area_type', 'attitude', 'driver_type']

    # Retrieve configuration
    context = read_config()

    # Assemble group information
    result = defaultdict(list)

    for indices in product(*[
        context["transport set"][d]["add"] for d in dims
    ]):
        # Create a new code by combining three
        result['code'].append(Code(
            id=''.join(c.id for c in indices),
            name=', '.join(c.name for c in indices),
        ))

        # Tuple of the values along each dimension
        result['index'].append(tuple(c.id for c in indices))

    if rtype == 'indexers':
        # Three tuples of members along each dimension
        indexers = zip(*result['index'])
        indexers = {
            d: xr.DataArray(list(i), dims='consumer_group')
            for d, i in zip(dims, indexers)
        }
        indexers['consumer_group'] = xr.DataArray(
            [c.id for c in result['code']],
            dims='consumer_group',
        )
        return indexers
    elif rtype is Code:
        return sorted(result['code'], key=str)
    else:
        raise ValueError(rtype)


def add_commodity_and_level(
    df: pd.DataFrame,
    default_level=None,
) -> pd.DataFrame:
    """Add input 'commodity' and 'level' to `df` based on 'technology'."""

    # Retrieve transport technology information from configuration
    t_info = get_context()["transport set"]["technology"]["add"]

    # Retrieve general commodity information
    c_info = set_info("commodity")

    @lru_cache()
    def t_cl(t):
        """Return the commodity and level given technology `t`."""
        input = t_info[t_info.index(t)].anno["input"]
        # Commodity must be specified
        commodity = input['commodity']
        # Use the default level for the commodity in the RES (per
        # commodity.yaml)
        level = (
            input.get('level', None)
            or c_info[c_info.index(commodity)].anno.get('level', None)
            or default_level
        )

        return pd.Series(dict(commodity=commodity, level=level))

    def func(row):
        """Modify `row` to fill in 'commodity' and 'level' columns."""
        return row.fillna(t_cl(row['technology']))

    # Process every row in `df`; return a new DataFrame
    return df.apply(func, axis=1)
