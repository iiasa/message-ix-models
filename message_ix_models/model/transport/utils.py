from collections import defaultdict
from functools import lru_cache
from itertools import product

import xarray as xr

from message_data.tools import Code, as_codes, get_context, load_data, set_info


# Configuration files
METADATA = [
    # Information about MESSAGE-Transport
    ('transport', 'callback'),
    ('transport', 'config'),
    ('transport', 'set'),
    ('transport', 'technology'),
    # Information about the MESSAGE V model
    ('transport', 'migrate', 'set'),
]

# Files containing data for input calculations and assumptions
FILES = [
    'ldv_class',
    'mer_to_ppp',
    'population-suburb-share',
    'ma3t/population',
    'ma3t/attitude',
    'ma3t/driver',
]


def read_config():
    """Read the transport model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """
    context = get_context()

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

    # Storage for exogenous data
    context.data = xr.Dataset()

    # Load data files
    for key in FILES:
        context.data[key] = load_data(context, 'transport', key,
                                      rtype=xr.DataArray)

    # Convert scalar parameters
    for key, val in context['transport callback'].pop('params').items():
        context.data[key] = eval(val) if isinstance(val, str) else val

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


def add_commodity_and_level(df, default_level=None):
    # Add input commodity and level
    t_info = get_context()["transport set"]["technology"]["add"]
    c_info = set_info("commodity")

    @lru_cache()
    def t_cl(t):
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

        return commodity, level

    def func(row):
        row[['commodity', 'level']] = t_cl(row['technology'])
        return row

    return df.apply(func, axis=1)
