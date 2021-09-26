from collections import defaultdict
from functools import lru_cache
from itertools import product

import xarray as xr
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import load_private_data
from sdmx.model import Code

#: Valid values of Context.regions for MESSAGEix-Transport; default first.
SETTINGS = dict(
    regions=["R11", "R12", "R14", "ISR"],
)


# Configuration files
METADATA = [
    # Information about MESSAGE-water
    ("water", "config"),
    ("water", "set"),
    ("water", "technology"),
    ("water", "set_cooling"),
    ("water", "technology_cooling"),
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

    # Merge technology.yaml with set.yaml
    context["water set"]["technology"]["add"] = context.pop("water technology")
    # else:
    #     if "water set_cooling" in context:
    #         # Already loaded
    #         return context
    #
    #     # Load water configuration
    #     for parts in METADATA:
    #         # Key for storing in the context
    #         key = " ".join(parts)
    #
    #         # Actual filename parts; ends with YAML
    #         _parts = list(parts)
    #         _parts[-1] += ".yaml"
    #
    #         context[key] = load_private_data(*_parts)
    #
    #     # Merge technology.yaml with set.yaml
    #     context["water set"]["technology"]["add"] = context.pop("water technology_cooling")
    # Convert some values to codes
    # for set_name, info in context["water set"].items():
    #     try:
    #         info["add"] = as_codes(info["add"])
    #     except KeyError:
    #         pass

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
            [c.id for c in result["code"]],
            dims="consumer_group",
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
