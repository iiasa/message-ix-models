from collections import defaultdict
from functools import lru_cache
from itertools import product

import pandas as pd
import xarray as xr
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import as_codes, eval_anno, load_private_data
from sdmx.model import Code

from message_data.model.transport.common import METADATA


def read_config(context=None):
    """Read the transport model configuration / metadata and store on `context`.

    Files from :data:`.METADATA` are stored with keys like "transport set" corresponding
    to :file:`data/transport/set.yaml`. Numerical values are converted to computation-
    ready data structures.
    """
    context = context or Context.get_instance(0)

    if "transport set" in context:
        # Already loaded
        return

    # Load transport configuration, copy onto the context
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_private_data(*_parts)

    # Merge technology.yaml with set.yaml
    context["transport set"]["technology"]["add"] = context.pop("transport technology")

    # Convert some values to codes
    for set_name, info in context["transport set"].items():
        try:
            info["add"] = as_codes(info["add"])
        except KeyError:
            pass


def consumer_groups(rtype=Code):
    """Iterate over consumer groups in ``sets.yaml``.

    NB this low-level method requires the transport configuration to be loaded
    (:func:`.read_config`), but does not perform this step itself.

    Parameters
    ----------
    rtype : optional
        Return type.
    """
    dims = ["area_type", "attitude", "driver_type"]
    # Assemble group information
    result = defaultdict(list)

    set_config = Context.get_instance()["transport set"]

    for indices in product(*[set_config[d]["add"] for d in dims]):
        # Create a new code by combining three
        result["code"].append(
            Code(
                id="".join(c.id for c in indices),
                name=", ".join(str(c.name) for c in indices),
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


def add_commodity_and_level(df: pd.DataFrame, default_level=None) -> pd.DataFrame:
    """Add input 'commodity' and 'level' to `df` based on 'technology'."""

    # Retrieve transport technology information from configuration
    ctx = Context.get_instance()
    t_info = ctx["transport set"]["technology"]["add"]

    # Retrieve general commodity information
    c_info = get_codes("commodity")

    @lru_cache()
    def t_cl(t: str) -> pd.Series:
        """Return the commodity and level given technology `t`."""
        # Retrieve the "input" annotation for this technology
        input = eval_anno(t_info[t_info.index(t)], "input")

        # Commodity ID
        commodity = input["commodity"]

        # Retrieve the code for this commodity
        c_code = c_info[c_info.index(commodity)]

        # Level, in order of precedence:
        # 1. Technology-specific input level from `t_code`.
        # 2. Default level for the commodity from `c_code`.
        # 3. `default_level` argument to this function.
        level = (
            input.get("level", None) or eval_anno(c_code, id="level") or default_level
        )

        return pd.Series(dict(commodity=commodity, level=level))

    # Process every row in `df`; return a new DataFrame
    return df.combine_first(df["technology"].apply(t_cl))
