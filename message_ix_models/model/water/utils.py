import copy
import logging
from collections import defaultdict
from functools import lru_cache
from itertools import product
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
from sdmx.model.v21 import Code

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import (
    load_package_data,
)

log = logging.getLogger(__name__)

# Configuration files
METADATA = [
    # Information about MESSAGE-water
    ("water", "config"),
    ("water", "set"),
    ("water", "technology"),
]


def read_config(context: Optional[Context] = None):
    """Read the water model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """

    context = context or Context.get_instance(-1)

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

        context[key] = load_package_data(*_parts)

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
                id="".join(str(c.id) for c in indices),
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


def add_commodity_and_level(df: pd.DataFrame, default_level=None):
    # Add input commodity and level
    t_info: list = Context.get_instance()["water set"]["technology"]["add"]
    c_info: list = get_codes("commodity")

    @lru_cache()
    def t_cl(t):
        input = t_info[t_info.index(t)].annotations["input"]
        # Commodity must be specified
        commodity = input["commodity"]
        # Use the default level for the commodity in the RES (per
        # commodity.yaml)
        level = (
            input.get("level", "water_supply")
            or c_info[c_info.index(commodity)].annotations.get("level", None)
            or default_level
        )

        return commodity, level

    def func(row: pd.Series):
        row[["commodity", "level"]] = t_cl(row["technology"])
        return row

    return df.apply(func, axis=1)


def map_yv_ya_lt(
    periods: tuple[int, ...], lt: int, ya: Optional[int] = None
) -> pd.DataFrame:
    """All meaningful combinations of (vintage year, active year) given `periods`.

    Parameters
    ----------
    periods : tuple[int, ...]
        A sequence of years.
    lt : int, lifetime
    ya : int, active year
        The first active year.
    Returns
    -------
    pd.DataFrame
        A DataFrame with columns 'year_vtg' and 'year_act'.
    """
    if not ya:
        ya = periods[0]
        log.info(f"First active year set as {ya!r}")
    if not lt:
        raise ValueError("Add a fixed lifetime parameter 'lt'")

    # The following lines are the same as
    # message_ix.tests.test_feature_vintage_and_active_years._generate_yv_ya

    # - Create a mesh grid using numpy built-ins
    # - Take the upper-triangular portion (setting the rest to 0)
    # - Reshape
    data = np.triu(np.meshgrid(periods, periods, indexing="ij")).reshape((2, -1))
    # Filter only non-zero pairs
    df = pd.DataFrame(
        filter(sum, zip(data[0, :], data[1, :])),
        columns=["year_vtg", "year_act"],
        dtype=np.int64,
    )

    # Select values using the `ya` and `lt` parameters
    return df.loc[(ya <= df.year_act) & (df.year_act - df.year_vtg <= lt)].reset_index(
        drop=True
    )

def safe_concat(input_df: list[pd.DataFrame] | pd.DataFrame) -> pd.DataFrame:
    """Optimized concatenation that avoids unnecessary operations.
    For lists with single DataFrame, returns it directly. For multiple DataFrames,
    uses pd.concat with copy=False to avoid duplicating data. Handles single
    DataFrame inputs by returning them unchanged.
    """
    if isinstance(input_df, list):
        return input_df[0] if len(input_df) == 1 else pd.concat(input_df, copy=False)
    return input_df

# Helper function for deep merging
def deep_merge(base, diff):
    """Recursively merges diff dict into a deep copy of base dict."""
    merged = copy.deepcopy(base)  # Start with a deep copy of base
    for key, value in diff.items():
        # Check if the key exists in merged and both values are dictionaries
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            # Overwrite or add the key-value pair from diff
            merged[key] = value
    return merged


class Rule:
    def base(self):
        return self.Base

    def diff(self):
        return self.Diff

    def get_rule(self):
        result = []
        base_dict = self.base()  # Get the base dictionary once
        for diff_item in self.Diff:
            if diff_item["condition"] == "SKIP":
                continue
            if diff_item:  # Only process non-empty dictionaries
                # Perform a deep merge instead of shallow update
                merged = deep_merge(base_dict, diff_item)
                result.append(merged)
        return result

    def change_unit(self, conversion_factor: float, new_unit: str):
        # replace current unit with new unit without magic methods
        self.base()["unit"] = new_unit
        self.base().value *= conversion_factor

    def __init__(self, Base=None, Diff=None):
        # Define default pipe flags
        default_pipe_flags = {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": False,
        }

        # Initialize Base and Diff
        initialized_base = Base or {}
        self.Diff = Diff or [{}, {}]  # Keep original Diff initialization

        # Ensure 'pipe' key exists in initialized_base and is a dictionary
        if "pipe" not in initialized_base or not isinstance(
            initialized_base.get("pipe"), dict
        ):
            initialized_base["pipe"] = {}
            # Initialize pipe if not present or not a dict

        # Start with defaults and update with user-provided pipe flags
        final_pipe_flags = default_pipe_flags.copy()
        final_pipe_flags.update(initialized_base.get("pipe", {}))

        # Update the initialized_base with the final pipe flags
        initialized_base["pipe"] = final_pipe_flags

        # Assign the processed Base to self.Base
        self.Base = initialized_base
