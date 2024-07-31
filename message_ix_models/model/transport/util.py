"""Utility code for MESSAGEix-Transport."""

import logging
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Union

import pandas as pd

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import numbers

log = logging.getLogger(__name__)


def input_commodity_level(
    context: Context, df: pd.DataFrame, default_level=None
) -> pd.DataFrame:
    """Add input 'commodity' and 'level' to `df` based on 'technology'.

    .. deprecated:: 2023-02-27
       Use :func:`.computations.input_commodity_level` instead.
    """
    # Retrieve transport technology information from configuration
    t_info = context.transport.spec.add.set["technology"]

    # Retrieve general commodity information
    c_info = get_codes("commodity")

    @lru_cache()
    def t_cl(t: str) -> pd.Series:
        """Return the commodity and level given technology `t`."""
        # Retrieve the "input" annotation for this technology
        input = t_info[t_info.index(t)].eval_annotation("input")

        # Commodity ID
        commodity = input["commodity"]

        # Retrieve the code for this commodity
        c_code = c_info[c_info.index(commodity)]

        # Level, in order of precedence:
        # 1. Technology-specific input level from `t_code`.
        # 2. Default level for the commodity from `c_code`.
        # 3. `default_level` argument to this function.
        level = (
            input.get("level", None) or c_code.eval_annotation("level") or default_level
        )

        return pd.Series(dict(commodity=commodity, level=level))

    # Process every row in `df`; return a new DataFrame
    return df.combine_first(df["technology"].apply(t_cl))


def path_fallback(context_or_regions: Union[Context, str], *parts) -> Path:
    """Return a :class:`.Path` constructed from `parts`.

    If ``context.model.regions`` (or a string value as the first argument) is defined
    and the file exists in a subdirectory of :file:`data/transport/{regions}/`, return
    its path; otherwise, return the path in :file:`data/transport/`.
    """
    if isinstance(context_or_regions, str):
        regions = context_or_regions
    else:
        # Use a value from a Context object, or a default
        regions = context_or_regions.model.regions

    candidates = (
        package_data_path("transport", regions, *parts),
        package_data_path("transport", *parts),
    )

    for c in candidates:
        if c.exists():
            return c

    raise FileNotFoundError(candidates)


def sum_numeric(iterable: Iterable, /, start=0) -> "numbers.Real":
    """Sum only the numeric values in `iterable`."""
    result = start
    for item in iterable:
        try:
            result += item
        except TypeError:
            pass
    return result
