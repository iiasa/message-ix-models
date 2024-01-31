"""Utility code for MESSAGEix-Transport."""
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, Hashable, Iterable, List, Optional, Tuple, Union

import pandas as pd
from genno import Key
from iam_units import registry  # noqa: F401
from message_ix_models import Context, Spec
from message_ix_models.model.structure import get_codes
from message_ix_models.util import private_data_path

log = logging.getLogger(__name__)


class KeySequence:
    """Utility class for generating sets of related :class:`Keys <.Key>`.

    .. todo:: Move upstream, to :mod:`genno`.
    """

    base: Key
    _generated: Dict[Hashable, Key]

    def __init__(self, *args, **kwargs):
        self.base = Key(*args, **kwargs)
        self._generated = {}

    def __next__(self) -> Key:
        int_keys: Iterable[int] = [
            (t if isinstance(t, int) else -1) for t in self._generated
        ]
        tag = max(int_keys) + 1
        result = self._generated[tag] = self.base + str(tag)
        return result

    def __call__(self, tag: Optional[Hashable] = None) -> Key:
        if tag:
            result = self._generated[tag] = self.base + str(tag)
            return result
        else:
            return next(self)

    def __getitem__(self, index: int) -> Key:
        tag = str(index)
        result = self._generated[tag] = self.base + tag
        return result

    @property
    def name(self) -> str:
        return self.base.name


def input_commodity_level(
    context: Context, df: pd.DataFrame, default_level=None
) -> pd.DataFrame:
    """Add input 'commodity' and 'level' to `df` based on 'technology'.

    .. deprecated:: 2023-02-27
       Use :func:`.computations.input_commodity_level` instead.
    """
    # Retrieve transport technology information from configuration
    t_info = context.transport.set["technology"]["add"]

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
        private_data_path("transport", regions, *parts),
        private_data_path("transport", *parts),
    )

    for c in candidates:
        if c.exists():
            return c

    raise FileNotFoundError(candidates)


def get_techs(context) -> Tuple[Spec, List, Dict]:
    """Return info about transport technologies, given `context`."""
    from . import build

    # Get a specification that describes this setting
    # TODO assume this is on `context`. This is valid within the code proper, but some
    #      tests currently rely on get_spec() being called here.
    spec = build.get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"].copy()

    # Subsets of transport technologies for aggregation and filtering
    t_groups: Dict[str, List[str]] = {"non-ldv": []}
    for tech in filter(  # Only include those technologies with children
        lambda t: len(t.child), context.transport.set["technology"]["add"]
    ):
        t_groups[tech.id] = list(c.id for c in tech.child)
        # Store non-LDV technologies
        if tech.id != "LDV":
            t_groups["non-ldv"].extend(t_groups[tech.id])

    return spec, technologies, t_groups
