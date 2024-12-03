"""Utility code for MESSAGEix-Transport."""

import logging
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Union

from message_ix_models import Context
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import numbers

    from genno.types import AnyQuantity

log = logging.getLogger(__name__)


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


def wildcard(value, units, dims: Sequence[Hashable]) -> "AnyQuantity":
    """Return a Quantity with 1 label "*" along each of `dims`.

    .. todo:: Move upstream, to `genno`.
    """
    import genno

    coords = {d: ["*"] for d in dims}
    try:
        return genno.Quantity(value, coords=coords, units=units)
    except TypeError:  # genno < 1.25
        return genno.Quantity(value, units).expand_dims(coords)
