"""Cache data for expensive operations.

This module extends :class:`genno.caching.Encoder` to handle classes common in
:mod:`message_ix_models`, so these can be used as arguments to cached functions and
included in the computed cache key:

- :class:`sdmx.model.IdentifiableArtefact`, including :class:`.Code`: hashed as their
  string representation / ID.
- :class:`ixmp.Platform`, :class:`xarray.Dataset`: ignored, with a warning logged.
- :class:`.ScenarioInfo`: only the :attr:`~ScenarioInfo.set` entries are hashed.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import is_dataclass
from enum import Enum
from types import FunctionType
from typing import Union

import genno.caching
import ixmp
import sdmx.model
import xarray as xr
from genno import Computer

from message_ix_models.types import AnyQuantity

from ._dataclasses import asdict
from .context import Context
from .scenarioinfo import ScenarioInfo

log = logging.getLogger(__name__)

# Computer to store the config used by the decorator. See .util.config.Config.cache_path
# and .cache_skip that update the contents.
COMPUTER = Computer()

# Show genno how to hash function arguments seen in message_ix_models


def _quantity(o: "AnyQuantity"):
    return tuple(o.to_series().to_dict())


try:
    genno.caching.Encoder.register(AnyQuantity)(_quantity)
except TypeError:  # Python 3.10 or earlier
    from genno.core.attrseries import AttrSeries
    from genno.core.sparsedataarray import SparseDataArray

    genno.caching.Encoder.register(AttrSeries)(_quantity)
    genno.caching.Encoder.register(SparseDataArray)(_quantity)


# Upstream
@genno.caching.Encoder.register
def _enum(o: Enum):
    return repr(o)


@genno.caching.Encoder.register
def _sdmx_identifiable(o: sdmx.model.IdentifiableArtefact):
    return str(o)


@genno.caching.Encoder.register
def _sdmx_internationalstring(o: sdmx.model.InternationalString):
    return tuple(o.localizations)


# First-party
@genno.caching.Encoder.register
def _context(o: Context):
    return o.asdict()


@genno.caching.Encoder.register
def _dataclass(o: object):
    return (
        asdict(o)
        if (is_dataclass(o) and not isinstance(o, type))
        else json.JSONEncoder().default(o)
    )


@genno.caching.Encoder.register
def _repr_only(o: Union[FunctionType]):
    return repr(o)


@genno.caching.Encoder.register
def _si(o: ScenarioInfo):
    return dict(o.set)


genno.caching.Encoder.ignore(xr.DataArray, xr.Dataset, ixmp.Platform)


def cached(func: Callable) -> Callable:
    """Decorator to cache the return value of a function `func`.

    On a first call, the data requested is returned and also cached under
    :meth:`.Config.cache_path`. On subsequent calls, if the cache exists, it is used
    instead of calling the (possibly slow) `func`.

    When :attr:`.Config.cache_skip` is :any:`True`, `func` is always called.

    See also
    --------
    :doc:`genno:cache` in the :mod:`genno` documentation
    """
    # Use the genno internals to wrap the function
    cached_load = genno.caching.decorate(func, computer=COMPUTER)

    if cached_load.__doc__ is not None:
        # Determine the indent
        line = cached_load.__doc__.split("\n")[-1]
        indent = len(line) - len(line.lstrip(" "))

        # Add a note that the results are cached
        cached_load.__doc__ += (
            f"\n\n{' ' * indent}Data returned by this function is cached using "
            ":func:`.cached`; see also :data:`.SKIP_CACHE`."
        )

    return cached_load
