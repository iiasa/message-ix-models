"""Cache data for expensive operations."""
import functools
import json
import logging
import pathlib
from typing import Callable

import genno.caching
import ixmp
import xarray as xr
from genno import Computer
from sdmx.model import Code

from .context import Context
from .scenarioinfo import ScenarioInfo

log = logging.getLogger(__name__)


#: Set to :obj:`True` to force reload.
SKIP_CACHE = False

# Paths already logged, to decrease verbosity
PATHS_SEEN = set()


class Encoder(json.JSONEncoder):
    """:class:`.JSONEncoder` that handles classes common in :mod:`message_ix_models`.

    Used by :func:`cached` to serialize arguments as a unique string, then hash them.

    :class:`pathlib.Path`, :class:`sdmx.Code`
        Serialized as their string representation.
    :class:`ixmp.Platform`, :class:`xarray.Dataset`
        Ignored, with a warning logged.
    :class:`ScenarioInfo`
        Only the :attr:`~ScenarioInfo.set` entries are serialized.
    """

    def default(self, o):
        if isinstance(o, (pathlib.Path, Code)):
            return str(o)
        elif isinstance(o, (xr.Dataset, ixmp.Platform)):
            log.warning(f"cached() key ignores {type(o)}")
            return ""
        elif isinstance(o, ScenarioInfo):
            return dict(o.set)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)


# Override genno's built-in encoder with the one above, covering more cases
genno.caching.PathEncoder = Encoder  # type: ignore [assignment, misc]


def cached(func: Callable) -> Callable:
    """Decorator to cache the return value of a function `func`.

    On a first call, the data requested is returned and also cached under
    :meth:`.Context.get_cache_path`. On subsequent calls, if the cache exists, it is
    used instead of calling the (possibly slow) `func`.

    When :data:`SKIP_CACHE` is true, `func` is always called.

    See also
    --------
    :doc:`genno:cache` in the :mod:`genno` documentation
    """
    # Determine and create the cache path
    cache_path = Context.get_instance(-1).get_cache_path()
    cache_path.mkdir(exist_ok=True, parents=True)

    if cache_path not in PATHS_SEEN:
        log.debug(f"{func.__name__}() will cache in {cache_path}")
        PATHS_SEEN.add(cache_path)

    # Create a temporary/throwaway Computer to carry values to genno.caching; use the
    # genno internals to wrap the function.
    # TODO this indicates poor design; instead make_cache_decorator() should take the
    #      args directly
    cached_load = genno.caching.make_cache_decorator(
        Computer(cache_path=cache_path, cache_skip=SKIP_CACHE), func
    )

    update_wrapper(cached_load, func)

    return cached_load


def update_wrapper(wrapper, wrapped):
    """Update `wrapper` so it has the same docstring etc. as `wrapped`.

    This ensures it is picked up by Sphinx. Also add a note that the results are cached.
    """
    # Let the functools equivalent do most of the work
    functools.update_wrapper(wrapper, wrapped)

    if wrapper.__doc__ is None:
        return

    # Determine the indent
    line = wrapper.__doc__.split("\n")[-1]
    indent = len(line) - len(line.lstrip(" "))

    wrapper.__doc__ += (
        f"\n\n{' ' * indent}Data returned by this function is cached using "
        ":func:`.cached`; see also :data:`.SKIP_CACHE`."
    )
