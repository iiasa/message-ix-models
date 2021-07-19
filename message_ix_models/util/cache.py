"""Cache data for expensive operations.

This code is lightly adapted from code in
https://github.com/transportenergy/ipcc-ar6-wg3-ch10
and earlier code by @khaeru.
"""
import functools
import json
import logging
import pathlib
import pickle
from hashlib import sha1
from typing import Callable

import ixmp
import xarray as xr
from sdmx.model import Code

from .context import Context

log = logging.getLogger(__name__)


#: Set to :obj:`True` to force reload.
SKIP_CACHE = False


class Encoder(json.JSONEncoder):
    """JSON Encoder that handles pathlib.Path; used by _arg_hash."""

    def default(self, o):
        if isinstance(o, (pathlib.Path, Code)):
            return str(o)
        elif isinstance(o, (xr.Dataset, ixmp.Platform)):
            log.warning(f"cached() key ignores {type(o)}")
            return ""
        elif o.__class__.__name__ == "ScenarioInfo":
            return dict(o.set)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)


def _arg_hash(*args, **kwargs):
    """Return a unique hash for *args, **kwargs; used by cached."""
    if len(args) + len(kwargs) == 0:
        unique = ""
    else:
        unique = json.dumps(args, cls=Encoder) + json.dumps(kwargs, cls=Encoder)

    # Uncomment for debugging
    # log.debug(f"Cache key hashed from: {unique}")

    return sha1(unique.encode()).hexdigest()


def cached(load_func: Callable) -> Callable:
    """Decorator to cache selected data.

    On a first call, the data requested is returned and also cached under
    :meth:`.Context.get_cache_path`. On subsequent calls, if the cache exists, it is
    used instead of calling the (possibly slow) `load_func`.

    When :data`SKIP_CACHE` is true, `load_func` is always called.
    """
    log.debug(f"Wrap {load_func.__name__} in cached()")

    # Wrap the call to load_func
    def cached_load(*args, **kwargs):
        # Path to the cache file
        name_parts = [load_func.__name__, _arg_hash(*args, **kwargs)]
        cache_path = (
            Context.get_instance()
            .get_cache_path("-".join(name_parts))
            .with_suffix(".pkl")
        )

        # Shorter name for logging
        short_name = f"{name_parts[0]}(<{name_parts[1][:8]}…>)"

        if not SKIP_CACHE and cache_path.exists():
            log.info(f"Cache hit for {short_name}")
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        else:
            log.info(f"Cache miss for {short_name}")
            data = load_func(*args, **kwargs)

            log.info(f"Cache {short_name} in {cache_path.parent}")
            with open(cache_path, "wb") as f:
                pickle.dump(data, f)

            return data

    update_wrapper(cached_load, load_func)

    return cached_load


def update_wrapper(wrapper, wrapped):
    """Update `wrapper` so it has the same docstring etc. as `wrapped`.

    This ensures it is picked up by Sphinx. Also add a note that the results are cached.
    """

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
