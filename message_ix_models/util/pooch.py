"""Utilities for using :doc:`Pooch <pooch:about>`."""
import logging
from pathlib import Path

import pooch

from .context import Context

log = logging.getLogger(__name__)


def fetch(args, **kwargs) -> Path:
    """Create a :class:`~pooch.Pooch` instance and fetch a single file.

    Files are stored under the directory identified by :meth:`.Context.get_cache_path`,
    unless `args` provides another location.

    Parameters
    ----------
    args
        Passed to :func:`pooch.create`.
    kwargs
        Passed to :meth:`pooch.Pooch.fetch`.

    Returns
    -------
    Path
        Path to the fetched file.

    See also
    --------
    :func:`.snapshot.load`
    """
    args.setdefault("path", Context.get_instance(-1).get_cache_path())

    p = pooch.create(**args)

    if len(p.registry) > 1:  # pragma: no cover
        raise NotImplementedError("fetch() with registries with >1 files")

    path = Path(p.fetch(next(iter(p.registry.keys())), **kwargs))

    log.info(f"Fetched {path}")

    return path
