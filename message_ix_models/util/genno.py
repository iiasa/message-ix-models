"""Utilities for working with :mod:`.genno`.

Most code appearing here **should** be migrated upstream, to genno itself.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genno import Computer

    from message_ix_models.types import KeyLike


log = logging.getLogger(__name__)


def insert(c: "Computer", key: "KeyLike", operation, tag: str = "pre") -> "KeyLike":
    """Insert a task that performs `operation` on `key`.

    1. The existing task at `key` is moved to a new key, ``{key}+{tag}``.
    2. A new task is inserted at `key` that performs `operation` on the output of the
       original task.

    One way to use :func:`insert` is with a ‘pass-through’ `operation` that, for
    instance, performs logging, assertions, or other steps, then returns its input
    unchanged. In this way, all other tasks in the graph referring to `key` receive
    exactly the same input as they would have previously, prior to the :func:`insert`
    call.

    It is also possible to insert `operation` that mutates its input in certain ways.

    .. todo:: Migrate to :py:`genno.Computer.insert()` or similar.

    Returns
    -------
    KeyLike
        same as the `key` parameter.
    """
    import genno

    # Determine a key for the task that to be shifted
    k_pre = genno.Key(key) + tag
    assert k_pre not in c

    # Move the existing task at `key` to `k_pre`
    c.graph[k_pre] = c.graph.pop(key)
    log.info(f"Move {key!r} to {k_pre!r}")

    # Add `operation` at `key`, operating on the output of the original task
    c.graph[key] = (operation, k_pre)

    return key
