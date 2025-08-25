"""Utilities for working with :mod:`.genno`.

Most code appearing here **should** be migrated upstream, to genno itself.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import KeyLike


try:
    from genno import Keys
except ImportError:
    # genno < 1.28 with message_ix/ixmp 3.7.0
    # TODO Remove when support for these upstream versions is dropped

    from types import SimpleNamespace

    Keys = SimpleNamespace  # type: ignore [assignment, misc]

__all__ = [
    "Collector",
    "Keys",
    "update_computer",
]


class Collector:
    """Helper class to collect and merge data at a target key.

    Example usage:

    .. code-block:: python

       # Create a Collector instance
       collector = Collector(target="FOO", key_cb="{}::foo".format)

       # Associate it with a particular Computer
       c = collector.computer = Computer()

       # Add a task
       collect("bar", func, "k1", "k2", arg1="baz", arg2="qux")

    These statements have the following effects:

    1. Add to `c` a task with the key "FOO" that calls :func:`.merge_data` on 1 or more
       inputs.
    2. Construct a key "bar::foo" using the `key_cb`.
    3. Add a task at "bar::foo" that calls :py:`func(k1, k2, arg1="baz", arg2="qux")`.
    4. Add "bar::foo" (denoting the output of (3)) to the keys merged by (1).
    """

    __slots__ = ("_computer", "_key_cb", "_target")

    def __init__(
        self, target: "KeyLike", key_cb: Callable[["KeyLike"], "KeyLike"]
    ) -> None:
        self._target = target
        self._key_cb = key_cb

    @property
    def computer(self) -> "Computer":
        return self._computer

    @computer.setter
    def computer(self, c: "Computer") -> None:
        from message_ix_models.report.operator import merge_data

        self._computer = c
        # Add the computation that merges data for this Collector
        assert self._target not in self._computer
        self._computer.graph[self._target] = (merge_data,)

    def __call__(self, _target_name: str, *args, **kwargs) -> "KeyLike":
        # Construct a key using the callback
        key = self._key_cb(_target_name)

        # Add a computation at `key` using the `args` and `kwargs`
        c = self._computer
        c.add(key, *args, **kwargs)

        # Extend the keys to be collected with `key`
        c.graph[self._target] = c.graph[self._target] + (key,)

        return key


def update_computer(a: "Computer", b: "Computer") -> None:
    """Update `a` with keys and tasks from `b`.

    For most keys, the task in `b` is copied to `a` at the same key. For the key
    "config", the contents of the :class:`dict` in `a` are updated with the values from
    the one in `b`. This overwrites or replaces existing configuration.

    .. todo:: Migrate upstream to a method like :py:`genno.Computer.update`.

    Raises
    ------
    RuntimeError
        - if any key already exists in `a` with a task different from the corresponding
          one in `b`.
        - if the key "context" maps to different :class:`Context` instances in `a` and
          `b`.
    """
    for k, v in b.graph.items():
        if k == "context":
            if a.graph.get(k, v) is not v:
                raise RuntimeError(f"Existing task {k} → {a.graph[k]} is not {v}")
        elif k == "config":
            target = a.graph.setdefault(k, dict())
            target.update(v)
        else:
            if k in a.graph and a.graph[k] != v:
                raise RuntimeError(
                    f"Existing task {k} → {a.graph[k]} would be overwritten by {v}"
                )
            assert k not in a.graph, k
            a.graph[k] = v
