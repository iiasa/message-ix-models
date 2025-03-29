"""Utilities for working with :mod:`.genno`.

Most code appearing here **should** be migrated upstream, to genno itself.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity, KeyLike

try:
    from genno.operator import as_quantity
except ImportError:
    # genno < 1.25, e.g. with message_ix/ixmp 3.7.0
    # TODO Remove when support for these upstream versions is dropped

    def as_quantity(info: Union[dict, float, str]) -> "AnyQuantity":
        import genno
        import pandas as pd
        from iam_units import registry

        if isinstance(info, str):
            q = registry.Quantity(info)
            return genno.Quantity(q.magnitude, units=q.units)
        elif isinstance(info, float):
            return genno.Quantity(info)
        elif isinstance(info, dict):
            data = info.copy()
            dim = data.pop("_dim")
            unit = data.pop("_unit")
            return genno.Quantity(pd.Series(data).rename_axis(dim), units=unit)
        else:
            raise TypeError(type(info))


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
    "as_quantity",
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
