"""Utilities for working with :mod:`.genno`.

Most code appearing here **should** be migrated upstream, to genno itself.
"""

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from genno.types import AnyQuantity

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


__all__ = [
    "as_quantity",
]
