import logging
from collections.abc import Mapping
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from iam_units import registry

if TYPE_CHECKING:
    from .scenarioinfo import ScenarioInfo

log = logging.getLogger(__name__)


@singledispatch
def convert_units(data: Any, **kwargs):
    """Convert units of `data`.

    With :class:`.pandas.Series`: for use with :meth:`~pandas.DataFrame.apply`.

    :py:`data.name`` is used to retrieve a tuple of (`factor`, `input_unit`,
    `output_unit`) from `unit_info`. The (:class:`float`) values of `data` are converted
    to :class:`pint.Quantity` with the `input_unit` and factor; then cast to
    `output_unit`, if provided.

    Parameters
    ----------
    data : pandas.Series
    unit_info : dict (str -> tuple)
        Mapping from quantity name (matched to ``s.name``) to 3-tuples of (`factor`,
        `input_unit`, `output_unit`). `output_unit` may be :obj:`None`. For example,
        see :data:`.ikarus.UNITS`.
    store : "magnitude" or "quantity"
        If "magnitude", the values of the returned series are the magnitudes of the
        results, with no output units. If "quantity", the values are scalar
        :class:`~pint.Quantity` objects.

    Returns
    -------
    pandas.Series
        Same shape, index, and values as `s`, with output units.
    """

    raise TypeError(type(data))


@convert_units.register
def _(
    data: pd.Series,
    unit_info: Mapping[str, tuple[float, str, Optional[str]]],
    store="magnitude",
) -> pd.Series:
    if store not in "magnitude quantity":
        raise ValueError(f"{store = }")

    # Retrieve the information from `unit_info`
    factor, unit_in, unit_out = unit_info[data.name]

    # Default: `unit_out` is the same as `unit_in`
    unit_out = unit_out or unit_in

    # - Convert the values to a pint.Quantity(array) with the input units
    # - Convert to output units
    # - According to `store`, either extract just the magnitude, or store scalar
    #   pint.Quantity objects.
    # - Reassemble into a series with index matching `s`
    result = registry.Quantity(factor * data.values, unit_in).to(unit_out)

    return pd.Series(
        result.magnitude if store == "magnitude" else result.tolist(),
        index=data.index,
        dtype=(float if store == "magnitude" else object),
        name=data.name,
    )


@convert_units.register(pd.DataFrame)
def _(data: pd.DataFrame, info: "ScenarioInfo") -> pd.DataFrame:
    columns = ["technology", "commodity", "unit"]
    if not set(columns) <= set(data.columns):
        log.debug(f"No unit conversion for data with columns {list(data.columns)}")
        return data

    def _convert_group(df):
        """Convert `df` in which (technology, level) are uniform."""
        row = df.iloc[1, :]

        factor = registry.Quantity(1.0, row["unit"])
        try:
            factor = factor.to(info.io_units(row["technology"], row["commodity"]))
        except Exception as e:
            log.error(f"{type(e).__name__}: {e!s}")

        if factor.magnitude != 1.0:
            return df.eval("value = value * @factor.magnitude").assign(
                unit=f"{factor.units:~}"
            )
        else:
            return df

    return data.groupby(columns, group_keys=False)[data.columns].apply(_convert_group)


@convert_units.register(dict)
def _(data: dict[str, pd.DataFrame], info: "ScenarioInfo") -> dict[str, pd.DataFrame]:
    return {k: convert_units(df, info=info) for k, df in data.items()}
