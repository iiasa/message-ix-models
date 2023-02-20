from typing import Mapping, Optional, Tuple
from warnings import catch_warnings, filterwarnings

import pandas as pd
from iam_units import registry


def series_of_pint_quantity(*args, **kwargs) -> pd.Series:
    """Suppress a spurious warning.

    Creating a :class:`pandas.Series` with a list of :class:`pint.Quantity` triggers a
    warning “The unit of the quantity is stripped when downcasting to ndarray,” even
    though the entire object is being stored and the unit is **not** stripped. This
    function suppresses this warning.
    """
    with catch_warnings():
        filterwarnings(
            "ignore",
            message="The unit of the quantity is stripped when downcasting to ndarray",
            module="pandas.core.dtypes.cast",
        )

        return pd.Series(*args, **kwargs)


def convert_units(
    s: pd.Series,
    unit_info: Mapping[str, Tuple[float, str, Optional[str]]],
    store="magnitude",
) -> pd.Series:
    """Convert units of `s`, for use with :meth:`~pandas.DataFrame.apply`.

    ``s.name`` is used to retrieve a tuple of (`factor`, `input_unit`, `output_unit`)
    from `unit_info`. The (:class:`float`) values of `s` are converted to
    :class:`pint.Quantity` with the `input_unit` and factor; then cast to `output_unit`,
    if provided.

    Parameters
    ----------
    s : pandas.Series
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
    if store not in "magnitude quantity":
        raise ValueError(f"{store = }")

    # Retrieve the information from `unit_info`
    factor, unit_in, unit_out = unit_info[s.name]

    # Default: `unit_out` is the same as `unit_in`
    unit_out = unit_out or unit_in

    # - Convert the values to a pint.Quantity(array) with the input units
    # - Convert to output units
    # - According to `store`, either extract just the magnitude, or store scalar
    #   pint.Quantity objects.
    # - Reassemble into a series with index matching `s`
    result = registry.Quantity(factor * s.values, unit_in).to(unit_out)

    return series_of_pint_quantity(
        result.magnitude if store == "magnitude" else result.tolist(),
        index=s.index,
        dtype=(float if store == "magnitude" else object),
        name=s.name,
    )
