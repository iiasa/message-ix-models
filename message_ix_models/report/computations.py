"""Atomic computations for MESSAGEix-GLOBIOM.

Some of these may migrate upstream to message_ix or ixmp in the future.
"""
from ixmp.reporting.utils import collect_units
from message_ix.reporting.computations import *  # noqa: F401,F403
from message_ix.reporting.computations import concat


def combine(*quantities, select=None, weights=None):
    """Sum distinct *quantities* by *weights*.

    Parameters
    ----------
    *quantities : Quantity
        The quantities to be added.
    select : list of dict
        Elements to be selected from each quantity. Must have the same number
        of elements as `quantities`.
    weights : list of float
        Weight applied to each quantity. Must have the same number of elements
        as `quantities`.

    Raises
    ------
    ValueError
        If the *quantities* have mismatched units.
    """
    # Handle arguments
    select = select or len(quantities) * [{}]
    weights = weights or len(quantities) * [1.]

    # Check units
    units = collect_units(*quantities)
    for u in units:
        # TODO relax this condition: modify the weights with conversion factors
        #      if the units are compatible, but not the same
        if u != units[0]:
            raise ValueError(f'Cannot combine() units {units[0]} and {u}')
    units = units[0]

    result = 0
    ref_dims = None

    for quantity, selector, weight in zip(quantities, select, weights):
        ref_dims = ref_dims or quantity.dims

        # Select data
        temp = quantity.sel(selector)

        # Dimensions along which multiple values are selected
        multi = [dim for dim, values in selector.items()
                 if isinstance(values, list)]

        if len(multi):
            # Sum along these dimensions
            temp = temp.sum(dim=multi)

        # .transpose() is necessary when Quantity is AttrSeries
        if len(quantity.dims) > 1:
            transpose_dims = tuple(filter(lambda d: d in temp.dims, ref_dims))
            temp = temp.transpose(*transpose_dims)

        result += weight * temp

    result.attrs['_unit'] = units

    return result


def group_sum(qty, group, sum):
    """Group by dimension *group*, then sum across dimension *sum*.

    The result drops the latter dimension.
    """
    return concat([values.sum(dim=[sum]) for _, values in qty.groupby(group)],
                  dim=group)


def share_curtailment(curt, *parts):
    """Apply a share of *curt* to the first of *parts*.

    If this is being used, it usually will indicate the need to split *curt*
    into multiple technologies; one for each of *parts*.
    """
    return parts[0] - curt * (parts[0] / sum(parts))
