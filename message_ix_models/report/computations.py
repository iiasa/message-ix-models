"""Atomic computations for MESSAGEix-GLOBIOM.

Some of these may migrate upstream to message_ix or ixmp in the future.
"""
from message_ix.reporting.computations import *  # noqa: F401,F403
from message_ix.reporting.computations import concat


def combine(*quantities, select, weights):
    """Sum distinct *quantities* by *weights*.

    Parameters
    ----------
    *quantities : Quantity
        The quantities to be added.
    select : list of dict
        Elements to be selected from each quantity. Must have the same number
        of elements as `quantities`.
    weights : list of float
        Weight applied to each . Must have the same number
        of elements as `quantities`.

    """
    # NB .transpose() is necessary when Quantity is AttrSeries.
    result = None

    for qty, s, w in zip(quantities, select, weights):
        multi = [dim for dim, values in s.items() if isinstance(values, list)]

        if result is None:
            result = w * (qty.sel(s).sum(dim=multi) if len(multi) else
                          qty.sel(s))
            dims = result.dims
        else:
            result += w * (qty.sel(s).sum(dim=multi) if len(multi) else
                           qty.sel(s)).transpose(*dims)

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
