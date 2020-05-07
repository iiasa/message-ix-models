"""Atomic computations for MESSAGEix-GLOBIOM.

Some of these may migrate upstream to message_ix or ixmp in the future.
"""
from itertools import zip_longest
import logging

from ixmp.reporting import Quantity
from ixmp.reporting.utils import collect_units
from message_ix.reporting.computations import *  # noqa: F401,F403
from message_ix.reporting.computations import concat

# Computations for specific models and projects
from message_data.model.transport.report import (  # noqa: F401
    check_computation as transport_check
)


log = logging.getLogger(__name__)


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


def update_scenario(scenario, *quantities, params=[]):
    """Update *scenario* with computed data from reporting *quantities*.

    Parameters
    ----------
    scenario : .Scenario
    quantities : .Quantity or pd.DataFrame
        If DataFrame, must be valid input to :meth:`.Scenario.add_par`.
    params : list of str, optional
        For every element of `quantities` that is a pd.DataFrame, the element
        of `params` at the same index gives the name of the parameter to
        update.
    """
    log.info("Update '{0.model}/{0.scenario}#{0.version}'".format(scenario))
    scenario.check_out()

    for order, (qty, par_name) in enumerate(zip_longest(quantities, params)):
        if isinstance(qty, Quantity):
            # Convert a Quantity to a DataFrame
            par_name = qty.name
            new = qty.to_series() \
                     .reset_index() \
                     .rename(columns={par_name: 'value'})
            new['unit'] = '{:~}'.format(qty.attrs['_unit'])
            qty = new

        # Add the data
        log.info(f'  {repr(par_name)} ← {len(qty)} rows')
        scenario.add_par(par_name, qty)

    scenario.commit('Data added using '
                    'message_data.reporting.computations.update_scenario')
