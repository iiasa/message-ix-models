"""Tests for reporting/."""
from functools import partial

from ixmp.reporting.utils import Quantity
from message_ix.reporting import Reporter
import pandas as pd

from message_data.reporting.computations import combine


def test_computation_combine():
    rep = Reporter()

    # Add data to the Reporter
    foo = ['foo1', 'foo2']
    bar = ['bar1', 'bar2']

    a = pd.Series(
        [1, 2, 3, 4],
        index=pd.MultiIndex.from_product([foo, bar], names=['foo', 'bar']),
    )
    b = pd.Series(
        [10, 20, 30, 40],
        index=pd.MultiIndex.from_product([bar, foo], names=['bar', 'foo']),
    )
    c = pd.Series([100, 200], index=pd.Index(foo, name='foo'))

    rep.add('a', Quantity(a))
    rep.add('b', Quantity(b))
    rep.add('c', Quantity(c))

    rep.add('d', (partial(combine, weights=[0.5, 1, 2]), 'a', 'b', 'c'))

    assert rep.get('d').loc[('foo2', 'bar1')] == 3 * 0.5 + 20 * 1 + 200 * 2
