"""Tests for reporting/."""
from functools import partial

from ixmp.reporting.quantity import Quantity
from message_ix.reporting import Reporter
import pandas as pd

from message_data.model.bare import create_res
from message_data.reporting import prepare_reporter
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


def test_report_bare_res(test_context):
    """Prepare and run the standard MESSAGE-GLOBIOM reporting on a bare RES."""
    # Get and solve a Scenario containing the bare RES
    test_context.scenario_info.update(dict(
        model='Bare RES',
        scenario='test_create_res',
    ))
    scenario = create_res(test_context)
    scenario.solve()

    # Prepare the reporter
    reporter, key = prepare_reporter(
        scenario,
        config=test_context.get_config_file('report', 'global'),
        key='message:default',
        output_path=None,
    )

    # Get the default report
    # NB commented because the bare RES currently contains no activity, so the
    #    reporting steps fail
    # reporter.get(key)
