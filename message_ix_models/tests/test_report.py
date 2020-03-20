"""Tests for reporting/."""
from functools import partial

from ixmp.reporting.quantity import Quantity
from message_ix.reporting import Reporter
import pandas as pd
import pytest
import yaml

from message_data.reporting import prepare_reporter
from message_data.reporting.computations import combine


# Minimal reporting configuration for testing
MIN_CONFIG = {
    'units': {
        'replace': {'???': ''},
    },
}


@pytest.fixture
def global_config(test_context):
    yield test_context.get_config_file('report', 'global')


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


def test_report_bare_res(test_context, solved_res, global_config):
    """Prepare and run the standard MESSAGE-GLOBIOM reporting on a bare RES."""
    # Prepare the reporter
    reporter, key = prepare_reporter(
        solved_res,
        config=global_config,
        key='message:default',
        output_path=None,
    )

    # Get the default report
    # NB commented because the bare RES currently contains no activity, so the
    #    reporting steps fail
    # reporter.get(key)


def test_apply_units(solved_res, tmp_path):
    qty = 'inv_cost'

    # Create a temporary config file
    config_path = tmp_path / 'reporting-config.yaml'
    config = MIN_CONFIG.copy()
    config_path.write_text(yaml.dump(config))

    # Prepare the reporter
    reporter, key = prepare_reporter(solved_res, config=config_path, key=qty,
                                     output_path=None)

    # Add some data to the scenario
    inv_cost = pd.DataFrame([
        ['R11_NAM', 'coal_ppl', '2010', 10.5, 'USD'],
        ['R11_LAM', 'coal_ppl', '2010',  9.5, 'USD'],
        ], columns='node_loc technology year_vtg value unit'.split())

    solved_res.remove_solution()
    solved_res.check_out()
    solved_res.add_par('inv_cost', inv_cost)

    # Units are retrieved
    assert reporter.get(key).attrs['_unit'] == 'USD_2005'

    # Add data with units that will be discarded
    inv_cost['unit'] = ['USD', 'kg']
    solved_res.add_par('inv_cost', inv_cost)

    # Units are discarded
    assert str(reporter.get(key).attrs['_unit']) == 'dimensionless'

    # Update configuration, re-create the reporter
    config['units']['apply'] = {'inv_cost': 'USD'}
    config_path.write_text(yaml.dump(config))
    solved_res.commit('')
    solved_res.solve()
    reporter, key = prepare_reporter(solved_res, config=config_path, key=qty,
                                     output_path=None)

    # Units are applied
    assert str(reporter.get(key).attrs['_unit']) == 'USD_2005'
