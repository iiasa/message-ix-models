"""Tests for reporting/."""
from functools import partial

from ixmp.reporting.quantity import Quantity
from message_ix.reporting import Reporter
import pandas as pd
import pytest

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
    )

    # Get the default report
    # NB commented because the bare RES currently contains no activity, so the
    #    reporting steps fail
    # reporter.get(key)


# Common data for tests
DATA_INV_COST = pd.DataFrame([
    ['R11_NAM', 'coal_ppl', '2010', 10.5, 'USD'],
    ['R11_LAM', 'coal_ppl', '2010',  9.5, 'USD'],
    ], columns='node_loc technology year_vtg value unit'.split())

IAMC_INV_COST = dict(
    variable='Investment Cost',
    base='inv_cost:nl-t-yv',
    year_time_dim='yv',
    var=['t'],
    unit='EUR_2005',
)


def test_apply_units(bare_res):
    qty = 'inv_cost'

    # Create a temporary config dict
    config = MIN_CONFIG.copy()

    # Prepare the reporter
    bare_res.solve()
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Add some data to the scenario
    inv_cost = DATA_INV_COST.copy()
    bare_res.remove_solution()
    bare_res.check_out()
    bare_res.add_par('inv_cost', inv_cost)
    bare_res.commit('')
    bare_res.solve()

    # Units are retrieved
    USD_2005 = reporter.unit_registry.Unit('USD_2005')
    assert reporter.get(key).attrs['_unit'] == USD_2005

    # Add data with units that will be discarded
    inv_cost['unit'] = ['USD', 'kg']
    bare_res.remove_solution()
    bare_res.check_out()
    bare_res.add_par('inv_cost', inv_cost)

    # Units are discarded
    assert str(reporter.get(key).attrs['_unit']) == 'dimensionless'

    # Update configuration, re-create the reporter
    config['units']['apply'] = {'inv_cost': 'USD'}
    bare_res.commit('')
    bare_res.solve()
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Units are applied
    assert str(reporter.get(key).attrs['_unit']) == USD_2005

    # Update configuration, re-create the reporter
    config['iamc'] = [IAMC_INV_COST]
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Units are converted
    df = reporter.get('Investment Cost:iamc').as_pandas()
    assert set(df['unit']) == {'EUR_2005'}


def test_iamc_replace_vars(bare_res):
    """Test the 'iamc variable names' reporting configuration."""
    scen = bare_res

    qty = 'inv_cost'
    config = {
        'iamc': [IAMC_INV_COST],
        'iamc variable names': {
            'Investment Cost|Coal_Ppl': 'Investment Cost|Coal',
        }
    }
    scen.check_out()
    scen.add_par('inv_cost', DATA_INV_COST)
    scen.commit('')
    scen.solve()

    reporter, key = prepare_reporter(scen, config=config, key=qty)
    df = reporter.get('Investment Cost:iamc').as_pandas()
    assert set(df['variable']) == {'Investment Cost|Coal'}
