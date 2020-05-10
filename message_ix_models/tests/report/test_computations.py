from functools import partial
import logging

from ixmp.reporting import as_quantity
from ixmp.testing import assert_logs
from message_ix.reporting import Reporter
from message_data.reporting.computations import update_scenario
from message_data.tools import ScenarioInfo, make_df


def test_update_scenario(bare_res, caplog):
    scen = bare_res
    # Number of rows in the 'demand' parameter
    N_before = len(scen.par('demand'))

    # A Reporter used as calculation engine
    calc = Reporter()

    # Target Scenario for updating data
    calc.add('target', scen)

    # Create a pd.DataFrame suitable for Scenario.add_par()
    units = 'GWa'
    demand = make_df(
        'demand',
        node='World',
        commodity='electr',
        level='secondary',
        year=ScenarioInfo(bare_res).Y[:10],
        time='year',
        value=1.0,
        unit=units)

    # Add to the Reporter
    calc.add('input', demand)

    # Task to update the scenario with the data
    calc.add('test 1', (partial(update_scenario, params=['demand']), 'target',
                        'input'))

    # Trigger the computation that results in data being added
    with assert_logs(caplog, "'demand' ← 10 rows", at_level=logging.DEBUG):
        # Returns nothing
        assert calc.get('test 1') is None

    # Rows were added to the parameter
    assert len(scen.par('demand')) == N_before + len(demand)

    # Modify the data
    demand['value'] = 2.0
    demand = demand.iloc[:5]
    # Convert to a Quantity object
    input = as_quantity(
        demand.set_index('node commodity level year time'.split())['value'],
        name='demand',
        units=units,
    )
    # Re-add
    calc.add('input', input)

    # Revise the task; the parameter name ('demand')
    calc.add('test 2', (update_scenario, 'target', 'input'))

    # Trigger the computation
    with assert_logs(caplog, "'demand' ← 5 rows"):
        calc.get('test 2')

    # Only half the rows have been updated
    assert scen.par('demand')['value'].mean() == 1.5
