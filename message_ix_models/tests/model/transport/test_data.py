import pandas as pd
from pandas.testing import assert_series_equal
import pytest
import xarray as xr

from message_data.model.bare import create_res
from message_data.model.transport.data import (
    get_consumer_groups,
    get_ldv_data,
)
from message_data.model.transport.data.groups import get_urban_rural_shares
from message_data.model.transport.data.ikarus import get_ikarus_data
from message_data.model.transport.utils import FILES, read_config
from message_data.tools import ScenarioInfo, load_data, make_df

from message_data.tests import binary_data_available


@pytest.mark.parametrize('key', FILES)
@pytest.mark.parametrize('rtype', (pd.Series, xr.DataArray))
def test_load_data(test_context, key, rtype):
    # Load transport metadata from files in both pandas and xarray formats
    result = load_data(test_context, 'transport', key, rtype=rtype)
    assert isinstance(result, rtype)


@binary_data_available
def test_ikarus(test_context):
    # Create bare RES
    test_context.scenario_info.update(dict(
        model='Bare RES',
        scenario='test_ikarus',
    ))
    test_context.regions = 'R11'
    scenario = create_res(test_context)
    s_info = ScenarioInfo(scenario)

    # get_ikarus_data() succeeds on text_context and the bare RES
    data = get_ikarus_data(s_info)

    # Returns a mapping
    # Retrieve DataFrame for par e.g. 'inv_cost' and tech e.g. 'rail_pub'
    inv = data['inv_cost']
    inv_rail_pub = inv[inv['technology'] == 'rail_pub']

    # 11 regions * 10 years (inv_cost has 'year_vtg' but not 'year_act' dim)
    rows_per_tech = 11 * 10
    N_techs = 18

    # Data have been loaded with the correct shape, unit and magnitude:
    # 1. Shape
    assert inv_rail_pub.shape == (rows_per_tech, 5)
    assert inv.shape == (rows_per_tech * N_techs, 5)

    # 2. Units
    units = inv_rail_pub['unit'].unique()
    assert len(units) == 1, 'Units for each (par, tec) must be unique'

    # Unit is parseable by pint
    pint_unit = test_context.units(units[0])

    # Unit has the correct dimensionality
    assert pint_unit.dimensionality == {'[currency]': 1, '[vehicle]': -1}

    # 3. Magnitude for year e.g. 2020
    values = inv_rail_pub[inv_rail_pub['year_vtg'] == 2020]['value']
    value = values.iloc[0]
    assert round(value, 3) == 3.233

    dims = {
        'technical_lifetime': {'[time]': 1},
        # Output units are in (passenger km) / energy, that's why mass and
        # time dimensions have to be checked.
        'output': {'[passenger]': 1, '[length]': -1, '[mass]': -1,
                   '[time]': 2},
        'capacity_factor': {'[passenger]': 1, '[length]': 1, '[vehicle]': -1,
                            '[time]': -1},
        'fix_cost': {'[currency]': 1, '[vehicle]': -1, '[time]': -1},
    }
    # Check dimensionality of ikarus pars with items in dims:
    for par, dim in dims.items():
        units = data[par]['unit'].unique()
        assert len(units) == 1, 'Units for each (par, tec) must be unique'
        # Unit is parseable by pint
        pint_unit = test_context.units(units[0])
        # Unit has the correct dimensionality
        assert pint_unit.dimensionality == dim

    # Specific magnitudes of other values to check
    checks = [
        dict(par='capacity_factor', year_vtg=2010, value=0.000905),
        dict(par='technical_lifetime', year_vtg=2010, value=14.7),
        dict(par='capacity_factor', year_vtg=2050, value=0.000886),
        dict(par='technical_lifetime', year_vtg=2050, value=14.7),
    ]
    defaults = dict(node_loc=s_info.N[-1], technology='ICG_bus', time='year')

    for check in checks:
        # Create expected data
        par_name = check.pop('par')
        check['year_act'] = check['year_vtg']
        exp = make_df(par_name, **defaults, **check)
        assert len(exp) == 1, 'Single row for expected value'

        # Use merge() to find data with matching column values
        columns = sorted(set(exp.columns) - {'value', 'unit'})
        result = exp.merge(data[par_name], on=columns, how='inner')

        # Single row matches
        assert len(result) == 1, result

        # Values match
        assert_series_equal(result['value_x'], result['value_y'],
                            check_exact=False, check_less_precise=3,
                            check_names=False)


@binary_data_available
def test_ldv(test_context):
    test_context.scenario_info.update(dict(
        model='Bare RES',
        scenario='test_create_res',
    ))
    read_config()

    test_context.regions = 'R11'
    scenario = create_res(test_context)
    info = ScenarioInfo(scenario)

    # Method runs without error
    data = get_ldv_data(info)

    # Data have the correct size: 11 regions × 13 periods × 12 technologies
    for par, df in data.items():
        assert len(df) == len(info.N[1:]) * (len(info.Y) + 3) * 12


@pytest.mark.xfail(reason='Needs normalization across consumer groups.')
@pytest.mark.parametrize('regions', ['R11'])
@pytest.mark.parametrize('pop_scen', ['GEA mix'])
def test_groups(test_context, regions, pop_scen):
    test_context.regions = regions
    test_context['transport population scenario'] = pop_scen

    result = get_consumer_groups(test_context)

    # Data have the correct size
    assert result.sizes == {'node': 11, 'year': 11, 'consumer_group': 27}

    # Data sum to 1 across the consumer_group dimension, i.e. consititute a
    # discrete distribution
    print(result.sum('consumer_group'))
    assert all(result.sum('consumer_group') == 1)


@binary_data_available
@pytest.mark.parametrize('regions', ['R11'])
@pytest.mark.parametrize('pop_scen', ['GEA mix', 'GEA supply', 'GEA eff'])
def test_urban_rural_shares(test_context, regions, pop_scen):
    test_context.regions = 'R11'
    test_context['transport population scenario'] = pop_scen

    # Shares can be retrieved
    get_urban_rural_shares(test_context)