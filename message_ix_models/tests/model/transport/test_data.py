import os

import pandas as pd
import pytest
import xarray as xr

from message_data.model.bare import create_res
from message_data.model.transport.data import (
    FILES,
    get_consumer_groups,
    get_ikarus_data,
    get_ldv_data,
)
from message_data.model.transport.data.groups import get_urban_rural_shares
from message_data.tools import load_data


pytestmark = pytest.mark.skipif(
    'TEAMCITY_VERSION' in os.environ,
    reason='Cannot access data on TeamCity server.')


@pytest.mark.parametrize('key', FILES)
@pytest.mark.parametrize('rtype', (pd.Series, xr.DataArray))
def test_load_data(test_context, key, rtype):
    result = load_data(test_context, 'transport', key, rtype=rtype)
    assert isinstance(result, rtype)


def test_ikarus(test_context):
    data = get_ikarus_data(test_context, None)

    # Data have been loaded with the correct units
    assert data.loc[2020, ('rail_pub', 'inv_cost')].dimensionality \
        == {'[currency]': 1, '[vehicle]': -1}


def test_ldv(test_context):
    test_context.scenario_info.update(dict(
        model='Bare RES',
        scenario='test_create_res',
    ))

    test_context.regions = 'R11'
    scenario = create_res(test_context)
    data = get_ldv_data(test_context, scenario)

    # 3 parameters × 11 regions × 13 periods × 12 technologies
    assert len(data) == 11 * 3 * 13 * 12
    assert set(data.columns) == {'technology', 'year', 'value', 'node', 'name'}


@pytest.mark.parametrize('regions', ['R11'])
@pytest.mark.parametrize('pop_scen', ['GEA mix'])
def test_groups(test_context, regions, pop_scen):
    test_context.regions = regions
    test_context['transport population scenario'] = pop_scen

    result = get_consumer_groups(test_context)

    # Currently only urban share;
    # 3 scenarios × 11 regions × 13 periods × {urban, rural}
    assert len(result) == 11 * 3 * 11 * 2


@pytest.mark.parametrize('regions', ['R11'])
@pytest.mark.parametrize('pop_scen', ['GEA mix', 'GEA supply', 'GEA eff'])
def test_urban_rural_shares(test_context, regions, pop_scen):
    test_context.regions = 'R11'
    test_context['transport population scenario'] = pop_scen

    # Shares can be successfully retrieved
    get_urban_rural_shares(test_context)
