import os

import pytest

from message_data.model.bare import create_res
from message_data.model.transport.data import (
    get_consumer_groups,
    get_ikarus_data,
    get_ldv_data,
)
from message_data.model.transport.data.groups import get_ma3t_data


pytestmark = pytest.mark.skipif(
    'TEAMCITY_VERSION' in os.environ,
    reason='Cannot access data on TeamCity server.')


def test_ikarus():
    data = get_ikarus_data(None)

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
    data = get_ldv_data(scenario)

    # 3 parameters × 11 regions × 13 periods × 12 technologies
    assert len(data) == 11 * 3 * 13 * 12
    assert set(data.columns) == {'technology', 'year', 'value', 'node', 'name'}


def test_groups(test_context):
    test_context.regions = 'R11'

    result = get_consumer_groups(test_context)

    # Currently only urban share;
    # 3 scenarios × 11 regions × 13 periods × {urban, rural}
    assert len(result) == 11 * 3 * 11 * 2

    result = get_ma3t_data(test_context)
    assert len(result) == 3
