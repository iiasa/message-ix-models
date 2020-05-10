from copy import deepcopy

import pytest

from message_data.model.bare import get_spec
from message_data.model.transport.utils import read_config
from message_data.model.transport.build import main as build


@pytest.fixture(scope='session')
def solved_bare_res_transport(_bare_res):
    # Pre-load transport config/metadata
    context = read_config()

    context['transport config']['data source']['LDV'] = 'US-TIMES MA3T'
    context['transport config']['data source']['non-LDV'] = 'IKARUS'

    scen = _bare_res.clone()
    build(scen, fast=True, quiet=False)
    scen.solve(solve_options=dict(lpmethod=4))

    yield scen


@pytest.fixture(scope="session")
def res_info(session_context):
    ctx = deepcopy(session_context)
    ctx.regions = 'R11'

    yield get_spec(ctx)["add"]
