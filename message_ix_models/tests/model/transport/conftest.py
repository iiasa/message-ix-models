import pytest

from message_data.model.transport.utils import read_config
from message_data.model.transport.build import main as build


@pytest.fixture(scope='session')
def solved_bare_res_transport(bare_res):
    # Pre-load transport config/metadata
    context = read_config()

    context['transport config']['data source']['LDV'] = 'US-TIMES MA3T'
    context['transport config']['data source']['non-LDV'] = 'IKARUS'

    scen = bare_res.clone()
    build(scen, fast=True, quiet=False)
    scen.solve(solve_options=dict(lpmethod=4))

    yield scen
