import pytest

from message_data.model.transport.report import check
from message_data.model.transport.utils import read_config
from message_data.model.transport.build import main as build

from message_data.tests import binary_data_available


@pytest.mark.parametrize('ldv, nonldv', [
    (None, None),
    pytest.param('US-TIMES MA3T', 'IKARUS', marks=binary_data_available),
])
def test_build_bare_res(bare_res, ldv, nonldv):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    # Pre-load transport config/metadata
    context = read_config()

    # Manually modify some of the configuration per test parameters
    context['transport config']['data source']['LDV'] = ldv
    context['transport config']['data source']['non-LDV'] = nonldv

    # Build succeeds without error
    build(bare_res, fast=True)


def test_solve_bare_res(solved_bare_res_transport):
    """Test that MESSAGE-Transport built on the bare RES will solve."""
    scen = solved_bare_res_transport

    # Use Reporting calculations to check the result
    result = check(scen)
    assert result.all(), f'\n{result}'
