import pytest

from message_data.model.transport.utils import read_config
from message_data.model.transport.build import main as build
from message_data.model.transport.check import check

from message_data.tests import binary_data_available


@pytest.mark.parametrize('nonldv_data_source', [
    None,
    pytest.param('IKARUS', marks=binary_data_available),
])
def test_build_bare_res(bare_res, nonldv_data_source):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    # Pre-load transport config/metadata
    context = read_config()

    # Manually modify some of the configuration per test parameters
    context['transport config']['data source']['non-LDV'] = nonldv_data_source

    # Build succeeds without error
    build(bare_res, fast=True)


def test_solve_bare_res(bare_res):
    """Test that MESSAGE-Transport built on the bare RES will solve."""
    build(bare_res, fast=True)

    # commented: for debugging
    # bare_res.to_excel('debug.xlsx')

    bare_res.solve()

    # Use Reporting calculations to check the result
    result = check(bare_res)
    assert result.all(), f'\n{result}'
