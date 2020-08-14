import pytest

from message_data.model.transport import build, report, utils


@pytest.mark.parametrize('ldv, nonldv', [
    (None, None),
    ("US-TIMES MA3T", "IKARUS"),
])
def test_build_bare_res(bare_res, ldv, nonldv):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    # Pre-load transport config/metadata
    context = utils.read_config()

    # Manually modify some of the configuration per test parameters
    context['transport config']['data source']['LDV'] = ldv
    context['transport config']['data source']['non-LDV'] = nonldv

    # Build succeeds without error
    build.main(bare_res, fast=True)


def test_solve_bare_res(solved_bare_res_transport):
    """Test that MESSAGE-Transport built on the bare RES will solve."""
    scen = solved_bare_res_transport

    # Use Reporting calculations to check the result
    result = report.check(scen)
    assert result.all(), f'\n{result}'


def test_get_spec(session_context):
    build.get_spec()
