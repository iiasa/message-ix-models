from message_data.model.transport.build import main as build
from message_data.model.transport.check import check


def test_build_bare_res(bare_res):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    build(bare_res)


def test_solve_bare_res(bare_res):
    """Test that MESSAGE-Transport built on the bare RES will solve."""
    build(bare_res, fast=True)

    # commented: for debugging
    # bare_res.to_excel('debug.xlsx')

    bare_res.solve()

    # Use Reporting calculations to check the result
    result = check(bare_res)
    assert result.all(), f'\n{result}'
