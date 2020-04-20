from message_data.model.transport.build import main as build
# from message_data.reporting.core import prepare_reporter


def test_build_bare_res(bare_res):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    build(bare_res)


def test_solve_bare_res(bare_res):
    """Test that MESSAGE-Transport built on the bare RES will solve."""
    build(bare_res, fast=True)

    # commented: for debugging
    # bare_res.to_excel('debug.xlsx')

    bare_res.solve()

    # Report the results
    #
    # rep = prepare_reporter(bare_res)
    #
    # rep.set_filters(t=[
    #     'transport freight load factor',
    #     'transport pax load factor',
    # ])
    #
    # print(rep.get('ACT'))
