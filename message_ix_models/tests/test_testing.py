from message_ix_models.testing import bare_res


def test_bare_res_solved(request, test_context):
    """:func:`bare_res` works with `solve` = :obj:`True`.

    This test can be removed once this feature of the test function is used by another
    test.
    """
    bare_res(request, test_context, solved=True)
