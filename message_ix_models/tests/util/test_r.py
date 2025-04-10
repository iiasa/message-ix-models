from message_ix_models.util import get_r_func


def test_get_func():
    """R code can be sourced and called."""
    get_df = get_r_func("tests.r.module:get_df")
    get_df()

    add = get_r_func("tests.r.module:add")
    add(1.2, 3.4)

    mul = get_r_func("tests.r.module:mul")
    mul(1.2, 3.4)
