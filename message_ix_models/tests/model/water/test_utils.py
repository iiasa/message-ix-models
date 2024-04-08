import pandas as pd
from sdmx.model.common import Code

from message_ix_models.model.water.utils import (
    map_yv_ya_lt,
    read_config,
)


def test_read_config(session_context):
    # read_config() returns a reference to the current context
    context = read_config()
    assert context is session_context

    # 'add' elements have been converted to Code objects
    assert isinstance(context["water"]["set"]["technology"]["add"][0], Code)


def test_map_yv_ya_lt():
    periods = (2010, 2020, 2030, 2040)
    lt = 20
    ya = 2020

    expected = pd.DataFrame(
        {
            "year_vtg": [2010, 2010, 2020, 2020, 2020, 2030, 2030, 2040],
            "year_act": [2020, 2030, 2020, 2030, 2040, 2030, 2040, 2040],
        }
    )

    result = map_yv_ya_lt(periods, lt, ya).reset_index(drop=True)
    # print(result)

    pd.testing.assert_frame_equal(result, expected)

    expected_no_ya = pd.DataFrame(
        {
            "year_vtg": [2010, 2010, 2010, 2020, 2020, 2020, 2030, 2030, 2040],
            "year_act": [2010, 2020, 2030, 2020, 2030, 2040, 2030, 2040, 2040],
        }
    )

    result_no_ya = map_yv_ya_lt(periods, lt).reset_index(drop=True)

    pd.testing.assert_frame_equal(result_no_ya, expected_no_ya)
