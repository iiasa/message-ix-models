import pandas as pd
import pytest

from message_ix_models.model.water.utils import (
    get_vintage_and_active_years,
    map_yv_ya_lt,
    read_config,
)


@pytest.fixture
def mock_scenario_info():
    """Mock ScenarioInfo with yv_ya property and year set."""

    class MockScenarioInfo:
        def __init__(self):
            self.yv_ya = pd.DataFrame(
                {
                    "year_vtg": [2010, 2010, 2010, 2020, 2020, 2030],
                    "year_act": [2010, 2020, 2030, 2020, 2030, 2030],
                }
            )
            self.set = {"year": [2010, 2020, 2030]}

    return MockScenarioInfo()


def test_read_config(test_context):
    # read_config() returns a reference to the current context
    context = read_config()
    assert context is test_context

    # config::'data files' have been read-in correctly
    assert context["water config"]["data files"] == [
        "cooltech_cost_and_shares_ssp_msg14",
        "tech_water_performance_ssp_msg",
    ]


@pytest.mark.parametrize(
    "technical_lifetime,expected_data",
    [
        (
            10,
            {
                "year_vtg": [2010, 2010, 2020, 2020, 2030],
                "year_act": [2010, 2020, 2020, 2030, 2030],
            },
        ),
        (
            20,
            {
                "year_vtg": [2010, 2010, 2010, 2020, 2020, 2030],
                "year_act": [2010, 2020, 2030, 2020, 2030, 2030],
            },
        ),
        (
            None,
            {
                "year_vtg": [2010, 2010, 2010, 2020, 2020, 2030],
                "year_act": [2010, 2020, 2030, 2020, 2030, 2030],
            },
        ),
    ],
)
def test_get_vintage_and_active_years(
    mock_scenario_info, technical_lifetime, expected_data
):
    """Test get_vintage_and_active_years function with different technical lifetimes."""
    result = get_vintage_and_active_years(mock_scenario_info, technical_lifetime)
    expected = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected)


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
