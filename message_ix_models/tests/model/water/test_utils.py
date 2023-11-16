from unittest.mock import patch

import pandas as pd
import xarray as xr

from message_ix_models.model.water.build import (
    Context,
    add_commodity_and_level,
    map_add_on,
    map_yv_ya_lt,
    read_config,
)


def test_read_config():
    # Mock the context
    context = Context(0)

    # Mock the data returned by load_private_data
    mock_data = {"test_key": "test_value"}

    # Mock the load_private_data function to return mock_data
    with patch("message_ix_models.util.load_private_data", return_value=mock_data):
        # Call the function to be tested
        result = read_config(context)

    # Assert the results
    assert isinstance(result, Context)
    assert result["water config"] == mock_data
    assert result["water set"] == mock_data
    assert result["water technology"] == mock_data


def test_map_add_on():
    # Mock the context
    Context(0)

    # Mock the data returned by read_config
    mock_data = {
        "water set": {
            "add_on": {"add": [Code(id="1", name="test_1")]},
            "type_addon": {"add": [Code(id="2", name="test_2")]},
        }
    }

    # Mock the read_config function to return mock_data
    with patch("your_module.read_config", return_value=mock_data):
        # Call the function to be tested
        result = map_add_on()

    # Assert the results
    expected = [Code(id="12", name="test_1, test_2")]
    assert result == expected

    # Testing with rtype = 'indexers'
    with patch("your_module.read_config", return_value=mock_data):
        result = map_add_on(rtype="indexers")

    expected = {
        "add_on": xr.DataArray(["1"], dims="consumer_group"),
        "type_addon": xr.DataArray(["2"], dims="consumer_group"),
        "consumer_group": xr.DataArray(["12"], dims="consumer_group"),
    }
    for key in expected:
        assert (result[key] == expected[key]).all().item()


def test_add_commodity_and_level():
    # Mock the dataframe
    df = pd.DataFrame({"technology": ["tech1", "tech2"]})

    # Mock the data returned by Context.get_instance and get_codes
    mock_context_data = {
        "water set": {
            "technology": {
                "add": pd.Series(
                    data=[
                        Code(
                            id="tech1",
                            anno={"input": {"commodity": "com1", "level": "lev1"}},
                        ),
                        Code(id="tech2", anno={"input": {"commodity": "com2"}}),
                    ],
                    name="tech",
                )
            }
        }
    }
    mock_codes_data = pd.Series(
        data=[
            Code(id="com1", anno={"level": "lev1"}),
            Code(id="com2", anno={"level": "lev2"}),
        ],
        name="com",
    )

    # Mock the Context.get_instance and get_codes functions to return mock_data
    with patch(
        "your_module.Context.get_instance", return_value=mock_context_data
    ), patch("your_module.get_codes", return_value=mock_codes_data):
        # Call the function to be tested
        result = add_commodity_and_level(df)

    # Assert the results
    expected = pd.DataFrame(
        {
            "technology": ["tech1", "tech2"],
            "commodity": ["com1", "com2"],
            "level": ["lev1", "lev2"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_map_yv_ya_lt():
    periods = (2010, 2020, 2030, 2040)
    lt = 20
    ya = 2020

    expected = pd.DataFrame(
        {"year_vtg": [2010, 2020, 2020, 2030], "year_act": [2020, 2020, 2030, 2040]}
    )

    result = map_yv_ya_lt(periods, lt, ya)

    pd.testing.assert_frame_equal(result, expected)

    # test with no active year specified
    expected_no_ya = pd.DataFrame(
        {
            "year_vtg": [2010, 2020, 2020, 2030, 2030, 2040],
            "year_act": [2020, 2020, 2030, 2030, 2040, 2040],
        }
    )

    result_no_ya = map_yv_ya_lt(periods, lt)

    pd.testing.assert_frame_equal(result_no_ya, expected_no_ya)
