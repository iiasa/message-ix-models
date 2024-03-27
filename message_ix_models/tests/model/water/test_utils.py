from unittest.mock import patch

import pandas as pd
import xarray as xr
from sdmx.model.common import Annotation, Code

from message_ix_models import Context
from message_ix_models.model.water.utils import (
    add_commodity_and_level,
    map_add_on,
    map_yv_ya_lt,
    read_config,
)
from message_ix_models.util import load_private_data


def test_read_config(test_context):
    # Mock the context
    context = test_context

    # Call the function to be tested
    result = read_config(context)

    config_parts = ["water", "config.yaml"]
    set_parts = ["water", "set.yaml"]
    technology_parts = ["water", "technology.yaml"]

    # Assert the results
    assert isinstance(result, Context)
    assert result["water config"] == load_private_data(*config_parts)
    assert result["water set"] == load_private_data(*set_parts)
    assert result["water technology"] == load_private_data(*technology_parts)


def test_map_add_on():
    # Mock the data returned by read_config
    mock_data = {
        "water set": {
            "add_on": {"add": [Code(id="1", name="test_1")]},
            "type_addon": {"add": [Code(id="2", name="test_2")]},
        }
    }

    # Mock the read_config function to return mock_data
    with patch(
        "message_ix_models.model.water.utils.read_config", return_value=mock_data
    ):
        # Call the function to be tested
        result = map_add_on()

    # Assert the results
    expected = [Code(id="12", name="test_1, test_2")]
    assert result == expected

    # Testing with rtype = 'indexers'
    with patch(
        "message_ix_models.model.water.utils.read_config", return_value=mock_data
    ):
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

    # FIXME Something here is seriously broken. Annotations need rework and
    # please clarify what and how the annotations will be accessed and how the
    # resulting data will be used!
    # Mock the data returned by Context.get_instance and get_codes
    mock_context_data = {
        "water set": {
            "technology": {
                "add": [
                    Code(
                        id="tech1",
                        annotations=[
                            Annotation("input", "commodity", "com1", "level", "lev1")
                        ],
                    ),
                    Code(
                        id="tech2",
                        annotations=[Annotation("input", "commodity", "com2")],
                    ),
                ],
            }
        }
    }
    mock_codes_data = [
        Code(id="com1", annotations=[Annotation("level", "lev1")]),
        Code(id="com2", annotations=[Annotation("level", "lev2")]),
    ]

    # Mock the Context.get_instance and get_codes functions to return mock_data
    with patch(
        "message_ix_models.util.context.Context.get_instance",
        return_value=mock_context_data,
    ), patch(
        "message_ix_models.model.structure.get_codes", return_value=mock_codes_data
    ):
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
