from unittest.mock import patch

import pandas as pd

from message_ix_models.model.water.data.water_supply import (
    add_e_flow, add_water_supply, map_basin_region_wat)


def test_map_basin_region_wat():
    # Mock the context
    context = {
        "water build info": {"Y": [2020, 2030, 2040]},
        "type_reg": "country",
        "regions": "test_region",
        "map_ISO_c": {"test_region": "test_ISO"},
        "RCP": "test_RCP",
        "REL": "test_REL",
        "time": "year",
    }

    # Mock the DataFrames read from CSV
    df_x = pd.DataFrame(
        {
            "BCU_name": ["test_BCU"],
        }
    )

    df_sw = pd.DataFrame(
        {
            "Unnamed: 0": [0],
            "BCU_name": ["test_BCU"],
            "test_value": [1],
        }
    )

    # Mock the function 'private_data_path' to return the mocked DataFrame
    with patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch("pandas.read_csv", return_value=df_sw):
        # Call the function to be tested
        result = map_basin_region_wat(context)

        # Assert the results
        assert isinstance(result, pd.DataFrame)
        assert all(
            col in result.columns
            for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
        )


def test_add_water_supply():
    # Mock the context
    context = {
        "water build info": {"Y": [2020, 2030, 2040]},
        "type_reg": "country",
        "regions": "test_region",
        "map_ISO_c": {"test_region": "test_ISO"},
        "RCP": "test_RCP",
        "REL": "test_REL",
        "time": "year",
        "nexus_set": "nexus",
        "get_scenario": lambda: {"firstmodelyear": 2020},
    }

    # Mock the DataFrames read from CSV
    df_node = pd.DataFrame({"BCU_name": ["test_BCU"], "REGION": ["test_REGION"]})

    df_gwt = pd.DataFrame(
        {
            "REGION": ["test_REGION"],
            "GW_per_km3_per_year": [1],
        }
    )

    df_hist = pd.DataFrame(
        {
            "BCU_name": ["test_BCU"],
            "hist_cap_sw_km3_year": [1],
            "hist_cap_gw_km3_year": [1],
        }
    )

    df_sw = pd.DataFrame(
        {
            "mode": ["test_mode"],
            "MSGREG": ["test_MSGREG"],
            "share": [1],
            "year": [2020],
            "time": ["year"],
        }
    )

    # Mock the function 'private_data_path' to return the mocked DataFrame
    with patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch("pandas.read_csv", return_value=df_node), patch(
        "your_module.map_basin_region_wat", return_value=df_sw
    ):
        # Call the function to be tested
        result = add_water_supply(context)

        # Assert the results
        assert isinstance(result, dict)
        assert "input" in result
        assert "output" in result
        assert "var_cost" in result
        assert "technical_lifetime" in result
        assert "inv_cost" in result

        for df in result.values():
            assert isinstance(df, pd.DataFrame)


def test_add_e_flow():
    # Mock the context
    context = {
        "water build info": {"Y": [2020, 2030, 2040]},
        "regions": "test_region",
        "RCP": "test_RCP",
        "time": "year",
        "SDG": True,
    }

    # Mock the DataFrames read from CSV
    df_sw = pd.DataFrame(
        {"Region": ["test_Region"], "value": [1], "year": [2020], "time": ["year"]}
    )

    df_env = pd.DataFrame(
        {"Region": ["test_Region"], "value": [1], "year": [2020], "time": ["year"]}
    )

    # Mock the function 'read_water_availability' to return the mocked DataFrame
    with patch(
        "your_module.read_water_availability", return_value=(df_sw, df_sw)
    ), patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch(
        "pandas.read_csv", return_value=df_sw
    ):
        # Call the function to be tested
        result = add_e_flow(context)

        # Assert the results
        assert isinstance(result, dict)
        assert "bound_activity_lo" in result
        assert isinstance(result["bound_activity_lo"], pd.DataFrame)
