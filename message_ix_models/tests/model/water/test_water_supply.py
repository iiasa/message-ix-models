from unittest.mock import patch

import pandas as pd
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)


def test_map_basin_region_wat(test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    context = test_context
    context["water build info"] = {"Y": [2020, 2030, 2040]}
    context.type_reg = "country"
    context.regions = "test_region"
    context.map_ISO_c = {"test_region": "test_ISO"}
    context.RCP = "test_RCP"
    context.REL = "test_REL"
    context.time = "year"

    # Mock the DataFrames read from CSV
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
        context["time"] = "year"
        # FIXME This is not working with context.type_reg == "country". Have you ever
        # confirmed that the code works in this case? If not, maybe this test is not
        # needed.
        result = map_basin_region_wat(context)

        # Assert the results
        assert isinstance(result, pd.DataFrame)
        assert all(
            col in result.columns
            for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
        )


def test_add_water_supply(test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    context = test_context
    context["water build info"] = {"Y": [2020, 2030, 2040]}
    context.type_reg = "country"
    context.regions = "test_region"
    context.map_ISO_c = {"test_region": "test_ISO"}
    context.RCP = "test_RCP"
    context.REL = "test_REL"
    context.time = "year"
    context.nexus_set = "nexus"

    mp = context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": "test water model",
        "scenario": "test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("node", ["loc1", "loc2"])
    s.add_set("year", [2020, 2030, 2040])

    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    context.set_scenario(s)
    context["water build info"] = ScenarioInfo(s)

    # Mock the DataFrames read from CSV
    df_node = pd.DataFrame({"BCU_name": ["test_BCU"], "REGION": ["test_REGION"]})

    pd.DataFrame(
        {
            "REGION": ["test_REGION"],
            "GW_per_km3_per_year": [1],
        }
    )

    pd.DataFrame(
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
        "message_ix_models.model.water.data.water_supply.map_basin_region_wat",
        return_value=df_sw,  # Adjust this import
    ), patch("message_ix_models.util.context.Context.get_scenario", return_value=s):
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


def test_add_e_flow(test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    context = test_context
    context["water build info"] = {"Y": [2020, 2030, 2040]}
    context.regions = "test_region"
    context.RCP = "test_RCP"
    context.REL = "test_REL"
    context.time = "year"
    context.SDG = True

    # Mock the DataFrames read from CSV
    df_sw = pd.DataFrame(
        {
            "Region": ["test_Region"],
            "value": [1],
            "year": [2020],
            "time": ["year"],
            "Unnamed: 0": [0],
            "BCU_name": ["test_BCU"],
        }
    )

    # Mock the function 'read_water_availability' to return the mocked DataFrame
    with patch(
        "message_ix_models.model.water.data.demands.read_water_availability",
        return_value=(df_sw, df_sw),
    ), patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch("pandas.read_csv", return_value=df_sw):
        # FIXME This doesn't work because read_water_availability() in line 749 of
        # water/data/demands expects the second column of df_sw to be "years", but it
        # contains the names of the columns at that point starting with df_sw here, not
        # something that pandas can convert to DateTimes!
        # Call the function to be tested
        result = add_e_flow(context)

        # Assert the results
        assert isinstance(result, dict)
        assert "bound_activity_lo" in result
        assert isinstance(result["bound_activity_lo"], pd.DataFrame)
