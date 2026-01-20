import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.water.data.demands import (
    add_irrigation_demand,
    add_sectoral_demands,
    add_water_availability,
)


@pytest.mark.parametrize(
    "water_context",
    [
        {"regions": "ZMB", "type_reg": "country", "SDG": "baseline", "time": "year"},
        {"regions": "ZMB", "type_reg": "country", "SDG": "ambitious", "time": "month"},
    ],
    indirect=True,
)
def test_add_sectoral_demands(water_context, water_scenario, assert_message_params):
    """Test add_sectoral_demands with country model configuration.

    Note: ZMB doesn't have complete data for all STATUS fields, and R11/R12
    lack harmonized ssp2_m_water_demands.csv files.
    """
    result = add_sectoral_demands(context=water_context)

    assert_message_params(result)

    # Check expected keys
    assert all(
        key in ("demand", "historical_new_capacity", "historical_activity", "share_commodity_lo")
        for key in result.keys()
    )

    # Validate demand columns
    assert all(
        col in result["demand"].columns
        for col in ["value", "unit", "level", "commodity", "node", "time", "year"]
    )

    # Validate historical_new_capacity columns
    assert all(
        col in result["historical_new_capacity"].columns
        for col in ["technology", "value", "unit", "node_loc", "year_vtg"]
    )


@pytest.mark.parametrize(
    "water_context",
    [
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "low", "time": "year"},
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "low", "time": "month"},
    ],
    indirect=True,
)
def test_add_water_availability(water_context, assert_message_params):
    """Test add_water_availability with global model configuration."""
    # ScenarioInfo needed for year sets
    sets = {"year": [2020, 2030, 2040]}
    water_context["water build info"] = ScenarioInfo(y0=2020, set=sets)

    result = add_water_availability(context=water_context)

    assert_message_params(result, expected_keys=["demand", "share_commodity_lo"])

    # Validate demand columns
    assert all(
        col in result["demand"].columns
        for col in ["value", "unit", "level", "commodity", "node", "time", "year"]
    )

    # Validate share_commodity_lo columns
    assert all(
        col in result["share_commodity_lo"].columns
        for col in ["shares", "value", "unit", "time", "node_share", "year_act"]
    )


def test_add_irrigation_demand(water_scenario, test_context, assert_message_params):
    """Test add_irrigation_demand with basic scenario."""
    result = add_irrigation_demand(context=test_context)

    assert_message_params(result, expected_keys=["land_input"])

    assert all(
        col in result["land_input"].columns
        for col in ["value", "unit", "level", "commodity", "node", "time", "year"]
    )
