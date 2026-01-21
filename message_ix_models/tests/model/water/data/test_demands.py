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
        # Global R11
        {"regions": "R11", "type_reg": "global", "SDG": "baseline", "time": "year"},
        # Global R12
        {"regions": "R12", "type_reg": "global", "SDG": "baseline", "time": "year"},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country", "SDG": "baseline", "time": "year"},
        # SDG="SDG" excluded: requires policy data files
    ],
    indirect=True,
)
def test_add_sectoral_demands(water_context, water_scenario, assert_message_params):
    """Test add_sectoral_demands with global and country model configurations."""
    result = add_sectoral_demands(context=water_context)

    assert_message_params(result)

    # Check expected keys
    assert all(
        key
        in ("demand", "historical_new_capacity", "historical_activity", "share_commodity_lo")
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
        # Global R11 (no monthly data)
        {"regions": "R11", "type_reg": "global", "RCP": "2p6", "REL": "low", "time": "year"},
        # Global R12 (monthly exists for REL=low)
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "low", "time": "year"},
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "low", "time": "month"},
        # Country ZMB (monthly exists for REL=low)
        {"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "low", "time": "year"},
        {"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "low", "time": "month"},
    ],
    indirect=True,
)
def test_add_water_availability(water_context, assert_message_params):
    """Test add_water_availability with global and country model configurations."""
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


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11
        {"regions": "R11", "type_reg": "global"},
        # Global R12
        {"regions": "R12", "type_reg": "global"},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country"},
    ],
    indirect=True,
)
def test_add_irrigation_demand(water_context, water_scenario, assert_message_params):
    """Test add_irrigation_demand with global and country model configurations."""
    result = add_irrigation_demand(context=water_context)

    assert_message_params(result, expected_keys=["land_input"])

    assert all(
        col in result["land_input"].columns
        for col in ["value", "unit", "level", "commodity", "node", "time", "year"]
    )
