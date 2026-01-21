import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11
        {"regions": "R11", "type_reg": "global", "RCP": "2p6", "REL": "med"},
        # Global R12
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "med"},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "med"},
    ],
    indirect=True,
)
def test_map_basin_region_wat(water_context):
    """Test map_basin_region_wat with global and country model configurations."""
    # ScenarioInfo needed for year sets
    sets = {"year": [2020, 2030, 2040]}
    water_context["water build info"] = ScenarioInfo(y0=2020, set=sets)

    result = map_basin_region_wat(water_context)

    assert isinstance(result, pd.DataFrame)
    assert all(
        col in result.columns
        for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
    )


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11
        {"regions": "R11", "type_reg": "global", "RCP": "2p6", "REL": "med"},
        # Global R12
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "med"},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "med"},
    ],
    indirect=True,
)
def test_add_water_supply(water_context, water_scenario, assert_message_params):
    """Test add_water_supply with global and country model configurations."""
    result = add_water_supply(water_context)

    assert_message_params(
        result,
        expected_keys=["input", "output", "var_cost", "technical_lifetime", "inv_cost"],
    )

    for df in result.values():
        assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11
        {"regions": "R11", "type_reg": "global", "RCP": "2p6", "REL": "med", "SDG": True},
        # Global R12
        {"regions": "R12", "type_reg": "global", "RCP": "2p6", "REL": "med", "SDG": True},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "med", "SDG": True},
    ],
    indirect=True,
)
def test_add_e_flow(water_context):
    """Test add_e_flow with global and country model configurations."""
    # ScenarioInfo needed for year sets
    sets = {"year": [2020, 2030, 2040]}
    water_context["water build info"] = ScenarioInfo(y0=2020, set=sets)

    result = add_e_flow(water_context)

    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)
