import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)


@pytest.mark.parametrize(
    "water_context",
    [{"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "med"}],
    indirect=True,
)
def test_map_basin_region_wat(water_context):
    """Test map_basin_region_wat with country model configuration."""
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
    [{"regions": "ZMB", "type_reg": "country", "RCP": "2p6", "REL": "med"}],
    indirect=True,
)
def test_add_water_supply(
    water_context, water_scenario, assert_message_params
):
    """Test add_water_supply with country model configuration."""
    result = add_water_supply(water_context)

    assert_message_params(
        result, expected_keys=["input", "output", "var_cost", "technical_lifetime", "inv_cost"]
    )

    for df in result.values():
        assert isinstance(df, pd.DataFrame)


def test_add_e_flow(test_context):
    """Test add_e_flow with global model (R12) configuration."""
    # Manual setup for R12 global model with SDG
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.time = "year"
    test_context.SDG = True

    result = add_e_flow(test_context)

    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)
