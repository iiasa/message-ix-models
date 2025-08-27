import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.water.data.water_for_ppl import (
    cool_tech,
    non_cooling_tec,
)
from message_ix_models.tests.model.water.conftest import setup_valid_basins


def _get_test_node(regions: str) -> str:
    """Return a valid test node for the given region configuration."""
    if regions == "R11":
        return "R11_CPA"
    elif regions == "R12":
        return "R12_CPA"
    else:
        # Country model - node is the country code itself
        return regions


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11 (has 6p0)
        {
            "regions": "R11",
            "type_reg": "global",
            "RCP": "no_climate",
            "REL": "med",
            "ssp": "SSP2",
        },
        {
            "regions": "R11",
            "type_reg": "global",
            "RCP": "6p0",
            "REL": "med",
            "ssp": "SSP2",
        },
        # Global R12 (no 6p0, use 7p0)
        {
            "regions": "R12",
            "type_reg": "global",
            "RCP": "no_climate",
            "REL": "med",
            "ssp": "SSP2",
        },
        {
            "regions": "R12",
            "type_reg": "global",
            "RCP": "7p0",
            "REL": "med",
            "ssp": "SSP2",
        },
        # ZMB excluded: cost projections only support R11/R12/R20
    ],
    indirect=True,
)
def test_cool_tec(request, water_context, assert_message_params):
    """Test cool_tech with global and country model configurations.

    Requires a scenario with input, historical_activity, and historical_new_capacity
    parameters for cooling technology derivation.
    """
    node = _get_test_node(water_context.regions)

    mp = water_context.get_platform()
    s = Scenario(
        mp=mp,
        model=f"{request.node.name}/test water model",
        scenario=f"{request.node.name}/test water scenario",
        version="new",
    )
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["gad_cc", "coal_ppl"])
    s.add_set("node", [node])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("mode", ["M1", "M2"])
    s.add_set("commodity", ["electricity", "gas"])
    s.add_set("level", ["secondary", "final"])
    s.add_set("time", ["year"])

    # Add input parameter
    df_add = pd.DataFrame(
        {
            "node_loc": [node],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "year_act": [2020],
            "mode": ["M1"],
            "node_origin": [node],
            "commodity": ["electricity"],
            "level": ["secondary"],
            "time": "year",
            "time_origin": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    # Add historical activity
    df_ha = pd.DataFrame(
        {
            "node_loc": [node],
            "technology": ["coal_ppl"],
            "year_act": [2020],
            "mode": ["M1"],
            "time": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    # Add historical new capacity
    df_hnc = pd.DataFrame(
        {
            "node_loc": [node],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "value": [1],
            "unit": "GWa",
        }
    )
    s.add_par("input", df_add)
    s.add_par("historical_activity", df_ha)
    s.add_par("historical_new_capacity", df_hnc)
    s.commit(comment="water cool_tech test scenario")

    water_context.set_scenario(s)
    water_context["water build info"] = ScenarioInfo(scenario_obj=s)

    # Set up valid_basins for water_for_ppl functions
    setup_valid_basins(water_context, regions=water_context.regions)

    result = cool_tech(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input"])

    # Validate input structure
    assert all(
        col in result["input"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_origin",
            "node_origin",
            "node_loc",
            "year_vtg",
            "year_act",
        ]
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
def test_non_cooling_tec(water_context, water_scenario, assert_message_params):
    """Test non_cooling_tec with global and country model configurations."""
    result = non_cooling_tec(context=water_context)

    assert_message_params(result, expected_keys=["input"])

    assert all(
        col in result["input"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_origin",
            "node_origin",
            "node_loc",
            "year_vtg",
            "year_act",
        ]
    )
