import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.demands import (
    add_irrigation_demand,
    add_sectoral_demands,
    add_water_availability,
)


@pytest.mark.parametrize(
    ["SDG", "time"], [("baseline", "year"), ("ambitious", "month")]
)
def test_add_sectoral_demands(request, test_context, SDG, time):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # FIXME
    # This doesn't work with ZMB because delineation/basins_country_ZMB.csv doesn't
    # contain "IND" for any STATUS field, but this is expected in
    # demands/get_basin_sizes(), which is required output to check which
    # set_target_rate_develop*() should be called
    # This doesn't work with R11 or R12 because
    # demands/harmonized/R*/ssp2_m_water_demands.csv doesn't exist
    test_context.SDG = SDG
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}
    test_context.time = time

    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("year", [2020, 2030, 2040])

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Call the function to be tested
    result = add_sectoral_demands(context=test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert all(
        key
        in (
            "demand",
            "historical_new_capacity",
            "historical_activity",
            "share_commodity_lo",
        )
        for key in result.keys()
    )
    assert all(
        col in result["historical_new_capacity"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "node_loc",
            "year_vtg",
        ]
    )
    assert all(
        col in result["demand"].columns
        for col in ["value", "unit", "level", "commodity", "node", "time", "year"]
    )


@pytest.mark.parametrize("time", ["year", "month"])
def test_add_water_availability(test_context, time):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "gloabl"
    test_context.regions = "R12"
    test_context.RCP = "2p6"
    test_context.REL = "low"
    test_context.time = time

    # Run the function to be tested
    result = add_water_availability(context=test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "demand" in result
    assert "share_commodity_lo" in result
    assert all(
        col in result["demand"].columns
        for col in [
            "value",
            "unit",
            "level",
            "commodity",
            "node",
            "time",
            "year",
        ]
    )
    assert all(
        col in result["share_commodity_lo"].columns
        for col in [
            "shares",
            "value",
            "unit",
            "time",
            "node_share",
            "year_act",
        ]
    )


def test_add_irrigation_demand(request, test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("year", [2020, 2030, 2040])

    s.commit(comment="basic water add_irrigation_demand test model")

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Run the function to be tested
    result = add_irrigation_demand(context=test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "land_input" in result
    assert all(
        col in result["land_input"].columns
        for col in [
            "value",
            "unit",
            "level",
            "commodity",
            "node",
            "time",
            "year",
        ]
    )
