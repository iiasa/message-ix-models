import pandas as pd
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)


def test_map_basin_region_wat(test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.time = "year"

    result = map_basin_region_wat(test_context)

    # Assert the results
    assert isinstance(result, pd.DataFrame)
    assert all(
        col in result.columns
        for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
    )


def test_add_water_supply(request, test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.time = "year"
    test_context.nexus_set = "nexus"

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
    s.add_set("node", ["loc1", "loc2"])
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="Commit test scenario")

    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    result = add_water_supply(test_context)

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
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.time = "year"
    test_context.SDG = True

    # Call the function to be tested
    result = add_e_flow(test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)
