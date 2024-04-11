from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.irrigation import add_irr_structure


def test_add_irr_structure(test_context):
    context = test_context

    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    context.type_reg = "country"
    context.regions = "ZMB"
    nodes = get_codes(f"node/{context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    context.map_ISO_c = {context.regions: nodes[0]}

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

    # FIXME same as above
    context["water build info"] = ScenarioInfo(s)

    # Call the function to be tested
    result = add_irr_structure(context)

    # Assert the results
    assert isinstance(result, dict)
    assert "input" in result
    assert "output" in result
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
    assert all(
        col in result["output"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_dest",
            "node_loc",
            "node_dest",
            "year_vtg",
            "year_act",
        ]
    )
