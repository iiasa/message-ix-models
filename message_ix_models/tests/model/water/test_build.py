import pytest

from message_ix_models import ScenarioInfo, testing
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.build import get_spec, map_basin
from message_ix_models.model.water.build import main as build


def test_build(request, test_context):
    # This is needed below and for the RES to contain the correct number of regions
    test_context.regions = "R11"
    scenario = testing.bare_res(request, test_context)

    # TODO If all water functions require these keys, set this up in a central location
    # or via default value
    # Ensure test_context has all necessary keys for build()
    test_context.nexus_set = "nexus"
    test_context.type_reg = "global"
    test_context.time = "year"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "6p0"
    test_context.REL = "med"
    test_context["water build info"] = ScenarioInfo(scenario_obj=scenario)

    # Code runs on the bare RES
    build(context=test_context, scenario=scenario)

    # New set elements were added
    assert "extract_surfacewater" in scenario.set("technology").tolist()


@pytest.mark.parametrize("nexus_set", ["nexus", "cooling"])
def test_get_spec(test_context, nexus_set):
    # Ensure test_context has all necessary keys for get_spec()
    test_context.nexus_set = nexus_set
    test_context.regions = "R12"
    test_context.type_reg = "global"

    if nexus_set == "nexus":
        # Need this to prepare for running get_spec() with nexus_set == "nexus"
        _ = map_basin(context=test_context)

    # Code runs
    spec = get_spec(context=test_context)

    # Expected return type
    assert isinstance(spec, dict) and len(spec) == 3

    # Contents are read correctly
    assert "water_supply" in spec["remove"].set["level"]
    assert "water_supply" in spec["add"].set["level"]
