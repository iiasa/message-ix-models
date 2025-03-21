import pandas as pd
import pandas.testing as pdt
from ixmp import Platform
from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data import water_supply_pt2_theseus1, water_supply_pt2
from message_ix import Scenario

def test_add_water_supply_run(request, test_context):
    # setup test_context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions.upper(): nodes[0]}
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
    s.commit(comment="commit test scenario")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    # run legacy and refactored implementations
    ws_legacy = water_supply_pt2.add_water_supply(test_context)
    ws_refact = water_supply_pt2_theseus1.add_water_supply(test_context)

    # compare results
    assert set(ws_legacy.keys()) == set(ws_refact.keys()), "parameter keys mismatch"
    for key in ws_legacy:
        try:
            pdt.assert_frame_equal(
                ws_legacy[key].sort_index(axis=1),
                ws_refact[key].sort_index(axis=1)
            )
        except AssertionError as e:
            raise AssertionError(f"diff for {key}: {e}")