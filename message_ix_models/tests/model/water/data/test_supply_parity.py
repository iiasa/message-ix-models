import time as pytime

import pandas.testing as pdt
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_supply import (
    add_e_flow as new_add_e_flow,
)
from message_ix_models.model.water.data.water_supply import (
    add_water_supply as new_add_water_supply,
)
from message_ix_models.model.water.data.water_supply import (
    map_basin_region_wat as new_map_basin_region_wat,
)
from message_ix_models.model.water.data.water_supply_legacy import (
    add_e_flow as add_e_flow_legacy,
)
from message_ix_models.model.water.data.water_supply_legacy import (
    add_water_supply as add_water_supply_legacy,
)
from message_ix_models.model.water.data.water_supply_legacy import (
    map_basin_region_wat as map_basin_region_wat_legacy,
)


#@pytest.mark.skip(reason="passed")
@map_basin_region_wat_legacy.minimum_version
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

    n_iter = 10
    start_time = pytime.time()
    for i in range(n_iter):
        result_rf = new_map_basin_region_wat(test_context)
    end_time = pytime.time()
    #print time taken to results.txt
    with open("supply_parity_results.txt", "a") as f:
        f.write(f"Time taken for map_basin_region_wat_new:"
                f" {(end_time - start_time)/n_iter} seconds\n")

    start_time = pytime.time()
    for i in range(n_iter):
        result = map_basin_region_wat_legacy(test_context)
    end_time = pytime.time()
    with open("supply_parity_results.txt", "a") as f:
        f.write(f"Time taken for map_basin_region_wat_legacy:"
                f" {(end_time - start_time)/n_iter} seconds\n")

    #compare results
    pdt.assert_frame_equal(result_rf.sort_index(axis=0).sort_index(axis=1),
                           result.sort_index(axis=0).sort_index(axis=1))


#@pytest.mark.skip(reason="passed")

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
    n_iter = 10
    # run legacy and refactored implementations
    start_time = pytime.time()
    for i in range(n_iter):
        ws_legacy = add_water_supply_legacy(test_context)
    end_time = pytime.time()
    with open("supply_parity_results.txt", "a") as f:
        f.write("Time taken for legacy add_water_supply: "
                f"{(end_time - start_time)/n_iter} seconds\n")

    start_time = pytime.time()
    for i in range(n_iter):
        ws_refact = new_add_water_supply(test_context)
    end_time = pytime.time()
    with open("supply_parity_results.txt", "a") as f:
        f.write("Time taken for refactored add_water_supply: "
                f"{(end_time - start_time)/n_iter} seconds\n")


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

#skip
#@pytest.mark.skip(reason="passed")
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
    n_iter = 10
    start_time = pytime.time()
    for i in range(n_iter):
        result_rf = new_add_e_flow(test_context)
    end_time = pytime.time()
    with open("supply_parity_results.txt", "a") as f:
        f.write(f"Time taken for new_add_e_flow: "
                f"{(end_time - start_time)/n_iter} seconds\n")

    start_time = pytime.time()
    for i in range(n_iter):
        result = add_e_flow_legacy(test_context)
    end_time = pytime.time()
    with open("supply_parity_results.txt", "a") as f:
        f.write("Time taken for add_e_flow legacy: "
                f"{(end_time - start_time)/n_iter} seconds\n")

    # both are dicts so compare values
    assert set(result_rf.keys()) == set(result.keys()), "parameter keys mismatch"
    for key in result_rf:
        pdt.assert_frame_equal(result_rf[key].sort_index(axis=1),
                               result[key].sort_index(axis=1))





