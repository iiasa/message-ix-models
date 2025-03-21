import pytest
import pandas as pd
import pandas.testing as pdt
from ixmp import Platform
from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data import water_supply, water_supply_total_refactor
from message_ix import Scenario

# dummy context for attribute access
class DummyContext(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"attribute {name} not found")
    def __setattr__(self, name, value):
        self[name] = value

# fixture with full real data configuration (as in test_water_supply.py)
@pytest.fixture
def real_context(test_context):
    ctx = DummyContext()
    # set basin mapping config
    sets = {"year": [2020, 2030, 2040]}
    ctx["water build info"] = ScenarioInfo(y0=2020, set=sets)
    ctx.type_reg = "country"
    ctx.regions = "ZMB"
    nodes = get_codes(f"node/{ctx.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    ctx.map_ISO_c = {ctx.regions: nodes[0]}
    ctx.RCP = "2p6"
    ctx.REL = "med"
    ctx.time = "year"
    # add extra keys that downstream functions need
    ctx.nexus_set = "nexus"
    ctx.SDG = True
    ctx.get_platform = getattr(test_context, "get_platform", lambda: Platform("local", dbtype="HSQLDB"))
    ctx.set_scenario = getattr(test_context, "set_scenario", lambda s: None)
    return ctx

def test_map_basin_region_wat_full_comparison(real_context):
    # full equality test for basin mapping outputs
    df_legacy = water_supply.map_basin_region_wat(real_context)
    df_refact = water_supply_total_refactor.map_basin_region_wat(real_context)
    pdt.assert_frame_equal(df_legacy.sort_index(axis=1), df_refact.sort_index(axis=1))

def test_add_water_supply_full_comparison(real_context):
    # update context for water supply test as in test_water_supply.py
    sets = {"year": [2020, 2030, 2040]}
    real_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    real_context.type_reg = "country"
    real_context.regions = "ZMB"
    nodes = get_codes(f"node/{real_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    real_context.map_ISO_c = {real_context.regions: nodes[0]}
    real_context.RCP = "2p6"
    real_context.REL = "med"
    real_context.time = "year"
    real_context.nexus_set = "nexus"

    mp = real_context.get_platform()
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
    s.commit(comment="commit test scenario")
    real_context.set_scenario(s)
    real_context["water build info"] = ScenarioInfo(s)

    ws_legacy = water_supply.add_water_supply(real_context)
    ws_refact = water_supply_total_refactor.add_water_supply(real_context)
    assert set(ws_legacy.keys()) == set(ws_refact.keys())
    for key in ws_legacy:
        pdt.assert_frame_equal(ws_legacy[key].sort_index(axis=1), ws_refact[key].sort_index(axis=1))

def test_add_e_flow_full_comparison(real_context):
    # update context for environmental flow test as in test_water_supply.py
    sets = {"year": [2020, 2030, 2040]}
    real_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    real_context.regions = "R12"
    nodes = get_codes(f"node/{real_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    real_context.map_ISO_c = {real_context.regions: nodes[0]}
    real_context.RCP = "2p6"
    real_context.REL = "med"
    real_context.time = "year"
    real_context.SDG = True

    ef_legacy = water_supply.add_e_flow(real_context)
    ef_refact = water_supply_total_refactor.add_e_flow(real_context)
    assert set(ef_legacy.keys()) == set(ef_refact.keys())
    for key in ef_legacy:
        pdt.assert_frame_equal(ef_legacy[key].sort_index(axis=1), ef_refact[key].sort_index(axis=1))