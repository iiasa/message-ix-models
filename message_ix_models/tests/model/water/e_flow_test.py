import pytest
import pandas as pd
import pandas.testing as pdt
from ixmp import Platform
from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data import water_supply_pt3, water_supply_pt3_refactor
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

    # Add debug prints for node mapping
    print("\nNODES IN TEST:")
    print(f"Regions: {real_context.regions}")
    print(f"Nodes from World child: {nodes}")
    print(f"map_ISO_c: {real_context.map_ISO_c}")

    ef_legacy = water_supply_pt3.add_e_flow(real_context)
    ef_refact = water_supply_pt3_refactor.add_e_flow(real_context)
    
    # Remove the output parameter checks
    print("\nLEGACY node_loc VALUES:")
    print(ef_legacy['bound_activity_lo']['node_loc'].unique())
    
    print("\nREFACTORED node_loc VALUES:")
    print(ef_refact['bound_activity_lo']['node_loc'].unique())
    
    assert set(ef_legacy.keys()) == set(ef_refact.keys())
    for key in ef_legacy:
        pdt.assert_frame_equal(ef_legacy[key].sort_index(axis=1), ef_refact[key].sort_index(axis=1))