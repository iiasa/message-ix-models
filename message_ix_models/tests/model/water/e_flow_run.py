from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data import water_supply_pt3, water_supply_pt3_refactor

# Configure context manually
class DummyContext(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"attribute {name} not found")
    def __setattr__(self, name, value):
        self[name] = value

def main():
    print("=== ENVIRONMENTAL FLOW TEST RUNNER ===")
    
    # Setup context
    print("\nSetting up context...")
    ctx = DummyContext()
    sets = {"year": [2020, 2030, 2040]}
    ctx["water build info"] = ScenarioInfo(y0=2020, set=sets)
    ctx.type_reg = "country"
    ctx.regions = "R12"
    nodes = get_codes(f"node/{ctx.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    ctx.map_ISO_c = {ctx.regions: nodes[0]}
    ctx.RCP = "2p6"
    ctx.REL = "med" 
    ctx.time = "year"
    ctx.SDG = True
    ctx.nexus_set = "nexus"

    print(f"Context configured with regions: {ctx.regions}")
    print(f"Node mapping: {ctx.map_ISO_c}")

    # Run legacy implementation
    print("\n=== RUNNING LEGACY IMPLEMENTATION ===")
    ef_legacy = water_supply_pt3.add_e_flow(ctx)
    print("\nLEGACY RESULTS:")
    print(f"Parameters: {list(ef_legacy.keys())}")
    if 'bound_activity_lo' in ef_legacy:
        print(f"node_loc values: {ef_legacy['bound_activity_lo']['node_loc'].unique()[:5]}...")
        print(f"Data shape: {ef_legacy['bound_activity_lo'].shape}")

    # Run refactored implementation 
    print("\n=== RUNNING REFACTORED IMPLEMENTATION ===")
    ef_refact = water_supply_pt3_refactor.add_e_flow(ctx)
    print("\nREFACTORED RESULTS:")
    print(f"Parameters: {list(ef_refact.keys())}")
    if 'bound_activity_lo' in ef_refact:
        print(f"node_loc values: {ef_refact['bound_activity_lo']['node_loc'].unique()[:5]}...")
        print(f"Data shape: {ef_refact['bound_activity_lo'].shape}")

    # Comparison
    print("\n=== COMPARISON RESULTS ===")
    try:
        assert set(ef_legacy.keys()) == set(ef_refact.keys()), "Parameter mismatch"
        print("✅ Parameter keys match")
        
        for key in ef_legacy:
            try:
                pd.testing.assert_frame_equal(
                    ef_legacy[key].sort_index(axis=1),
                    ef_refact[key].sort_index(axis=1),
                    check_names=True
                )
                print(f"✅ {key} data matches")
            except AssertionError as e:
                print(f"❌ {key} data mismatch")
                print(f"Difference:\n{e}")
    except Exception as e:
        print(f"❌ Comparison failed: {e}")

if __name__ == "__main__":
    main()