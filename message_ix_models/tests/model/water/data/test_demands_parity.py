import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.demands_pt3 import add_sectoral_demands as old_add_sectoral_demands
from message_ix_models.model.water.data.water_demand_pt3_rf import add_sectoral_demands as new_add_sectoral_demands


def sort_and_reset(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and reset DataFrame index for robust comparisons."""
    return df.sort_index(axis=1).reset_index(drop=True)

@new_add_sectoral_demands.minimum_version
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
    result1 = new_add_sectoral_demands(context=test_context)
    result2 = old_add_sectoral_demands(context=test_context)
    # First, verify that both outputs have the same keys.
    assert set(result1.keys()) == set(result2.keys())
    
    # Then, assert equality of each corresponding DataFrame after sorting.
    for key in result1:
         pd.testing.assert_frame_equal(
             sort_and_reset(result1[key]),
             sort_and_reset(result2[key]),
             check_dtype=False
         )



