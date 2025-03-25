import pytest
from message_ix import Scenario
from typing import Union
import pandas as pd

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes

from message_ix_models.model.water.data.demands_rf import read_water_availability as new_read_water_availability, add_water_availability as new_add_water_availability, add_sectoral_demands as new_add_sectoral_demands
from message_ix_models.model.water.data.demands import read_water_availability as old_read_water_availability, add_water_availability as old_add_water_availability, add_sectoral_demands as old_add_sectoral_demands




def sort_and_reset(df: Union[pd.DataFrame, pd.Series]) -> Union[pd.DataFrame, pd.Series]:
    """Sort and reset DataFrame or Series index for robust comparisons."""
    if isinstance(df, pd.DataFrame):
        return df.sort_index(axis=1).reset_index(drop=True)
    elif isinstance(df, pd.Series):
        return df.sort_index().reset_index(drop=True)
    else:
        raise TypeError(f"Expected DataFrame or Series, got {type(df)}")

@new_add_sectoral_demands.minimum_version
@pytest.mark.parametrize(
    ["SDG", "time"], [("baseline", "year"), ("ambitious", "month")]
)

#deactivate this test for now
#@pytest.mark.skip(reason="passed")
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



#@pytest.mark.skip(reason="passed")
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
    result1_sw, result1_gw = new_read_water_availability(context=test_context)
    result2_sw, result2_gw = old_read_water_availability(context=test_context)

    result1 = new_add_water_availability(context=test_context)
    result2 = old_add_water_availability(context=test_context)

    assert set(result1.keys()) == set(result2.keys())

    for key in result1:

        pd.testing.assert_frame_equal(
            sort_and_reset(result1[key]),
            sort_and_reset(result2[key]),
            check_dtype=False
        )





