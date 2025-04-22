import time

import pandas.testing as pdt
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.irrigation import (
    add_irr_structure as add_irr_structure_refactor,
)
from message_ix_models.model.water.data.irrigation_legacy import (
    add_irr_structure,
)


def test_add_irr_structure(test_context):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}

    mp = test_context.get_platform()
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
    test_context["water build info"] = ScenarioInfo(s)
    n_iter = 10
    # Call the function to be tested
    start_time = time.time()
    for _ in range(n_iter):
        result = add_irr_structure(test_context)
    end_time = time.time()
    print(f"Time taken for add_irr_structure: {end_time - start_time} seconds")

    start_time = time.time()
    for _ in range(n_iter):
        result_refactor = add_irr_structure_refactor(test_context)
    end_time = time.time()
    print(f"Time taken for add_irr_structure_refactor: {end_time - start_time} seconds")

    # Assert the results are identical
    assert result.keys() == result_refactor.keys()
    for key in result:
        # Use pandas testing utility to compare DataFrames
        pdt.assert_frame_equal(result[key], result_refactor[key], check_dtype=False)
