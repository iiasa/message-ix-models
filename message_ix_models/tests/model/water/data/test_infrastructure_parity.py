import time as pytime

import pandas.testing as pdt
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)
from message_ix_models.model.water.data.infrastructure_refactor import (
    add_desalination as add_desalination_refactor,
)
from message_ix_models.model.water.data.infrastructure_refactor import (
    add_infrastructure_techs as add_infrastructure_techs_refactor,
)


# NB: This also tests start_creating_input_dataframe() and prepare_input_dataframe()
# from the same file since they are called by add_infrastructure_techs()
#@pytest.mark.skip(reason="already tested")
@pytest.mark.parametrize("SDG", ["baseline", "not_baseline"])
def test_add_infrastructure_techs(test_context, SDG, request):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.SDG = SDG
    test_context.time = "year"
    test_context.type_reg = "country"
    test_context.regions = "ZMB"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}

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

    s.commit(comment="basic water add_infrastructure_techs test model")

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Call the function to be tested
    start_time = pytime.time()
    result = add_infrastructure_techs(context=test_context)
    end_time = pytime.time()
    #write time to file infrastructure_time.txt
    with open("infrastructure_time.txt", "a") as f:
        f.write(f"Time taken for add_infrastructure_techs: {end_time - start_time} seconds\n")

    start_time = pytime.time()
    result_refactor = add_infrastructure_techs_refactor(context=test_context)
    end_time = pytime.time()
    with open("infrastructure_time.txt", "a") as f:
        f.write(f"Time taken for add_infrastructure_techs_refactor: {end_time - start_time} seconds\n")

    # Assert the results are identical
    assert result.keys() == result_refactor.keys()
    for key in result:
        # Use pandas testing utility to compare DataFrames
        pdt.assert_frame_equal(result[key], result_refactor[key], check_dtype=False)


def test_add_desalination(test_context, request):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.time = "year"
    test_context.type_reg = "global"
    test_context.regions = "R11"
    test_context.RCP = "7p0"

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

    s.commit(comment="basic water add_infrastructure_techs test model")

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Call the function to be tested
    start_time = pytime.time()
    result = add_desalination(context=test_context)
    end_time = pytime.time()
    with open("infrastructure_time.txt", "a") as f:
        f.write(f"Time taken for add_desalination: {end_time - start_time} seconds\n")

    start_time = pytime.time()
    result_refactor = add_desalination_refactor(context=test_context)
    end_time = pytime.time()
    with open("infrastructure_time.txt", "a") as f:
        f.write(f"Time taken for add_desalination_refactor: {end_time - start_time} seconds\n")


    # Assert the results are identical
    assert result.keys() == result_refactor.keys()
    for key in result:
        # Use pandas testing utility to compare DataFrames
        pdt.assert_frame_equal(result[key], result_refactor[key], check_dtype=False)
