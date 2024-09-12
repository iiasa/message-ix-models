import os.path

import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.report import report_full
from message_ix_models.util import package_data_path


# NB: this tests all functions in model/water/reporting
@pytest.mark.xfail(reason="Temporary, for #106")
def test_report_full(test_context, request):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.time = "year"
    test_context.type_reg = "global"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    # test_context.map_ISO_c = {test_context.regions: nodes[0]}

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
    s.add_set("node", nodes)

    s.commit(comment="basic water report_full test model")
    s.set_as_default()
    # Remove quiet=True to debug using the output
    s.solve(quiet=True)

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Run the function to be tested
    report_full(sc=s, reg=test_context.regions)

    # Since the function doesn't return anything, check that output file is produced in
    # correct location
    result_file = (
        package_data_path().parents[0] / f"reporting_output/{s.model}_{s.scenario}.csv"
    )
    assert os.path.isfile(result_file)
