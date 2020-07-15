import message_ix
import pytest

from message_data.model.bare import get_spec
from message_data.model.transport import demand


def test_demand_dummy(test_context):
    """Consumer-group-specific commodities are generated."""
    test_context.regions = "R11"
    info = get_spec(test_context)["add"]

    assert any(demand.dummy(info)["commodity"] == "transport pax URLMM")


@pytest.mark.xfail(reason="Incomplete")
def test_from_external_data(test_context):
    test_context.regions = "R11"
    info = get_spec(test_context)["add"]

    demand.from_external_data(info)


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    result = demand.from_scenario(scenario)
    print(result)
    pytest.fail()
