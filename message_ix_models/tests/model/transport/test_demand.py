from message_data.model.bare import get_spec
from message_data.model.transport import demand


def test_demand_dummy(test_context):
    """Consumer-group-specific commodities are generated."""
    test_context.regions = 'R11'
    info = get_spec(test_context)["add"]

    assert any(demand.dummy(info)["commodity"] == "transport pax URLMM")
