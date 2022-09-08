import pytest

from message_data.model.buildings.build import get_spec, get_techs
from message_data.model.buildings.report import configure_legacy_reporting


@pytest.mark.parametrize("commodity", [None, "gas"])
def test_get_techs(test_context, commodity):
    test_context.regions = "R12"
    spec = get_spec(test_context)
    result = get_techs(spec, commodity)

    # Generated technologies with buildings sector and end-use
    assert "gas_resid_cook" in result

    # Generated technologies for residuals of corresponding *_rc in the base model spec
    print(result)
    assert "gas_afofi" in result


def test_configure_legacy_reporting(test_context):
    config = dict()

    configure_legacy_reporting(config)

    # Generated technology names are added to the appropriate sets
    assert ["meth_afofi"] == config["rc meth"]
    assert "h2_fc_AFOFI" in config["rc h2"]
