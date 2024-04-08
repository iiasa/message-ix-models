from message_ix_models import testing
from message_ix_models.model.water.build import build, get_spec


def test_build(request, test_context):
    scenario = testing.bare_res(request, test_context)

    # Code runs on the bare RES
    build(scenario)

    # New set elements were added
    assert "extract_surfacewater" in scenario.set("technology").tolist()


def test_get_spec(session_context):
    # Code runs
    spec = get_spec()

    # Expected return type
    assert isinstance(spec, dict) and len(spec) == 3

    # Contents are read correctly
    assert "water_supply" in spec["require"].set["level"]
