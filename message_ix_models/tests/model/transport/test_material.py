from message_ix_models import Context
from message_ix_models.model.transport import Config
from message_ix_models.model.transport.material import get_groups


def test_get_groups(test_context: Context) -> None:
    # Prepare a Config instance
    cfg = Config.from_context(test_context)

    # Function runs without error
    result = get_groups(cfg)

    # Does not contain mispellings of the technologies in technology.yaml
    assert "ICAe_ptrp" not in result["t"]
    assert "ICEm_ptrp" not in result["t"]
