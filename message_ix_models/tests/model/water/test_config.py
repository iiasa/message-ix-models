from message_ix_models.model.water.config import Config


def test_config_from_context(test_context) -> None:
    test_context.water = {"RCP": "2p6", "SDG": "SDG", "REL": "med"}

    cfg = Config.from_context(test_context)

    assert isinstance(cfg, Config)
    assert cfg.RCP == "2p6"
    assert cfg.SDG == "SDG"
    assert cfg.REL == "med"
