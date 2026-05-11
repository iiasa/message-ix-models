from message_ix_models.model.water.config import Config


def test_config_from_context(test_context) -> None:
    test_context.water = {"RCP": "2p6", "SDG": "SDG", "REL": "med"}

    cfg = Config.from_context(test_context)

    assert isinstance(cfg, Config)
    assert cfg.RCP == "2p6"
    assert cfg.SDG == "SDG"
    assert cfg.REL == "med"


def test_config_from_legacy_context_attributes(test_context) -> None:
    test_context.RCP = "7p0"
    test_context.REL = "high"
    test_context.reduced_basin = True
    test_context.filter_list = ["1|AFR"]
    test_context.num_basins = 1

    cfg = Config.from_context(test_context)

    assert cfg.RCP == "7p0"
    assert cfg.REL == "high"
    assert cfg.reduced_basin is True
    assert cfg.filter_list == ["1|AFR"]
    assert cfg.num_basins == 1


def test_config_apply_legacy_context_attributes(test_context) -> None:
    cfg = Config(
        nexus_set="cooling",
        RCP="7p0",
        REL="high",
        reduced_basin=True,
        filter_list=["1|AFR"],
        num_basins=1,
    )

    test_context.water = cfg
    cfg.apply(test_context)

    assert test_context.water is cfg
    assert test_context.nexus_set == "cooling"
    assert test_context.RCP == "7p0"
    assert test_context.REL == "high"
    assert test_context.reduced_basin is True
    assert test_context.filter_list == ["1|AFR"]
    assert test_context.num_basins == 1
