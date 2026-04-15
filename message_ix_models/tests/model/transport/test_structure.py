from message_ix_models import Context
from message_ix_models.model.transport import Config
from message_ix_models.model.transport.structure import (
    get_commodity_groups,
    get_technology_groups,
)


def test_get_commodity_groups(test_context: Context) -> None:
    config = Config.from_context(test_context)

    # Function runs
    c = get_commodity_groups(config.spec)

    # The "_T" groups is equal to the union of all others
    assert set(
        c["activity P"] + c["activity F"] + c["disutility"] + c["vehicle activity"]
    ) == set(c["_T"])


def test_get_technology_groups(test_context: Context) -> None:
    config = Config.from_context(test_context)

    # Function runs
    t = get_technology_groups(config.spec)

    # Vehicle technologies for each mode are present
    assert {"2W", "AIR", "BUS", "F RAIL", "F ROAD", "LDV", "RAIL"} < set(t)

    # Vehicle technologies for each service are present
    assert {"F", "P"} < set(t)

    # Service groups aggregate vehicle technologies for related modes
    assert set(t["2W"] + t["AIR"] + t["BUS"] + t["LDV"] + t["RAIL"]) == set(t["P"])
    assert set(t["F RAIL"] + t["F ROAD"]) == set(t["F"])

    # All vehicle technologies
    assert not any(" usage" in tech for tech in t["vehicle"]), t["vehicle"]
    assert set(t["F"] + t["P"] + t["OTHER"]) == set(t["vehicle"])

    # # of LDV usage technologies: 27 consumer groups × # of LDV technologies
    assert 27 * len(t["LDV"]) == len(t["usage LDV"])

    # Vehicle and usage technologies
    assert set(t["vehicle"] + t["usage"]) == set(t["_T"])
