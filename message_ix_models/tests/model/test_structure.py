import pytest

from message_ix_models.model.structure import get_codes


@pytest.mark.parametrize(
    "name",
    (
        "cd_links/unit",
        "level",
        "node/ISR",
        "node/R11",
        "node/R14",
        "node/R32",
        "node/RCP",
        "technology",
    ),
)
def test_get_codes(name):
    get_codes(name)


def test_get_codes_hierarchy():
    """get_codes() returns objects with the expected hierarchical relationship."""
    codes = get_codes("node/R11")

    AUT = codes[codes.index("AUT")]
    R11_WEU = codes[codes.index("R11_WEU")]
    World = codes[codes.index("World")]

    assert R11_WEU is AUT.parent
    assert AUT in R11_WEU.child

    assert World is R11_WEU.parent
    assert R11_WEU in World.child
