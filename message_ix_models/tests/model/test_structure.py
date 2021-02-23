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
