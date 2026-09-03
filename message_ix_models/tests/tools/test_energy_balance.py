from typing import TYPE_CHECKING

import pandas as pd
import pytest

import message_ix_models.util.sdmx
from message_ix_models.tools.energy_balance import get_source, load_data
from message_ix_models.tools.eurostat import ESTAT_ENERGY_BALANCE_UNSD
from message_ix_models.tools.unsd import UNSD_ENERGY_BALANCE

if TYPE_CHECKING:
    from message_ix_models import Context


@pytest.mark.parametrize(
    "country, cls, label",
    [
        ("KAZ", UNSD_ENERGY_BALANCE, "398"),
        ("SRB", ESTAT_ENERGY_BALANCE_UNSD, "RS"),
        ("UKR", UNSD_ENERGY_BALANCE, "804"),  # Eurostat series ends in 2020
        ("MDA", UNSD_ENERGY_BALANCE, "498"),  # Eurostat series starts in 2010
        ("GRC", ESTAT_ENERGY_BALANCE_UNSD, "EL"),  # Not the ISO 3166-1 alpha-2 code
        ("XKX", ESTAT_ENERGY_BALANCE_UNSD, "XK"),  # Not in ISO 3166-1
    ],
)
def test_get_source(country, cls, label) -> None:
    assert (cls, label) == get_source(country)


def test_get_source_unknown() -> None:
    with pytest.raises(ValueError, match="XXX"):
        get_source("XXX")


def test_load_data(monkeypatch, test_context: "Context") -> None:
    """The plain entry point returns the same frame shape from either provider."""
    index = pd.MultiIndex.from_tuples(
        [("398", "B04_NG", "B03_04", "HSO", "2000")],
        names=["REF_AREA", "COMMODITY", "TRANSACTION", "UNIT", "TIME_PERIOD"],
    )
    monkeypatch.setattr(
        message_ix_models.util.sdmx,
        "fetch_data",
        lambda *a, **kw: pd.Series([-7.0], index=index, name="value"),
    )

    result = load_data("KAZ", start=2000, end=2000)

    assert ["n", "y", "product", "flow", "value"] == list(result.columns)
    assert [("KAZ", 2000, "B04_NG", "B03_04", -7.0)] == list(
        result.itertuples(index=False, name=None)
    )
