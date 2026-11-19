import pandas as pd
import pytest

from message_ix_models.model.water.data.pre_processing.basin_allocation import (
    country_to_region_map,
    distribute_by_shares,
)


@pytest.mark.parametrize(
    ("country", "region"),
    [
        ("USA", "NAM"),
        ("CHN", "CHN"),
        ("IND", "SAS"),
    ],
)
def test_country_to_region_map_uses_dominant_overlap(country, region):
    assert country_to_region_map("R12")[country] == region


def test_distribute_by_shares():
    totals = pd.DataFrame({"region": ["R"], "year": [2030], "value": [10.0]})
    shares = pd.DataFrame(
        {"region": ["R", "R"], "BCU_name": ["a", "b"], "share": [0.25, 0.75]}
    )

    result = distribute_by_shares(
        totals,
        shares,
        on=["region"],
        value_col="value",
        output_col="allocated",
    )

    allocated = result.set_index("BCU_name")["allocated"]
    assert allocated["a"] == pytest.approx(2.5)
    assert allocated["b"] == pytest.approx(7.5)
