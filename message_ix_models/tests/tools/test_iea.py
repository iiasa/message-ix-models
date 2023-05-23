import numpy as np
import pandas as pd
import pytest

from message_ix_models.tools.iea.weo import get_weo_data


def test_get_weo_data():
    result = get_weo_data()

    # Check that the minimum and maximum years are correct
    assert min(result.year) == "2021"
    assert max(result.year) == "2050"

    # Check that the regions are correct (in the past, "Europe" changed to "European Union")
    assert all(
        [
            "European Union",
            "United States",
            "Japan",
            "Russia",
            "China",
            "India",
            "Middle East",
            "Africa",
        ]
        == result.region.unique()
    )

    # Check one sample value
    assert (
        result.loc[
            (result.technology == "steam_coal_subcritical")
            & (result.region == "United States")
            & (result.year == "2021")
            & (result.cost_type == "capital_costs"),
            "value",
        ].values[0]
        == 1800
    )
