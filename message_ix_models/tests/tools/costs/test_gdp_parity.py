import time

import pandas as pd
import pandas.testing as pdt
import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.gdp import (
    adjust_cost_ratios_with_gdp,
    adjust_cost_ratios_with_gdp_legacy,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)


def assert_equal_result(legacy, refactored):
    if isinstance(legacy, dict) and isinstance(refactored, dict):
        # Ensure the dictionaries have the same keys
        assert set(legacy.keys()) == set(refactored.keys()), (
            "Dictionary keys do not match"
        )
        # Recursively compare each value in the dictionary
        for key in legacy:
            assert_equal_result(legacy[key], refactored[key])
    elif isinstance(legacy, pd.DataFrame) and isinstance(refactored, pd.DataFrame):
        legacy = legacy.sort_index(axis=1)
        refactored = refactored.sort_index(axis=1)
        pdt.assert_frame_equal(legacy, refactored)
    elif isinstance(legacy, pd.Series) and isinstance(refactored, pd.Series):
        legacy = legacy.sort_index()
        refactored = refactored.sort_index()
        pdt.assert_series_equal(legacy, refactored)
    else:
        raise ValueError(
            f"Type mismatch: legacy type {type(legacy)} vs "
            f"refactored type {type(refactored)}"
        )

#@pytest.mark.skip(reason="Skipping test_adjust_cost_ratios_with_gdp")
@pytest.mark.parametrize("module", ("energy", "materials", "cooling"))
def test_adjust_cost_ratios_with_gdp(test_context, module) -> None:
    # Set parameters
    test_context.model.regions = "R12"

    # Mostly defaults
    config = Config(module=module, node="R12", scenario="SSP2")

    # Get regional differentiation
    region_diff = apply_regional_differentiation(config)
    n_iter = 5
    # Get adjusted cost ratios based on GDP per capita
    start_time = time.time()
    for _ in range(n_iter):
        result_legacy = adjust_cost_ratios_with_gdp_legacy(region_diff, config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(
            f"Time taken for adjust_cost_ratios_with_gdp: "
            f"{(end_time - start_time) / n_iter} seconds\n"
        )

    # Get adjusted cost ratios based on GDP per capita using vectorized approach
    start_time = time.time()
    for _ in range(n_iter):
        result_vectorized = adjust_cost_ratios_with_gdp(region_diff, config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(
            f"Time taken for adjust_cost_ratios_with_gdp_vectorized:"
            f"{(end_time - start_time) / n_iter} seconds\n"
        )

    # Assert that the results are equal
    assert_equal_result(result_legacy, result_vectorized)
