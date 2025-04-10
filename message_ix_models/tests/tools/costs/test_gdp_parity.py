import time

import pandas as pd
import pandas.testing as pdt
import pytest
from sdmx.model.common import Code

from message_ix_models.model.structure import get_codes
from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.gdp import (
    adjust_cost_ratios_with_gdp,
    adjust_cost_ratios_with_gdp_vectorized,
    process_raw_ssp_data,
)
from message_ix_models.tools.costs.gdp import (
    process_raw_ssp_data as process_raw_ssp_data_legacy,
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
            f"Type mismatch: legacy type {type(legacy)} vs refactored type {type(refactored)}"
        )


#@pytest.mark.skip(reason="Skipping test_process_raw_ssp_data")
@pytest.mark.parametrize("node", ("R11", "R12"))
def test_process_raw_ssp_data(test_context, node) -> None:
    # Set the "regions" value on the context
    test_context.model.regions = node
    config = Config(node=node)

    # Retrieve list of node IDs
    nodes = get_codes(f"node/{node}")
    # Convert to string
    regions = set(map(str, nodes[nodes.index(Code(id="World"))].child))

    # Function runs
    # - context is ignored by process_raw_ssp_data
    # - node is ignored by process_raw_ssp_data1
    n_iter = 1
    start_time = time.time()
    for _ in range(n_iter):
        result = process_raw_ssp_data(context=test_context, config=config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(f"Time taken for process_raw_ssp_data: {(end_time - start_time) / n_iter} seconds\n")

    start_time = time.time()
    for _ in range(n_iter):
        result_legacy = process_raw_ssp_data_legacy(context=test_context, config=config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(f"Time taken for process_raw_ssp_data_legacy: {(end_time - start_time) / n_iter} seconds\n")

    # Assert that the results are equal
    assert_equal_result(result, result_legacy)

@pytest.mark.skip(reason="Skipping test_adjust_cost_ratios_with_gdp")
@pytest.mark.parametrize("module", ("energy", "materials", "cooling"))
def test_adjust_cost_ratios_with_gdp(test_context, module) -> None:
    # Set parameters
    test_context.model.regions = "R12"

    # Mostly defaults
    config = Config(module=module, node="R12", scenario="SSP2")

    # Get regional differentiation
    region_diff = apply_regional_differentiation(config)
    n_iter = 10
    # Get adjusted cost ratios based on GDP per capita
    start_time = time.time()
    for _ in range(n_iter):
        result = adjust_cost_ratios_with_gdp(region_diff, config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(f"Time taken for adjust_cost_ratios_with_gdp: {(end_time - start_time) / n_iter} seconds\n")

    # Get adjusted cost ratios based on GDP per capita using vectorized approach
    start_time = time.time()
    for _ in range(n_iter):
        result_vectorized = adjust_cost_ratios_with_gdp_vectorized(region_diff, config)
    end_time = time.time()
    with open("time_taken_gdp.txt", "a") as f:
        f.write(f"Time taken for adjust_cost_ratios_with_gdp_vectorized: {(end_time - start_time) / n_iter} seconds\n")

    # Assert that the results are equal
    assert_equal_result(result, result_vectorized)
