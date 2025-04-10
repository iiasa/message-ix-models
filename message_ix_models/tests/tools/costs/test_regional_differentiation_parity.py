import time

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
    get_weo_data,
    get_weo_data_fast,
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

#@pytest.mark.skip(reason="Skipping test_get_weo_data")
def test_get_weo_data() -> None:
    n_iter = 5
    start_time = time.time()
    for _ in range(n_iter):
        result_legacy = get_weo_data()
    end_time = time.time()
    with open("weo_data_time.txt", "a") as f:
        f.write(f"Time taken for legacy get_weo_data:"
               f"{(end_time - start_time) / n_iter} seconds\n")
    start_time = time.time()
    for _ in range(n_iter):
        result_fast = get_weo_data_fast()
    end_time = time.time()
    with open("weo_data_time.txt", "a") as f:
        f.write(f"Time taken for fast get_weo_data:"
               f"{(end_time - start_time) / n_iter} seconds\n")

    assert_equal_result(result_legacy, result_fast)

