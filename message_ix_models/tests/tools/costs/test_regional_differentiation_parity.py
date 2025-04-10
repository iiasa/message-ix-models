import time
import cProfile
import pstats

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.regional_differentiation import (
    adjust_technology_mapping,
    apply_regional_differentiation,
    get_intratec_data,
    get_raw_technology_mapping,
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

@pytest.mark.skip(reason="Skipping test_get_weo_data")
def test_get_weo_data() -> None:

    start_time = time.time()
    result_legacy = get_weo_data()
    end_time = time.time()
    with open("weo_data_time.txt", "a") as f:
        f.write(f"Time taken for legacy get_weo_data: {end_time - start_time} seconds\n")
    start_time = time.time()
    result_fast = get_weo_data_fast()
    end_time = time.time()
    with open("weo_data_time.txt", "a") as f:
        f.write(f"Time taken for fast get_weo_data: {end_time - start_time} seconds\n")

    assert_equal_result(result_legacy, result_fast)

@pytest.mark.skip(reason="Skipping test_get_intratec_data") 
def test_get_intratec_data() -> None:
    res = get_intratec_data()

    # Check that the regions of R12 are present
    assert all(
        [
            "R11_NAM",
            "R11_LAM",
            "R11_WEU",
            "R11_EEU",
            "R11_FSU",
            "R11_AFR",
            "R11_MEA",
            "R11_SAS",
            "R11_CPA",
            "R11_PAS",
            "R11_PAO",
        ]
        == res.node.unique()
    )


@pytest.mark.parametrize(
    "module, t_exp, rds_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}, {"weo"}),
        ("materials", {"biomass_NH3", "meth_h2", "furnace_foil_steel"}, {"energy"}),
        (
            "cooling",
            {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"},
            {"energy"},
        ),
    ),
)
@pytest.mark.skip(reason="Skipping test_get_raw_technology_mapping")
def test_get_raw_technology_mapping(module, t_exp, rds_exp) -> None:
    # Function runs without error
    result = get_raw_technology_mapping(module)

    # Expected technologies are present
    assert t_exp <= set(result.message_technology.unique())

    # Expected values for regional differentiation sources
    assert rds_exp <= set(result.reg_diff_source.unique())


@pytest.mark.parametrize("module", ("energy", "materials", "cooling"))
@pytest.mark.skip(reason="Skipping test_adjust_technology_mapping")
def test_adjust_technology_mapping(module) -> None:
    energy_raw = get_raw_technology_mapping("energy")

    # Function runs without error
    result = adjust_technology_mapping(module)

    # For module="energy", adjustment has no effect; output data are the same
    if module == "energy":
        assert energy_raw.equals(result)

    # The "energy" regional differentiation source is not present in the result data
    assert "energy" not in result.reg_diff_source.unique()

    # The "weo" regional differentiation source is present in the result data
    assert "weo" in result.reg_diff_source.unique()


@pytest.mark.parametrize(
    "module, t_exp",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_res1"}),
        ("materials", {"biomass_NH3", "meth_h2", "furnace_foil_steel"}),
        ("cooling", {"coal_ppl__cl_fresh", "gas_cc__air", "nuc_lc__ot_fresh"}),
    ),
)

#@pytest.mark.skip(reason="Skipping test_apply_regional_differentiation")
def test_apply_regional_differentiation(module, t_exp) -> None:
    """Regional differentiation is applied correctly for each `module`."""
    config = Config(module=module)

    n_iter = 5
    # Function runs without error
    start_time = time.time()
    for _ in range(n_iter):
        result = apply_regional_differentiation(config, vectorized=True)
    end_time = time.time()

    with open("apply_regional_differentiation_time.txt", "a") as f:
        f.write(f"Time taken for vectorized apply_regional_differentiation: {(end_time - start_time) / n_iter} seconds\n")
    start_time = time.time()
    for _ in range(n_iter):
        result_legacy = apply_regional_differentiation(config, vectorized=False)
    end_time = time.time()
    with open("apply_regional_differentiation_time.txt", "a") as f:
        f.write(f"Time taken for legacy apply_regional_differentiation: {(end_time - start_time) / n_iter} seconds\n")
    assert_equal_result(result_legacy, result)
