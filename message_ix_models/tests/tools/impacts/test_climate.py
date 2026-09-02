"""Tests for tools.impacts.climate — GMT array extraction."""

import numpy as np
import pandas as pd
import pytest

from message_ix_models.tools.impacts.climate import (
    _GSAT_VAR,
    GmtArray,
    gmt_ensemble,
    gmt_expectation,
    load_magicc_gmt,
)


@pytest.fixture
def wide_df():
    """3 runs, 5 years, int year columns."""
    years = list(range(2020, 2025))
    rows = []
    for i in range(3):
        row = {"Model": f"run_{i}", "Variable": "GSAT"}
        for y in years:
            row[y] = 1.0 + 0.01 * (y - 2020) + 0.1 * i
        rows.append(row)
    return pd.DataFrame(rows)


def test_gmt_ensemble_year_labels(wide_df):
    result = gmt_ensemble(wide_df, ["Model", "Variable"])
    assert isinstance(result, GmtArray)
    np.testing.assert_array_equal(result.years, [2020, 2021, 2022, 2023, 2024])


def test_gmt_ensemble_str_year_columns():
    df = pd.DataFrame({"id": ["a", "b"], "2020": [1.0, 1.1], "2025": [2.0, 2.1]})
    result = gmt_ensemble(df, ["id"])
    assert result.values.shape == (2, 2)
    np.testing.assert_array_equal(result.years, [2020, 2025])


def test_gmt_expectation_mean_across_rows():
    df = pd.DataFrame(
        {"id": ["a", "b", "c"], 2020: [1.0, 2.0, 3.0], 2025: [4.0, 5.0, 6.0]}
    )
    result = gmt_expectation(gmt_ensemble(df, ["id"]))
    assert result.values.shape == (2,)
    assert result.values[0] == pytest.approx(2.0)  # mean(1, 2, 3)
    assert result.values[1] == pytest.approx(5.0)  # mean(4, 5, 6)


def test_gmt_expectation_nan_handling():
    df = pd.DataFrame({"id": ["a", "b"], 2020: [1.0, np.nan], 2025: [3.0, 5.0]})
    result = gmt_expectation(gmt_ensemble(df, ["id"]))
    assert result.values[0] == pytest.approx(1.0)  # nanmean(1.0, nan)
    assert result.values[1] == pytest.approx(4.0)  # nanmean(3.0, 5.0)


def _write_iamc_xlsx(path, *, model_runs=("run_0", "run_1"), variable=_GSAT_VAR):
    rows = [
        {
            "Model": f"MAGICCv7.5.3|{run}",
            "Scenario": "scen",
            "Region": "World",
            "Variable": variable,
            "Unit": "degC",
            2020: 1.0,
            2025: 1.2,
        }
        for run in model_runs
    ]
    pd.DataFrame(rows).to_excel(path, sheet_name="data", index=False)


def test_load_magicc_gmt_reads_run_rows(tmp_path):
    _write_iamc_xlsx(tmp_path / "foo_IAMC_climateassessment.xlsx")

    result = load_magicc_gmt(tmp_path)

    assert result.values.shape == (2, 2)
    np.testing.assert_array_equal(result.years, [2020, 2025])


def test_load_magicc_gmt_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_magicc_gmt(tmp_path)


def test_load_magicc_gmt_multiple_files_raises(tmp_path):
    _write_iamc_xlsx(tmp_path / "foo_IAMC_climateassessment.xlsx")
    _write_iamc_xlsx(tmp_path / "bar_IAMC_climateassessment.xlsx")

    with pytest.raises(ValueError, match="Multiple"):
        load_magicc_gmt(tmp_path)


def test_load_magicc_gmt_no_run_rows_raises(tmp_path):
    # Rows present, but Model doesn't contain "|run_" (e.g. percentile rows only).
    _write_iamc_xlsx(tmp_path / "foo_IAMC_climateassessment.xlsx", model_runs=("p50",))

    with pytest.raises(ValueError, match="No individual GSAT runs found"):
        load_magicc_gmt(tmp_path)
