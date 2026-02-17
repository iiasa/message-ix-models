"""Tests for tools.impacts.climate — GMT array extraction."""

import numpy as np
import pandas as pd
import pytest

from message_ix_models.tools.impacts.climate import (
    GmtArray,
    gmt_ensemble,
    gmt_expectation,
)


class TestGmtEnsemble:
    @pytest.fixture
    def wide_df(self):
        """3 runs, 5 years, int year columns."""
        years = list(range(2020, 2025))
        rows = []
        for i in range(3):
            row = {"Model": f"run_{i}", "Variable": "GSAT"}
            for y in years:
                row[y] = 1.0 + 0.01 * (y - 2020) + 0.1 * i
            rows.append(row)
        return pd.DataFrame(rows)

    def test_shape(self, wide_df):
        result = gmt_ensemble(wide_df, ["Model", "Variable"])
        assert isinstance(result, GmtArray)
        assert result.values.shape == (3, 5)
        assert len(result.years) == 5

    def test_year_labels(self, wide_df):
        result = gmt_ensemble(wide_df, ["Model", "Variable"])
        np.testing.assert_array_equal(result.years, [2020, 2021, 2022, 2023, 2024])

    def test_values_correct(self, wide_df):
        result = gmt_ensemble(wide_df, ["Model", "Variable"])
        # First run, first year: 1.0 + 0 + 0 = 1.0
        assert result.values[0, 0] == pytest.approx(1.0)
        # Third run, last year: 1.0 + 0.04 + 0.2 = 1.24
        assert result.values[2, 4] == pytest.approx(1.24)

    def test_str_year_columns(self):
        df = pd.DataFrame({"id": ["a", "b"], "2020": [1.0, 1.1], "2025": [2.0, 2.1]})
        result = gmt_ensemble(df, ["id"])
        assert result.values.shape == (2, 2)
        np.testing.assert_array_equal(result.years, [2020, 2025])


class TestGmtExpectation:
    def test_mean_across_rows(self):
        df = pd.DataFrame(
            {"id": ["a", "b", "c"], 2020: [1.0, 2.0, 3.0], 2025: [4.0, 5.0, 6.0]}
        )
        result = gmt_expectation(df, ["id"])
        assert result.values.shape == (2,)
        assert result.values[0] == pytest.approx(2.0)  # mean(1, 2, 3)
        assert result.values[1] == pytest.approx(5.0)  # mean(4, 5, 6)

    def test_nan_handling(self):
        df = pd.DataFrame({"id": ["a", "b"], 2020: [1.0, np.nan], 2025: [3.0, 5.0]})
        result = gmt_expectation(df, ["id"])
        assert result.values[0] == pytest.approx(1.0)  # nanmean(1.0, nan)
        assert result.values[1] == pytest.approx(4.0)  # nanmean(3.0, 5.0)
