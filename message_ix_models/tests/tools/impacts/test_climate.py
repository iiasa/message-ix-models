"""Tests for tools.impacts.climate — GMT input parsing."""

import numpy as np
import pandas as pd
import pytest

from message_ix_models.tools.impacts.climate import (
    _MAGICC_GSAT_VARIABLE,
    load_gmt,
    load_magicc_ensemble,
    load_magicc_percentiles,
    percentiles_to_ensemble,
)


class TestLoadGmt:
    """Test auto-detection dispatch in load_gmt."""

    def test_ndarray_1d(self):
        gmt = np.linspace(1.0, 2.5, 81)
        gmt_2d, years = load_gmt(gmt)
        assert gmt_2d.shape == (1, 81)
        assert years[0] == 2020
        assert years[-1] == 2100

    def test_ndarray_2d(self):
        gmt = np.random.default_rng(42).normal(1.5, 0.3, size=(100, 81))
        gmt_2d, years = load_gmt(gmt)
        assert gmt_2d.shape == (100, 81)

    def test_ndarray_2d_with_n_runs(self):
        gmt = np.random.default_rng(42).normal(1.5, 0.3, size=(100, 81))
        gmt_2d, years = load_gmt(gmt, n_runs=10)
        assert gmt_2d.shape == (10, 81)

    def test_unsupported_type(self):
        with pytest.raises(TypeError, match="Unsupported source type"):
            load_gmt(42)

    def test_unrecognized_dataframe(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        with pytest.raises(ValueError, match="not recognized"):
            load_gmt(df)

    def test_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_gmt("/nonexistent/file.xlsx")


class TestLoadMagiccEnsemble:
    """Test MAGICC ensemble parsing with synthetic data."""

    @pytest.fixture
    def synthetic_magicc_df(self):
        """Create a minimal synthetic MAGICC DataFrame."""
        n_runs = 5
        years = list(range(2020, 2031))
        rows = []
        for i in range(n_runs):
            row = {
                "Model": f"MAGICC|run_{i}",
                "Scenario": "test_scenario",
                "Variable": _MAGICC_GSAT_VARIABLE,
                "Region": "World",
                "Unit": "K",
            }
            for y in years:
                row[y] = 1.0 + 0.01 * y + 0.1 * i
            rows.append(row)
        return pd.DataFrame(rows)

    def test_basic_load(self, synthetic_magicc_df):
        gmt_2d, years = load_magicc_ensemble(synthetic_magicc_df)
        assert gmt_2d.shape == (5, 11)
        assert len(years) == 11

    def test_dispatch_from_load_gmt(self, synthetic_magicc_df):
        gmt_2d, years = load_gmt(synthetic_magicc_df, n_runs=3)
        assert gmt_2d.shape == (3, 11)

    def test_empty_filter(self):
        df = pd.DataFrame(
            {
                "Model": ["something"],
                "Variable": ["wrong_variable"],
                "Scenario": ["test"],
            }
        )
        with pytest.raises(ValueError, match="No GSAT ensemble data"):
            load_magicc_ensemble(df)


class TestLoadMagiccPercentiles:
    @pytest.fixture
    def synthetic_percentile_df(self):
        years = list(range(2020, 2031))
        rows = []
        for pct in ["5.0th Percentile", "50.0th Percentile", "95.0th Percentile"]:
            row = {
                "Model": "MAGICC",
                "Scenario": "test",
                "Variable": f"{_MAGICC_GSAT_VARIABLE}|{pct}",
                "Region": "World",
                "Unit": "K",
            }
            for y in years:
                row[y] = 1.0 + 0.01 * (y - 2020)
            rows.append(row)
        return pd.DataFrame(rows)

    def test_basic_load(self, synthetic_percentile_df):
        result = load_magicc_percentiles(synthetic_percentile_df)
        assert len(result) == 3
        for label, (gmt, years) in result.items():
            assert len(gmt) == 11
            assert len(years) == 11


class TestPercentilesToEnsemble:
    def test_weighted(self):
        trajectories = {
            "p10": (np.ones(10), np.arange(2020, 2030)),
            "p50": (np.ones(10) * 1.5, np.arange(2020, 2030)),
            "p90": (np.ones(10) * 2.0, np.arange(2020, 2030)),
        }
        gmt_2d, years = percentiles_to_ensemble(trajectories)
        assert gmt_2d.shape == (3, 10)
        assert len(years) == 10

    def test_unknown_method(self):
        with pytest.raises(ValueError, match="Unknown method"):
            percentiles_to_ensemble({}, method="invalid")
