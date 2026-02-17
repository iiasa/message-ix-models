"""Tests for tools.impacts.temporal — year resampling."""

import pandas as pd
import pytest

from message_ix_models.tools.impacts import extract_region_code
from message_ix_models.tools.impacts.temporal import sample_to_model_years


class TestSampleToModelYears:
    @pytest.fixture
    def annual_df(self):
        """Annual data 2020-2040 for 3 basins."""
        years = list(range(2020, 2041))
        data = {"basin": ["B1", "B2", "B3"]}
        for y in years:
            data[y] = [float(y)] * 3  # value = year for easy verification
        return pd.DataFrame(data)

    def test_point_method(self, annual_df):
        model_years = [2020, 2025, 2030, 2035, 2040]
        result = sample_to_model_years(
            annual_df, ["basin"], model_years, method="point"
        )
        assert list(result.columns) == ["basin"] + model_years
        # Values should be the year itself
        assert result[2025].iloc[0] == 2025.0
        assert result[2040].iloc[0] == 2040.0

    def test_average_method(self, annual_df):
        model_years = [2020, 2025, 2030]
        result = sample_to_model_years(
            annual_df, ["basin"], model_years, method="average"
        )
        # 2025 should average 2021-2025 = mean(2021..2025) = 2023
        assert pytest.approx(result[2025].iloc[0]) == 2023.0

    def test_forward_fill_beyond_range(self, annual_df):
        model_years = [2020, 2030, 2040, 2050]
        result = sample_to_model_years(
            annual_df, ["basin"], model_years, method="point"
        )
        # 2050 is beyond 2040: forward-filled from 2040
        assert result[2050].iloc[0] == result[2040].iloc[0]

    def test_invalid_method(self, annual_df):
        with pytest.raises(ValueError, match="method must be"):
            sample_to_model_years(annual_df, ["basin"], [2020], method="cubic")

    def test_missing_year_column(self, annual_df):
        # 2050 is beyond the input range (2020-2040) but gets forward-filled.
        # 2019 is before input range and truly missing.
        with pytest.raises(ValueError, match="not found in input"):
            sample_to_model_years(annual_df, ["basin"], [2019, 2020], method="point")

    def test_no_year_columns(self):
        df = pd.DataFrame({"basin": ["B1"], "name": ["test"]})
        with pytest.raises(ValueError, match="No integer year columns"):
            sample_to_model_years(df, ["basin", "name"], [2020])


class TestExtractRegionCode:
    def test_with_prefix(self):
        assert extract_region_code("R12_AFR") == "AFR"
        assert extract_region_code("R12_WEU") == "WEU"

    def test_without_prefix(self):
        assert extract_region_code("AFR") == "AFR"
        assert extract_region_code("RCPA") == "RCPA"

    def test_other_prefix(self):
        # Should not strip non-R12 prefixes
        assert extract_region_code("R11_AFR") == "R11_AFR"
