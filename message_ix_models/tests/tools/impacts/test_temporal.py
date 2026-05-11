"""Tests for tools.impacts.temporal — year resampling."""

import pandas as pd
import pytest

from message_ix_models.tools.impacts.temporal import sample_to_model_years


@pytest.fixture
def annual_df():
    years = list(range(2020, 2041))
    data = {"basin": ["B1", "B2", "B3"]}
    for y in years:
        data[y] = [float(y)] * 3
    return pd.DataFrame(data)


@pytest.fixture
def decadal_df():
    rows = [
        {"node": "R12_AFR", 2020: 0.4, 2030: 0.6, 2040: 0.8, 2050: 1.0},
        {"node": "R12_NAM", 2020: 1.0, 2030: 0.9, 2040: 0.85, 2050: 0.8},
    ]
    return pd.DataFrame(rows)


def test_point_method(annual_df):
    model_years = [2020, 2025, 2030, 2035, 2040]
    result = sample_to_model_years(annual_df, ["basin"], model_years, method="point")
    assert list(result.columns) == ["basin"] + model_years
    assert result[2025].iloc[0] == 2025.0
    assert result[2040].iloc[0] == 2040.0


def test_average_method(annual_df):
    model_years = [2020, 2025, 2030]
    result = sample_to_model_years(annual_df, ["basin"], model_years, method="average")
    assert pytest.approx(result[2025].iloc[0]) == 2023.0


def test_forward_fill_beyond_range(annual_df):
    model_years = [2020, 2030, 2040, 2050]
    result = sample_to_model_years(annual_df, ["basin"], model_years, method="point")
    assert result[2050].iloc[0] == result[2040].iloc[0]


def test_missing_year_column(annual_df):
    with pytest.raises(ValueError, match="not found in input"):
        sample_to_model_years(annual_df, ["basin"], [2019, 2020], method="point")


def test_interpolate_linear_between_inputs(decadal_df):
    result = sample_to_model_years(
        decadal_df,
        ["node"],
        [2020, 2025, 2030, 2035, 2040, 2045, 2050],
        method="interpolate",
    )
    afr = result.set_index("node").loc["R12_AFR"]
    assert pytest.approx(afr[2025]) == 0.5
    assert pytest.approx(afr[2035]) == 0.7
    assert pytest.approx(afr[2045]) == 0.9
    assert afr[2020] == 0.4
    assert afr[2050] == 1.0


def test_interpolate_forward_fill_beyond_last_input(decadal_df):
    result = sample_to_model_years(
        decadal_df, ["node"], [2050, 2060, 2100], method="interpolate"
    )
    afr = result.set_index("node").loc["R12_AFR"]
    assert afr[2060] == afr[2050]
    assert afr[2100] == afr[2050]


def test_interpolate_drop_below_first_input_year_by_default(decadal_df):
    result = sample_to_model_years(
        decadal_df, ["node"], [2010, 2015, 2020, 2030], method="interpolate"
    )
    cols = [c for c in result.columns if isinstance(c, int)]
    assert 2010 not in cols
    assert 2015 not in cols
    assert 2020 in cols
    assert 2030 in cols


def test_interpolate_extrapolate_below_backfills(decadal_df):
    result = sample_to_model_years(
        decadal_df,
        ["node"],
        [2010, 2020, 2030],
        method="interpolate",
        extrapolate_below=True,
    )
    afr = result.set_index("node").loc["R12_AFR"]
    assert afr[2010] == afr[2020]


def test_interpolate_independent_per_row(decadal_df):
    result = sample_to_model_years(
        decadal_df, ["node"], [2025, 2035], method="interpolate"
    ).set_index("node")
    assert pytest.approx(result.loc["R12_AFR", 2025]) == 0.5
    assert pytest.approx(result.loc["R12_NAM", 2025]) == 0.95
    assert pytest.approx(result.loc["R12_AFR", 2035]) == 0.7
    assert pytest.approx(result.loc["R12_NAM", 2035]) == 0.875
