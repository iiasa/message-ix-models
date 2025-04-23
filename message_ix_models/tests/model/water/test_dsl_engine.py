# test_standard_operation.py
import sys

import numpy as np
import pandas as pd
import pytest

from message_ix_models.model.water.dsl_engine import (
    _build_kw_args,
    _build_pipe_arguments,
    eval_field,
    feed_make_df,
)


@pytest.mark.xfail(
    sys.version_info < (3, 10),
    reason="Python 3.9 does not support the required features",
)
def xfail_python_older_than_3_10():
    pass


# --- Fixtures and monkeypatches ---


@pytest.fixture
def dummy_df():
    return pd.DataFrame({"value": [1, 2]}, index=[2000, 2010])


@pytest.fixture
def dummy_rule():
    return {"type": "parameter", "node": "World", "value": "value"}


@pytest.fixture
def monkey_pipes(monkeypatch):
    def _tag(name):
        def _pipe(df, *args, **kwargs):
            df = df.copy()
            df[name] = [(name, args, kwargs)] * len(df)
            return df

        return _pipe

    monkeypatch.setattr("message_ix_models.util.broadcast", _tag("broadcast"))
    monkeypatch.setattr(
        "message_ix_models.model.water.utils.map_yv_ya_lt",
        lambda periods, lt, ya: _tag("map_yv_ya_lt"),
    )
    monkeypatch.setattr("message_ix_models.util.same_node", _tag("same_node"))
    monkeypatch.setattr("message_ix_models.util.same_time", _tag("same_time"))

    monkeypatch.setattr(
        "message_ix.make_df", lambda _type, **kw: pd.DataFrame({"origin": [kw]})
    )


# --- Tests for helpers ---


def test_eval_field_single_df(dummy_df):
    expr = "df[value] * 2"
    dfs = {"df": dummy_df}
    assert eval_field(expr, dfs).equals(dummy_df["value"] * 2)


def test_feed_make_df_sl(dummy_rule, dummy_df):
    dummy_rule["value"] = "df[value] * 2"
    kwargs = feed_make_df(dummy_rule, dummy_df, skip_kwargs=[])
    assert kwargs["value"].equals(dummy_df["value"] * 2)


def test_feed_make_df_pl(dummy_rule, dummy_df):
    dummy_rule["value"] = "(df1[value] + df2[value] * 2) / df3[value]"
    dfs = {"df1": dummy_df, "df2": dummy_df * 10, "df3": dummy_df * 100}
    kwargs = feed_make_df(dummy_rule, dfs, skip_kwargs=[])
    expected = (dummy_df["value"] + (dummy_df * 10)["value"] * 2) / (
        dummy_df["value"] * 100
    )
    assert kwargs["value"].equals(expected)


@pytest.mark.parametrize(
    "flags, expected_len",
    [
        (
            dict(
                flag_broadcast=True,
                broadcast_year=2030,
                flag_map_yv_ya_lt=False,
                lt=None,
            ),
            2,
        ),
        (
            dict(
                flag_broadcast=False,
                broadcast_year=None,
                flag_map_yv_ya_lt=True,
                lt=30,
            ),
            1,
        ),
        (
            dict(
                flag_broadcast=True,
                broadcast_year=2050,
                flag_map_yv_ya_lt=True,
                lt=30,
            ),
            3,
        ),
    ],
)
def test_build_pipe_arguments(flags, expected_len):
    out = _build_pipe_arguments(
        **flags,
        year_wat=(2020, 2050),
        first_year=2020,
    )
    assert len(out) == expected_len


def test_build_kw_args_full():
    node_loc = pd.DataFrame({"n": [0, 1]})
    sub_time = pd.Series([0, 1], name="t")
    kw = _build_kw_args(
        flag_node_loc=True,
        node_loc=node_loc["n"],
        flag_time=True,
        sub_time=sub_time,
        extra_args={"foo": 1},
    )
    assert set(kw) == {"node_loc", "time", "foo"}


def test_eval_field_non_string(dummy_df):
    # If the expression is not a string, it should return the expression unchanged.
    result = eval_field(42, dummy_df)
    assert result == 42


def test_eval_field_no_valid_prefix(dummy_df):
    # If there is no valid DataFrame reference in the expression,
    # it should return the expression unchanged.
    expr = "2 * 5"
    result = eval_field(expr, dummy_df)
    assert result == expr


def test_eval_field_already_quoted(dummy_df):
    # If the column reference is already quoted, it should be evaluated correctly.
    expr = "df['value'] * 3"
    dfs = {"df": dummy_df}
    result = eval_field(expr, dfs)
    pd.testing.assert_series_equal(result, dummy_df["value"] * 3)


def test_build_kw_args_no_node_loc():
    # When flag_node_loc is False, node_loc should not be added.
    kw = _build_kw_args(
        flag_node_loc=False,
        node_loc=None,
        flag_time=False,
        sub_time=None,
        extra_args={"foo": 1},
    )
    assert "node_loc" not in kw


def test_build_kw_args_node_loc_dataframe():
    # When flag_node_loc is True with a DataFrame, the specified column should be used.
    df = pd.DataFrame({"n": [0, 1]})
    kw = _build_kw_args(
        flag_node_loc=True,
        node_loc=df["n"],
        flag_time=False,
        sub_time=None,
        extra_args=None,
    )
    pd.testing.assert_series_equal(kw["node_loc"], df["n"])


def test_build_kw_args_node_loc_ndarray():
    # When flag_node_loc is True with an ndarray, it should use the ndarray as is.
    arr = np.array([5, 6])
    kw = _build_kw_args(
        flag_node_loc=True,
        node_loc=arr,
        flag_time=False,
        sub_time=None,
        extra_args=None,
    )
    assert (kw["node_loc"] == arr).all()


def test_build_kw_args_node_loc_error_none():
    # Should raise ValueError when node_loc is required but missing.
    with pytest.raises(ValueError):
        _build_kw_args(
            flag_node_loc=True,
            node_loc=None,
            flag_time=False,
            sub_time=None,
            extra_args=None,
        )

def test_build_kw_args_time_flag_false():
    # When flag_time is False, no time key should be added.
    kw = _build_kw_args(
        flag_node_loc=False,
        node_loc=None,
        flag_time=False,
        sub_time=None,
        extra_args={"bar": 2},
    )
    assert "time" not in kw


def test_build_kw_args_time_series():
    # When flag_time is True with a pd.Series, it should add the time key.
    series_time = pd.Series([10, 20])
    kw = _build_kw_args(
        flag_node_loc=False,
        node_loc=None,
        flag_time=True,
        sub_time=series_time,
        extra_args=None,
    )
    pd.testing.assert_series_equal(kw["time"], series_time)


def test_build_kw_args_time_error_single_character():
    # A single character string for sub_time should raise ValueError.
    with pytest.raises(ValueError):
        _build_kw_args(
            flag_node_loc=False,
            node_loc=None,
            flag_time=True,
            sub_time="y",
            extra_args=None,
        )


def test_build_kw_args_time_error_none():
    # When sub_time is None and flag_time is True, it should raise ValueError.
    with pytest.raises(ValueError):
        _build_kw_args(
            flag_node_loc=False,
            node_loc=None,
            flag_time=True,
            sub_time=None,
            extra_args=None,
        )


def test_build_kw_args_time_unexpected():
    # An unexpected type for sub_time should raise ValueError.
    with pytest.raises(ValueError):
        _build_kw_args(
            flag_node_loc=False,
            node_loc=None,
            flag_time=True,
            sub_time=123,
            extra_args=None,
        )
