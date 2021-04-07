"""Tests of :mod:`message_ix_models.util`."""
import logging
import re
from pathlib import Path

import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    MESSAGE_DATA_PATH,
    MESSAGE_MODELS_PATH,
    as_codes,
    broadcast,
    copy_column,
    ffill,
    iter_parameters,
    load_package_data,
    load_private_data,
    make_source_tech,
    package_data_path,
    private_data_path,
)

_actual_package_data = Path(__file__).parents[1].joinpath("data")


def test_as_codes():
    """Forward reference to a child is silently dropped."""
    data = dict(
        foo=dict(child=["bar"]),
        bar=dict(name="Bar!"),
    )
    result = as_codes(data)
    assert result[1] not in result[0].child


def test_broadcast(caplog):
    # Debug message logged with length-0 values
    with caplog.at_level(logging.DEBUG, logger="message_ix_models"):
        broadcast(pd.DataFrame(columns=["foo", "bar"]), foo=[], bar=[])

    assert "Don't broadcast over 'foo'; labels [] have length 0" in caplog.messages

    # TODO expand


@pytest.mark.parametrize(
    "data",
    (
        set(),
        # dict() with a value that is not a str or a further dict()
        dict(foo="foo", bar=[1, 2, 3]),
    ),
)
def test_as_codes_invalid(data):
    """as_codes() rejects invalid data."""
    with pytest.raises(TypeError):
        as_codes(data)


def test_copy_column():
    df = pd.DataFrame([[0, 1], [2, 3]], columns=["a", "b"])
    df = df.assign(c=copy_column("a"), d=4)
    assert all(df["c"] == [0, 2])
    assert all(df["d"] == 4)


def test_ffill():
    years = list(range(6))

    df = (
        make_df(
            "fix_cost",
            year_act=[0, 2, 4],
            year_vtg=[0, 2, 4],
            technology=["foo", "bar", "baz"],
            unit="USD",
        )
        .pipe(broadcast, node_loc=["A", "B", "C"])
        .assign(value=list(map(float, range(9))))
    )

    # Function completes
    result = ffill(df, "year_vtg", years, "year_act = year_vtg")

    assert 2 * len(df) == len(result)
    assert years == sorted(result["year_vtg"].unique())

    # Cannot ffill on "value" and "unit" dimensions
    with pytest.raises(ValueError, match="value"):
        ffill(df, "value", [])

    # TODO test some specific values


def test_iter_parameters(test_context):
    """Parameters indexed by set 'node' can be retrieved."""
    result = list(iter_parameters("node"))
    assert result[0] == "abs_cost_activity_soft_lo"
    assert result[-1] == "var_cost"
    # The length of this list depends on message_ix. Changes in message_ix may increase
    # the number of parameters, so use <= to future-proof. See the method comments.
    assert 99 <= len(result)


@pytest.mark.parametrize("path", _actual_package_data.rglob("*.yaml"))
def test_load_package_data(path):
    """Existing package data can be loaded."""
    load_package_data(*path.relative_to(_actual_package_data).parts)


def test_load_package_data_twice(caplog):
    """Loading the same data twice logs a message."""
    caplog.set_level(logging.DEBUG, logger="message_ix_models")
    load_package_data("node", "R11")
    load_package_data("node", "R11")
    assert "'node R11' already loaded; skip" in caplog.messages


def test_load_package_data_invalid():
    """load_package_data() raises an exception for an unsupported file type."""
    with pytest.raises(ValueError):
        load_package_data("test.xml")


@pytest.mark.xfail(
    condition=MESSAGE_DATA_PATH is None, reason="Requires message_data to be installed."
)
def test_load_private_data(*parts, suffix=None):
    load_private_data("sources.yaml")


def test_make_source_tech():
    info = ScenarioInfo()
    info.set["node"] = ["World", "node0", "node1"]
    info.set["year"] = [1, 2, 3]

    values = dict(
        capacity_factor=1.0,
        output=2.0,
        var_cost=3.0,
        technical_lifetime=4.0,
    )
    common = dict(
        commodity="commodity",
        level="level",
        mode="mode",
        technology="technology",
        time="time",
        time_dest="time",
        unit="unit",
    )
    # Code runs
    result = make_source_tech(info, common, **values)
    # Result is dictionary with the expected keys
    assert isinstance(result, dict)
    assert set(result.keys()) == set(values.keys())

    # "World" node does not appear in results
    assert set(result["output"]["node_loc"].unique()) == set(info.N[1:])

    for df in result.values():
        # Results have 2 nodes × 3 years
        assert len(df) == 2 * 3
        # No empty values
        assert not df.isna().any(None)

    del values["var_cost"]
    with pytest.raises(ValueError, match=re.escape("needs values for {'var_cost'}")):
        make_source_tech(info, common, **values)


def test_package_data_path(*parts, suffix=None):
    assert MESSAGE_MODELS_PATH.joinpath("data", "foo", "bar") == package_data_path(
        "foo", "bar"
    )


@pytest.mark.xfail(
    condition=MESSAGE_DATA_PATH is None, reason="Requires message_data to be installed."
)
def test_private_data_path(*parts, suffix=None):
    assert MESSAGE_DATA_PATH.joinpath("data", "foo", "bar") == private_data_path(
        "foo", "bar"
    )
