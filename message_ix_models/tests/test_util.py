"""Tests of :mod:`message_ix_models.util`."""
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from iam_units import registry
from message_ix import Scenario, make_df
from pandas.testing import assert_series_equal

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    MESSAGE_DATA_PATH,
    MESSAGE_MODELS_PATH,
    as_codes,
    broadcast,
    check_support,
    convert_units,
    copy_column,
    ffill,
    iter_parameters,
    load_package_data,
    load_private_data,
    local_data_path,
    make_source_tech,
    maybe_query,
    package_data_path,
    private_data_path,
    series_of_pint_quantity,
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

    # With Codes already, the function is a pass-through
    assert result == as_codes(result)


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


def test_check_support(test_context):
    """:func:`.check_support` raises an exception for missing/non-matching values."""
    args = [test_context, dict(regions=["R11", "R14"]), "Test data available"]

    # Setting not set → KeyError
    with pytest.raises(KeyError, match="regions"):
        check_support(*args)

    # Accepted value
    test_context.regions = "R11"
    check_support(*args)

    # Wrong setting
    test_context.regions = "FOO"
    with pytest.raises(
        NotImplementedError,
        match=re.escape("Test data available for ['R11', 'R14']; got 'FOO'"),
    ):
        check_support(*args)


def test_convert_units(recwarn):
    """:func:`.convert_units` and :func:`.series_of_pint_quantity` work."""
    # Common arguments
    args = [pd.Series([1.1, 10.2, 100.3], name="bar"), dict(bar=(10.0, "lb", "kg"))]

    exp = series_of_pint_quantity(
        [registry("4.9895 kg"), registry("46.2664 kg"), registry("454.9531 kg")],
    )

    # With store="quantity", a series of pint.Quantity is returned
    result = convert_units(*args, store="quantity")
    assert all(np.isclose(a, b, atol=1e-4) for a, b in zip(exp.values, result.values))

    # With store="magnitude", a series of floats
    exp = pd.Series([q.magnitude for q in exp.values], name="bar")
    assert_series_equal(exp, convert_units(*args, store="magnitude"), check_dtype=False)

    # Other values for store= are errors
    with pytest.raises(ValueError, match="store='foo'"):
        convert_units(*args, store="foo")

    # series_of_pint_quantity() successfully caught warnings
    assert 0 == len(recwarn)


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


_MST_COMMON = dict(
    commodity="commodity",
    level="level",
    mode="mode",
    technology="technology",
    time="time",
    time_dest="time",
    unit="unit",
)
_MST_VALUES = dict(
    capacity_factor=1.0,
    output=2.0,
    var_cost=3.0,
    technical_lifetime=4.0,
)


def test_make_source_tech0():
    info = ScenarioInfo()
    info.set["node"] = ["World", "node0", "node1"]
    info.set["year"] = [1, 2, 3]

    values = _MST_VALUES.copy()

    # Code runs
    result = make_source_tech(info, _MST_COMMON, **values)

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
        make_source_tech(info, _MST_COMMON, **values)


def test_make_source_tech1(test_mp):
    """Test make_source_tech() with a Scenario object as input."""
    s = Scenario(test_mp, model="model", scenario="scenario", version="new")
    s.add_set("node", ["World", "node0", "node1"])
    s.add_set("technology", ["t"])
    s.add_horizon([1, 2, 3])
    s.commit("")

    make_source_tech(s, _MST_COMMON, **_MST_VALUES)


def test_maybe_query():
    """:func:`.maybe_query` works as intended."""
    s = pd.Series(
        [0, 1, 2, 3],
        index=pd.MultiIndex.from_product(
            [["a", "b"], ["c", "d"]], names=["foo", "bar"]
        ),
    )

    # No-op
    assert_series_equal(s, maybe_query(s, None))

    # Select a few rows
    assert 2 == len(maybe_query(s, "bar == 'c'"))


def test_local_data_path(pytestconfig, tmp_env):
    assert Path(pytestconfig.invocation_dir).joinpath("foo", "bar") == local_data_path(
        "foo", "bar"
    )


def test_package_data_path():
    assert MESSAGE_MODELS_PATH.joinpath("data", "foo", "bar") == package_data_path(
        "foo", "bar"
    )


@pytest.mark.xfail(
    condition=MESSAGE_DATA_PATH is None, reason="Requires message_data to be installed."
)
def test_private_data_path():
    assert MESSAGE_DATA_PATH.joinpath("data", "foo", "bar") == private_data_path(
        "foo", "bar"
    )
