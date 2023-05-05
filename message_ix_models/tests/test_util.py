"""Tests of :mod:`message_ix_models.util`."""
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from iam_units import registry
from ixmp.testing import assert_logs
from message_ix import Scenario, make_df
from message_ix.testing import make_dantzig
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
    replace_par_data,
    same_node,
    same_time,
    series_of_pint_quantity,
    strip_par_data,
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
    # Base data frame to be broadcast, with 2 rows and dimensions:
    # - a: length 2
    # - b, c, d: missing
    N_a = 2
    base = pd.DataFrame([["a0", 1.2], ["a1", 3.4]], columns=["a", "value"]).assign(
        b=None, c=None, d=None
    )

    # broadcast works with DataFrame.pipe(), using keyword arguments
    result = base.pipe(
        broadcast, b="b0 b1 b2".split(), c="c0 c1 c2 c3".split(), d=["d0"]
    )
    # Results have the expected length: original × cartesian product of 3, 4, and 1
    assert N_a * 3 * 4 * 1 == len(result)
    # Resulting array is completely full, no missing labels
    assert not result.isna().any(axis=None)

    # Length zero labels for one dimension—debug message is logged
    with caplog.at_level(logging.DEBUG, logger="message_ix_models"):
        result = base.pipe(broadcast, b="b0 b1".split(), c="c0 c1".split(), d=[])

    # Debug message is logged
    assert "Don't broadcast over 'd'; labels [] have length 0" in caplog.messages
    caplog.clear()
    assert N_a * 2 * 2 * 1 == len(result)  # Expected length
    assert result["d"].isna().all()  # Dimension d remains empty
    assert not result.drop("d", axis=1).isna().any(axis=None)  # Others completely full

    # Using a DataFrame as the first/only positional argument, plus keyword arguments
    labels = pd.DataFrame(dict(b="b0 b1 b2".split(), c="c0 c1 c2".split()))

    result = base.pipe(broadcast, labels, d="d0 d1".split())
    assert N_a * 3 * 2 == len(result)  # (b, c) dimensions linked with 3 pairs of labels
    assert not result.isna().any(axis=None)  # Completely full

    # Using a positional argument with only 1 column
    result = base.pipe(broadcast, labels[["b"]], c="c0 c1 c2 c3".split(), d=["d0"])
    assert N_a * 3 * 4 * 1 == len(result)  # Expected length
    assert not result.isna().any(axis=None)  # Completely full

    # Overlap between columns in the positional argument and keywords
    with pytest.raises(ValueError):
        result = base.pipe(broadcast, labels, c="c0 c1 c2 c3".split(), d=["d0"])

    # Extra, invalid dimensions result in ValueError
    with pytest.raises(ValueError):
        base.pipe(broadcast, b="b0 b1 b2".split(), c="c0 c1 c2 c3".split(), e=["e0"])

    labels["e"] = "e0 e1 e2".split()

    with pytest.raises(ValueError):
        base.pipe(broadcast, labels, d=["d0"])


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
    with pytest.raises(KeyError, match="baz"):
        check_support(test_context, dict(baz=["baz"]), "Baz is not set")

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
    assert all(
        np.isclose(a, b, atol=1e-4 * registry.kg)
        for a, b in zip(exp.values, result.values)
    )

    # With store="magnitude", a series of floats
    exp = pd.Series([q.magnitude for q in exp.values], name="bar")
    assert_series_equal(exp, convert_units(*args, store="magnitude"), check_dtype=False)

    # Other values for store= are errors
    with pytest.raises(ValueError, match="store = 'foo'"):
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
        assert not df.isna().any(axis=None)

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


def test_local_data_path(tmp_path_factory, session_context):
    assert tmp_path_factory.getbasetemp().joinpath(
        "data0", "foo", "bar"
    ) == local_data_path("foo", "bar")


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


@pytest.mark.parametrize(
    "name, func, col", [("node", same_node, "node_loc"), ("time", same_time, "time")]
)
def test_same(name, func, col):
    """Test both :func:`.same_node` and :func:`.same_time`."""
    df_in = pd.DataFrame(
        {
            col: ["foo", "bar", "baz"],
            f"{name}_dest": None,
            f"{name}_origin": None,
            "value": [1.1, 2.2, 3.3],
        }
    )

    df_out = func(df_in)

    assert not df_out.isna().any(axis=None)
    assert_series_equal(df_out[f"{name}_dest"], df_in[col], check_names=False)
    assert_series_equal(df_out[f"{name}_origin"], df_in[col], check_names=False)


def test_replace_par_data(caplog, test_context):
    """Test :func:`.replace_par_data`."""
    # Generate a scenario. This scenario has 3 data points in each of "input" and
    # "output" with technology="transport_from_seattle".
    s = make_dantzig(test_context.get_platform())

    # Arguments to replace_par_data()
    parameters = ["input", "output"]
    filters = dict(mode=["to_chicago", "to_topeka"])
    to_replace = dict(technology={"transport_from_seattle": "tfs"})

    with s.transact("Add a new set element, to which values will be renamed"):
        s.add_set("technology", "tfs")

    # Function runs
    replace_par_data(s, parameters, filters=filters, to_replace=to_replace)

    for data in map(lambda n: s.par(n, filters=dict(node_loc="seattle")), parameters):
        # Data points selected by `filters` have been relabeled
        assert 2 == len(data.query("technology == 'tfs'"))

        # Data points not selected by `filters` are not affected
        assert 1 == len(data.query("technology == 'transport_from_seattle'"))


def test_strip_par_data(caplog, test_context):
    """Test the "dry run" feature of :func:`.strip_par_data`."""
    s = make_dantzig(test_context.get_platform())

    N = len(s.par("output"))
    strip_par_data(s, "technology", "canning_plant", dry_run=True, dump=dict())

    assert_logs(
        caplog,
        [
            "Remove data with technology='canning_plant' (DRY RUN)",
            "2 rows in 'output'",
            "with commodity=['cases']",
            "with level=['supply']",
        ],
    )
    # Nothing was actually removed
    assert N == len(s.par("output"))
