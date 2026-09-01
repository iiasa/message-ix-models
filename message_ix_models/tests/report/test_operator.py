import os
import re
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path

import ixmp
import message_ix
import pandas as pd
import pandas.testing as pdt
import pyam
import pytest
import xarray as xr
from genno import Computer, Quantity
from ixmp.testing import assert_logs
from message_ix.testing import make_dantzig

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.report.operator import (
    compound_growth,
    filter_ts,
    from_url,
    full,
    get_ts,
    gwp_factors,
    latest_reporting,
    make_output_path,
    model_periods,
    remove_ts,
    remove_zeros,
    share_curtailment,
    summarize,
)


@pytest.fixture
def c() -> Computer:
    return Computer()


@pytest.fixture
def scenario(test_context):
    mp = test_context.get_platform()
    yield make_dantzig(mp)


def test_compound_growth():
    """:func:`.compound_growth` on a 2-D quantity."""
    qty = Quantity(
        xr.DataArray(
            [
                [1.01, 1.0, 1.02, 1e6],  # Varying growth rates for x=x1
                [1.0, 1.0, 1.0, 1.0],  # No rates/constant for x=x2
            ],
            coords=(["x1", "x2"], [2020, 2021, 2030, 2035]),
            dims=("x", "t"),
        )
    )

    # Function runs
    result = compound_growth(qty, "t")

    # Results have expected values
    r1 = result.sel(x="x1")
    assert all(1.0 == r1.sel(t=2020))
    assert all(1.01 == r1.sel(t=2021) / r1.sel(t=2020))
    assert all(1.0 == r1.sel(t=2030) / r1.sel(t=2021))
    assert all(1.02**5 == r1.sel(t=2035) / r1.sel(t=2030))

    assert all(1.0 == result.sel(x="x2"))


def test_filter_ts():
    df = pd.DataFrame([["foo"], ["bar"]], columns=["variable"])
    assert 2 == len(df)

    # Operator runs
    result = filter_ts(df, re.compile(".(ar)"))

    # Only matching rows are returned
    assert 1 == len(result)

    # Only the first match group in `expr` is preserved
    assert {"ar"} == set(result.variable.unique())


def test_from_url(scenario):
    full_url = f"ixmp://{scenario.platform.name}/{scenario.url}"

    # Operator runs
    result = from_url(full_url)
    # Result is of the default class
    assert result.__class__ is ixmp.TimeSeries
    # Same object was retrieved
    assert scenario.url == result.url

    # Same, but specifying message_ix.Scenario
    result = from_url(full_url, message_ix.Scenario)
    assert result.__class__ is message_ix.Scenario
    assert scenario.url == result.url


@pytest.mark.parametrize(
    "coords, dims, exp_len",
    (
        ([], (), 1),  # No dimensions
        ([["x1", "x2"]], ("x",), 2),  # 1-D
        ([["x1", "x2"], ["y1", "y2", "y3"]], ("x", "y"), 6),  # 2-D
        pytest.param(  # Too few coords
            [["x1", "x2"]],
            ("x", "y"),
            0,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(  # Too few IDs
            [["x1", "x2"], ["y1", "y2", "y3"]],
            ("x",),
            0,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ),
)
def test_full(coords: list, dims: tuple, exp_len: int) -> None:
    result = full(*coords, dims=dims)

    assert exp_len == len(result)


def test_get_remove_ts(caplog, scenario):
    # get_ts() runs
    result0 = get_ts(scenario)
    pdt.assert_frame_equal(scenario.timeseries(), result0)

    # Can be used through a Computer

    c = Computer()
    c.require_compat("message_ix_models.report.operator")
    c.add("scenario", scenario)

    key = c.add("test1", "get_ts", "scenario", filters=dict(variable="GDP"))
    result1 = c.get(key)
    assert 3 == len(result1)

    # remove_ts() can be used through Computer
    key = c.add("test2", "remove_ts", "scenario", "config", after=1964)

    # Task runs, logs
    # NB this log message is incorrect, because ixmp's JDBCBackend is unable to delete
    #    data stored with "meta=True". Only 1 row is removed
    with assert_logs(caplog, "Remove 2 of 6 (1964 <= year) rows of time series data"):
        c.get(key)

    # See comment above; only one row is removed
    assert 6 - 1 == len(scenario.timeseries())

    # remove_ts() can be used directly
    remove_ts(scenario)

    # All non-'meta' data were removed
    assert 3 == len(scenario.timeseries())


def test_gwp_factors():
    result = gwp_factors()

    assert ("gwp metric", "e", "e equivalent") == result.dims


@pytest.fixture(scope="module")
def context_with_reporting_data(session_context: Context) -> Iterator[Context]:
    context = deepcopy(session_context)

    # Common time series data
    data = pd.DataFrame(
        [["m", "s", "", "DantzigLand", "u", 1234, 1.0]],
        columns=["Model", "Scenario", "Variable", "Region", "Unit", "Year", "Value"],
    )

    # Data for latest_reporting_from_file()
    base_dir = context.get_local_path("report")
    for v in (3, 4):
        path = base_dir.joinpath(f"test_latest_reporting_s_v{v}", "all.csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        data.assign(Variable=f"Variable|File {v}", value=float(v)).to_csv(
            path, index=False
        )
    # A dummy/empty directory
    base_dir.joinpath("test_latest_reporting_s_v99").mkdir(exist_ok=True)

    # Data for latest_reporting_from_platform()
    mp = context.get_platform()
    s = make_dantzig(mp, solve=True)
    for v in (1, 2):
        s_clone = s.clone(model="test_latest_reporting", scenario="s")
        assert v == s_clone.version
        s_clone.check_out(timeseries_only=True)
        s_clone.add_timeseries(
            data.assign(Variable=f"Variable|Platform {v}", value=float(v))
        )
        s_clone.commit("")
    # A clone, but with no solution
    s_clone = s.clone(model="test_latest_reporting", scenario="s", keep_solution=False)

    try:
        yield context
    finally:
        context.delete()


@pytest.mark.parametrize(
    "use, variable",
    [
        ({"file"}, "Variable|File 4"),
        ({"platform"}, "Variable|Platform 2"),
        ({"file", "platform"}, "Variable|File 4"),  # Version 4 from file > 2 from mp
    ],
)
def test_latest_reporting(
    caplog: pytest.LogCaptureFixture,
    context_with_reporting_data: Context,
    use: set[str],
    variable: str,
) -> None:
    ctx = context_with_reporting_data

    # With missing scenario identifiers
    info = ScenarioInfo()
    # Function runs
    result = latest_reporting(ctx, info, use=use)
    # Returns None
    assert None is result
    # Message is logged about unavailable data
    assert "None/None → NO DATA" == caplog.messages[0]

    # With existing data
    info = ScenarioInfo(model="test_latest_reporting", scenario="s")

    # Operator returns a data frame
    result = latest_reporting(ctx, info, use=use)
    assert result is not None

    # Data frame is from the expected source and version
    assert variable in result.variable.unique()


def test_make_output_path(tmp_path, c):
    # Configure a Computer, ensuring the output_dir configuration attribute is set
    c.configure(output_dir=tmp_path)

    # Add a computation that invokes make_output_path
    c.add("test", make_output_path, "config", "foo.csv")

    # Returns the correct path
    assert tmp_path.joinpath("foo.csv") == c.get("test")


def test_model_periods():
    # Prepare input data
    si = ScenarioInfo()
    si.year_from_codes(get_codes("year/B"))
    cat_year = pd.DataFrame(si.set["cat_year"], columns=["type_year", "year"])

    # Operator runs
    result = model_periods(si.set["year"], cat_year)

    assert isinstance(result, list)
    assert all(isinstance(y, int) for y in result)
    assert 2020 == min(result)


def test_remove_zeros() -> None:
    import numpy as np

    # Test data:
    # - Mix of 0.0, NaN, and non-zero for year 2019.
    # - 0.0 for years 2020 amd 2021.
    idx = pd.MultiIndex.from_product(
        [["v1", "v2", "v3"], [2019, 2020, 2021]], names=("variable", "year")
    )
    data = (
        pd.Series([0.0, 0.0, 0.0, np.nan, 0, 0, 1.0, 0.0, 0.0], index=idx, name="value")
        .reset_index()
        .assign(model="m", scenario="s", region="r", unit="")
    )

    # Function runs with pd.DataFrame input
    result = remove_zeros(data, 2020)

    # Number of data points removed
    assert 1 == len(data) - len(result)
    # Data points removed are for years < `year` argument
    assert 0 == len(result.query("variable == 'v1' and year == 2019"))

    # Function runs with pyam.IamDataFrame input
    data1 = pyam.IamDataFrame(data)
    result1 = remove_zeros(data1, 2020)

    # Number of data points removed
    assert 1 == len(data1) - len(result1)
    # Data points removed are for years < `year` argument
    assert 0 == len(result1.as_pandas().query("variable == 'v1' and year == 2019"))  # type: ignore [arg-type]


@pytest.mark.xfail(reason="Incomplete")
def test_share_curtailment():
    share_curtailment()


_p = Path("/example/base/dir")

# Arguments to summarize(); example of the "transport all" report key as of 2025-11-21
SUMMARIZE_ARGS = [
    [
        _p / "base-fe-intensity-gdp.pdf",
        _p / "inv-cost-ldv.pdf",
        _p / "demand.pdf",
        _p / "demand" / "share.pdf",
        None,
        _p / "energy" / "c.pdf",
        _p / "energy" / "c-share.pdf",
        _p / "stock" / "ldv.pdf",
        _p / "stock" / "non-ldv.pdf",
    ],
    [[None, None], None],
    _p / "sdmx",
    [None, None, None, None, None, None, None, None, None, None, None, None],
]


@pytest.mark.parametrize(
    "args",
    (
        (SUMMARIZE_ARGS,),  # Single list arg to summarize() containing further items
        SUMMARIZE_ARGS,  # Multiple positional args to summarize()
    ),
)
def test_summarize(args) -> None:
    # Operator runs
    result = summarize(*args)

    # Results as expected
    assert re.fullmatch(
        rf""" 9 file or directory paths:
    4 in ({re.escape(os.sep)})example\1base\1dir\1
    1 in \1example\1base\1dir\1demand\1
    2 in \1example\1base\1dir\1energy\1
    2 in \1example\1base\1dir\1stock\1
16 × None
 0 other items""",
        result,
    )
