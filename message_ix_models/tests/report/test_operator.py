import re

import ixmp
import message_ix
import pandas as pd
import pandas.testing as pdt
import pytest
import xarray as xr
from genno import Computer, Quantity
from ixmp.testing import assert_logs
from message_ix.testing import make_dantzig

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.report.operator import (
    compound_growth,
    filter_ts,
    from_url,
    get_ts,
    gwp_factors,
    make_output_path,
    model_periods,
    remove_ts,
    share_curtailment,
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


@pytest.mark.xfail(reason="Incomplete")
def test_share_curtailment():
    share_curtailment()
