import re

import pandas as pd
import pytest
import xarray as xr
from genno import Computer, Quantity

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.report.computations import (
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


@pytest.mark.xfail(reason="Incomplete")
def test_from_url():
    from_url()


@pytest.mark.xfail(reason="Incomplete")
def test_get_ts():
    get_ts()


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
def test_remove_ts():
    remove_ts()


@pytest.mark.xfail(reason="Incomplete")
def test_share_curtailment():
    share_curtailment()
