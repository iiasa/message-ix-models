import pytest
from genno import Quantity
from genno.testing import assert_qty_equal, assert_units
from message_ix import Scenario
from numpy.testing import assert_allclose

from message_data.model.transport import Config
from message_data.model.transport.operator import (
    advance_fv,
    distance_ldv,
    distance_nonldv,
    factor_input,
    iea_eei_fv,
    pdt_per_capita,
    transport_check,
)
from message_data.model.transport.util import get_techs
from message_data.projects.navigate import T35_POLICY


def test_advance_fv():
    result = advance_fv(dict(regions="R12"))

    assert ("n",) == result.dims
    # Results only for R12
    assert 12 == len(result.coords["n"])
    assert {"[mass]": 1, "[length]": 1} == result.units.dimensionality, result


@pytest.mark.skip(reason="Operator has been removed.")
@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_distance_ldv(test_context, regions):
    "Test :func:`.distance_ldv`."
    ctx = test_context
    ctx.model.regions = regions

    Config.from_context(ctx)

    # Fake reporting config from the context
    config = dict(transport=ctx.transport)

    # Computation runs
    result = distance_ldv(config)

    # Computed value has the expected dimensions
    assert ("nl", "driver_type") == result.dims

    # Check some computed values
    assert_allclose(
        [13930, 45550],
        result.sel(nl=f"{regions}_NAM", driver_type=["M", "F"]),
        rtol=2e-4,
    )


@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_distance_nonldv(regions):
    "Test :func:`.distance_nonldv`."
    # Configuration
    config = dict(regions=regions)

    # Computation runs
    result = distance_nonldv(config)

    # Computed value has the expected dimensions and units
    assert ("nl", "t") == result.dims
    assert result.units.is_compatible_with("km / vehicle / year")

    # Check a computed value
    assert_qty_equal(
        Quantity(32.7633, units="Mm / vehicle / year", name="non-ldv distance"),
        result.sel(nl=f"{regions}_EEU", t="BUS", drop=True),
    )


@pytest.mark.parametrize(
    "options, any_change",
    (
        ({}, False),
        (dict(navigate_scenario=T35_POLICY.TEC), True),
        (dict(navigate_scenario=T35_POLICY.ACT), False),
        (dict(navigate_scenario=T35_POLICY.ALL), True),  # i.e. all
    ),
)
def test_factor_input(test_context, options, any_change):
    cfg = Config.from_context(test_context, options=options)

    # Simulate inputs appearing in a Computer
    y = [2020, 2045, 2050, 2060, 2110]
    spec, techs, t_groups = get_techs(test_context)

    # Function runs
    result = factor_input(y, techs, dict(t=t_groups), dict(transport=cfg))

    # No change to 2020 values
    assert all(1.0 == result.sel(y=2020))

    # Check intermediate values
    k = 5 if any_change else 0
    t = "ELC_100"
    assert all((1 - 0.015) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))
    t = "FR_ICE_L"
    assert all((1 - 0.02) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))
    t = "con_ar"
    assert all((1 - 0.013) ** k == result.sel(y=2050, t=t) / result.sel(y=2045, t=t))

    # No change after 2050
    assert all(1.0 == result.sel(y=2060) / result.sel(y=2050))
    assert all(1.0 == result.sel(y=2110) / result.sel(y=2050))


def test_iea_eei_fv():
    # TODO expand with additional cases
    result = iea_eei_fv("tonne-kilometres", dict(regions="R12"))

    assert 12 == len(result)


DATA = """
n        y     value
R11_AFR  2020  0
R11_AFR  2050  100
R11_AFR  2100  200
R11_WEU  2020  100
R11_WEU  2050  200
R11_WEU  2100  300
"""


def test_pdt_per_capita():
    """Test :func:`.pdt_per_capita`."""
    from io import StringIO

    import pandas as pd

    # Input data: GDP (PPP, per capita)
    gdp_ppp_cap = Quantity(
        pd.read_fwf(StringIO(DATA)).astype({"y": int}).set_index(["n", "y"])["value"],
        units="kUSD / passenger / year",
    )
    # PDT: reference value for the bas period
    pdt_ref = Quantity(
        pd.Series({"R11_AFR": 10000.0, "R11_WEU": 20000.0}).rename_axis("n"),
        units="km / year",
    )
    # Configuration: defaults
    config = dict(transport=Config())

    result = pdt_per_capita(gdp_ppp_cap, pdt_ref, 2020, config)
    # print(f"{result = }")

    # Data have the expected dimensions and shape
    assert {"n", "y"} == set(result.dims)
    assert gdp_ppp_cap.shape == result.shape
    # Data have the expected units
    assert_units(result, "km / year")


@pytest.mark.xfail(reason="Incomplete test")
def test_transport_check(test_context):
    s = Scenario(test_context.get_platform(), model="m", scenario="s", version="new")

    transport_check(s, Quantity())
