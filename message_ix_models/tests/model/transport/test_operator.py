from importlib.metadata import version
from typing import TYPE_CHECKING

import genno
import numpy.testing as npt
import pytest
from genno.testing import assert_qty_equal
from message_ix import Scenario
from numpy.testing import assert_allclose
from packaging.version import parse

from message_ix_models.model.transport import Config, factor
from message_ix_models.model.transport.operator import (
    broadcast_advance,
    distance_ldv,
    distance_nonldv,
    factor_input,
    factor_ssp,
    groups_iea_eweb,
    sales_fraction_annual,
    transport_check,
    uniform_in_dim,
)
from message_ix_models.model.transport.structure import get_technology_groups
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.project.ssp import SSP_2024

if TYPE_CHECKING:
    from message_ix_models import Context


@pytest.mark.xfail(reason="Incomplete")
def test_broadcast_advance():
    result = broadcast_advance()

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


@pytest.mark.xfail(reason="Pending updates to message-ix-models")
@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_distance_nonldv(test_context, regions):
    "Test :func:`.distance_nonldv`."
    test_context.model.regions = regions

    # Computation runs
    result = distance_nonldv(test_context)

    # Computed value has the expected dimensions and units
    assert {"nl", "t"} == set(result.dims)
    assert result.units.is_compatible_with("km / vehicle / year")

    # Check a computed value
    assert_qty_equal(
        genno.Quantity(32.7633, units="Mm / vehicle / year", name="non-ldv distance"),
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
def test_factor_input(test_context, options, any_change) -> None:
    cfg = Config.from_context(test_context, options=options)

    # Simulate inputs appearing in a Computer
    y = [2020, 2045, 2050, 2060, 2110]
    techs = cfg.spec.add.set["technology"]
    t_groups = get_technology_groups(cfg.spec)

    # Function runs
    result = factor_input(y, techs, dict(t=t_groups), dict(transport=cfg))

    # No change to 2020 values
    assert all(1.0 == result.sel(y=2020))  # type: ignore [arg-type]

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


@pytest.mark.parametrize("ssp", SSP_2024)
def test_factor_ssp(test_context, ssp: SSP_2024) -> None:
    cfg = Config.from_context(test_context, options=dict(ssp=ssp))

    # Simulate inputs appearing in a Computer
    n = ["R12_AFR", "R12_NAM"]
    y = [2020, 2025, 2030, 2050, 2100, 2110]
    config = dict(transport=cfg)

    layers = [
        factor.Constant(4.0, "n y"),
        factor.ScenarioSetting.of_enum(SSP_2024, "1=L 2=M 3=H 4=L 5=H", default="M"),
    ]

    # Function runs
    result = factor_ssp(config, n, y, info=factor.Factor(layers))

    assert {"n", "y"} == set(result.dims)


def test_groups_iea_eweb(test_context: "Context") -> None:
    cfg = Config.from_context(test_context)

    # Function runs
    groups_iea_eweb(cfg.spec.add.set["technology"])


@pytest.mark.skipif(
    parse(version("genno")) < parse("1.25.0"), reason="Requires genno >= 1.25.0"
)
@uniform_in_dim.minimum_version
def test_sales_fraction_annual():
    q = genno.Quantity(
        [[12.4, 6.1]], coords={"y": [2020], "n": list("AB")}, units="year"
    )

    result = sales_fraction_annual(q)

    # Result has same dimensions as input
    assert set(q.dims) == set(result.dims)
    # Results are bounded in y at the last period given in input
    assert 2020 == max(q.coords["y"])
    # Results sum to 1.0
    npt.assert_allclose([1.0, 1.0], result.sum(dim="y"))

    # Values for earlier periods are uniform…
    assert result.sel(n="A", y=1997).item() == result.sel(n="A", y=2020).item()
    assert result.sel(n="B", y=2009).item() == result.sel(n="B", y=2020).item()
    # Except the earliest period, containing the remainder of the distribution
    assert result.sel(n="A", y=1996).item() <= result.sel(n="A", y=2020).item()
    assert result.sel(n="B", y=2008).item() <= result.sel(n="B", y=2020).item()


@pytest.mark.xfail(reason="Incomplete test")
def test_transport_check(test_context):
    s = Scenario(test_context.get_platform(), model="m", scenario="s", version="new")

    transport_check(s, genno.Quantity())
