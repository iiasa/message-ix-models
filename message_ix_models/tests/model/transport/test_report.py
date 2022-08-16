import logging

import pytest
from genno.testing import assert_qty_equal
from message_ix.reporting import MissingKeyError, Quantity
from message_ix_models.testing import NIE
from numpy.testing import assert_allclose
from pytest import mark, param

from message_data.model.transport import computations, configure
from message_data.model.transport.report import PLOTS, callback  # noqa: F401
from message_data.model.transport.testing import (
    MARK,
    built_transport,
    simulated_solution,
)
from message_data.reporting import prepare_reporter, register

log = logging.getLogger(__name__)


def test_register_cb():
    register(callback)


@pytest.mark.parametrize(
    "regions, years, solved",
    (
        param(
            "R11",
            "A",
            False,
            marks=[
                MARK[1],
                pytest.mark.xfail(
                    raises=MissingKeyError,
                    reason="required key 'ACT:nl-t-yv-va-m-h' not defined w/o solution",
                ),
            ],
        ),
        param("R11", "A", True, marks=MARK[1]),
        ("R12", "A", True),
        param("R14", "A", True, marks=MARK[0]),
        param("ISR", "A", True, marks=NIE),
    ),
)
def test_report_bare(request, test_context, tmp_path, regions, years, solved):
    """Run MESSAGEix-Transport–specific reporting."""
    register(callback)

    ctx = test_context
    ctx.update(
        regions=regions,
        years=years,
        report=dict(
            config="global.yaml",
            # key="transport all",
            key="stock:nl-t-ya-driver_type:ldv",
            output_dir=tmp_path,
        ),
    )
    ctx["output dir"] = tmp_path

    scenario = built_transport(request, ctx, solved=solved)

    # commented: for debugging
    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    rep, key = prepare_reporter(test_context, scenario)

    # Get the catch-all key, including plots etc.
    rep.get(key)


@pytest.mark.parametrize("regions", ["R11", "R12"])
def test_distance_ldv(test_context, regions):
    "Test :func:`.computations.distance_ldv`."
    ctx = test_context
    ctx.regions = regions

    configure(ctx)

    # Fake reporting config from the context
    config = dict(transport=ctx["transport config"])

    # Computation runs
    result = computations.distance_ldv(config)

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
    "Test :func:`.computations.ldv_distance`."
    # Configuration
    config = dict(transport=dict(regions=regions))

    # Computation runs
    result = computations.distance_nonldv(config)

    # Computed value has the expected dimensions and units
    assert ("nl", "t") == result.dims
    assert result.units.is_compatible_with("km / vehicle / year")

    # Check a computed value
    assert_qty_equal(
        Quantity(32.7633, units="Mm / vehicle / year"),
        result.sel(nl=f"{regions}_EEU", t="BUS", drop=True),
    )


@pytest.fixture
def quiet_genno(caplog):
    """Quiet some log messages from genno via by :func:`.reporting.prepare_reporter`."""
    caplog.set_level(logging.WARNING, logger="genno.config")
    caplog.set_level(logging.WARNING, logger="genno.compat.pyam")


@mark.usefixtures("quiet_genno")
def test_simulated_solution(request, test_context, regions="R12", years="B"):
    # The message_data.reporting.prepare_reporter works on the simulated data
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context)

    # A quantity for a MESSAGEix variable was added and can be retrieved
    k = rep.full_key("ACT")
    rep.get(k)

    # A quantity for MESSAGEix can be computed
    k = rep.full_key("out")
    rep.get(k)

    # A quantity for message_data.model.transport can be computed
    k = "stock:nl-t-ya-driver_type:ldv"
    result = rep.get(k)
    assert 0 < len(result)


@mark.usefixtures("quiet_genno")
@pytest.mark.parametrize(
    "plot_name",
    # # All plots
    # list(PLOTS.keys()),
    # Only a subset
    [
        # "energy-by-cmdty",
        "stock-ldv",
        # "stock-non-ldv",
    ],
)
def test_plot_simulated(request, test_context, plot_name, regions="R12", years="B"):
    """Plots are generated correctly using simulated data."""
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context)

    # print(rep.describe(f"plot {plot_name}"))  # DEBUG

    # Succeeds
    rep.get(f"plot {plot_name}")


@mark.usefixtures("quiet_genno")
def test_iamc_simulated(
    request, tmp_path_factory, test_context, regions="R12", years="B"
):
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context)

    # Key collecting both file output/scenario update
    # NB the trailing colons are necessary because of how genno handles report.yaml
    rep.add("test", ["transport iamc file:", "transport iamc store:"])

    # print(rep.describe("transport iamc store"))  # DEBUG
    # print(rep.describe("scenario"))  # DEBUG
    # print(rep.describe("test"))  # DEBUG

    rep.get("test")

    # File with output was created
    assert (
        tmp_path_factory.getbasetemp()
        .joinpath("data0", "report", "transport.csv")
        .exists()
    )

    # Retrieve time series data stored on the scenario object
    ts = rep.get("scenario").timeseries()
    # print(ts)  # DEBUG

    # The reported variables were stored
    assert {"Transport|Stock|Road|Passenger|LDV|Elc_100"} <= set(
        ts["variable"].unique()
    )
