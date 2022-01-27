import logging

import pytest
from numpy.testing import assert_allclose

from message_ix_models.util import private_data_path
from message_ix_models.testing import NIE

from message_data.model.transport import configure
from message_data.model.transport.report import callback, computations
from message_data.reporting import prepare_reporter, register

from . import built_transport

log = logging.getLogger(__name__)


def test_register_cb():
    register(callback)


@pytest.mark.parametrize(
    "regions, years, solved",
    (
        pytest.param("R11", "A", False),
        pytest.param("R11", "A", True),
        pytest.param("R14", "A", True, marks=NIE),
        pytest.param("ISR", "A", True, marks=NIE),
    ),
)
def test_report_bare(request, test_context, tmp_path, regions, years, solved):
    """Run MESSAGEix-Transport–specific reporting."""
    register(callback)

    ctx = test_context
    ctx.regions = regions
    ctx.years = years
    ctx["output dir"] = tmp_path

    scenario = built_transport(request, ctx, solved=solved)

    # commented: for debugging
    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    rep, key = prepare_reporter(
        scenario,
        private_data_path("report", "global.yaml"),
        # "transport all",
        "stock:nl-t-ya-driver_type:ldv",
    )
    rep.configure(output_dir=tmp_path)

    # Get the catch-all key, including plots etc.
    rep.get(key)


@pytest.mark.parametrize("regions", ["R11"])
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
        [13930, 45550], result.sel(nl="R11_NAM", driver_type=["M", "F"]), rtol=2e-4
    )


@pytest.mark.xfail(reason="Under development")
@pytest.mark.parametrize("regions", ["R11"])
def test_distance_nonldv(regions):
    "Test :func:`.computations.ldv_distance`."
    # Configuration
    config = dict(transport=dict(regions=regions))

    # Computation runs
    result = computations.distance_nonldv(config)

    # Computed value has the expected dimensions
    assert ("nl", "t", "y") == result.dims

    # TODO Check some computed values
