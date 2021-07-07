import logging
import os

import pytest
from numpy.testing import assert_allclose

from message_ix_models.util import private_data_path

from message_data.model.transport import read_config
from message_data.model.transport.report import callback, computations
from message_data.reporting import prepare_reporter, register
from message_data.testing import NIE

from . import built_transport

log = logging.getLogger(__name__)


def test_register_cb():
    register(callback)


@pytest.mark.skipif(
    "TEAMCITY_BUILD_TRIGGERED_BY" in os.environ,
    reason="Temporary (undiagnosed failures on TeamCity; passes locally)",
)
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

    read_config(ctx)

    scenario = built_transport(request, ctx, solved=solved)

    dump_path = tmp_path / "scenario.xlsx"
    log.info(f"Dump contents to {dump_path}")
    scenario.to_excel(dump_path)

    rep, key = prepare_reporter(
        scenario,
        private_data_path("report", "global.yaml"),
        "transport all",
    )
    rep.configure(output_dir=tmp_path)

    # Get the catch-all key, including plots etc.
    rep.get(key)


def test_ldv_distance(test_context):
    "Test the :func:`ldv_distance()` computation."
    ctx = test_context

    # Computation runs
    result = computations.ldv_distance(ctx["transport config"])

    # Computed value has the expected dimensions
    assert ("nl", "driver_type") == result.dims

    # Check some computed values
    assert_allclose(
        [13930, 45550], result.sel(nl="R11_NAM", driver_type=["M", "F"]), rtol=2e-4
    )
