import pytest

from message_ix_models.util import private_data_path

from message_data.model.transport.report import callback
from message_data.reporting import prepare_reporter, register
from message_data.testing import NIE

from . import built_transport


def test_register_cb():
    register(callback)


@pytest.mark.parametrize(
    "regions, solved",
    (
        pytest.param("R11", True),
        # FIXME
        pytest.param(
            "R11", False, marks=pytest.mark.skip(reason="Interference from other tests")
        ),
        pytest.param("R14", True, marks=NIE),
        pytest.param("ISR", True, marks=NIE),
    ),
)
def test_report_bare(request, transport_context_f, tmp_path, regions, solved):
    """Run MESSAGEix-Transport–specific reporting."""
    register(callback)

    ctx = transport_context_f
    ctx["output dir"] = tmp_path
    ctx.regions = regions

    scenario = built_transport(request, ctx, solved=solved)

    rep, key = prepare_reporter(
        scenario, private_data_path("report", "global.yaml"), "transport all"
    )
    rep.configure(output_dir=tmp_path)

    # Get the catch-all key, including plots etc.
    rep.get(key)
