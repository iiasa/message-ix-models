import pytest

from message_data.model.transport.report import callback
from message_data.reporting import prepare_reporter, register

from . import built_transport


def test_register_cb():
    register(callback)


@pytest.mark.skip("Very slow")  # TODO debug this
@pytest.mark.parametrize("regions", ["R11"])
def test_report_bare(request, transport_context_f, tmp_path, regions):
    """Run MESSAGE-Transport–specific reporting."""
    register(callback)

    ctx = transport_context_f
    ctx["output dir"] = tmp_path
    ctx.regions = regions

    scenario = built_transport(request, ctx, solved=True)

    rep, key = prepare_reporter(
        scenario, ctx.get_config_file("report", "global"), "out::transport"
    )

    # The key is added, can be computed and written to file
    path = tmp_path / "out__transport.xlsx"
    rep.write(key, path)

    # commented: for debugging
    # print(path)

    # out::transport contains a specific, defined aggregate
    rep.get(key).sel(t="freight truck")

    # in::transport can be reported to file
    key = rep.full_key("in::transport")
    rep.write(key, tmp_path / "in__transport.xlsx")

    # Get the catch-all key, including plots etc.
    rep.get("transport all")
