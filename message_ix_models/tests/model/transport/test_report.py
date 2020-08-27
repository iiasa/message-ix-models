from message_data.model.transport.report import callback
from message_data.reporting import prepare_reporter, register


def test_register_cb():
    register(callback)


def test_report_bare(solved_bare_res_transport, session_context, tmp_path):
    """Run MESSAGE-Transport–specific reporting."""
    register(callback)

    session_context["output dir"] = tmp_path

    rep, key = prepare_reporter(
        solved_bare_res_transport,
        session_context.get_config_file("report", "global"),
        "out::transport",
    )

    # The key is added, can be computed and written to file
    path = tmp_path / "out::transport.xlsx"
    rep.write(key, path)
    # print(path)

    # The result has a specific aggregate calculated
    rep.get(key).sel(t="freight truck")

    # Get the catch-all key, including plots etc.
    rep.get("transport all")
