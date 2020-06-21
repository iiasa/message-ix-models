from message_data.model.transport.report import callback
from message_data.reporting import prepare_reporter, register
from message_data.tests import binary_data_available


def test_register_cb():
    register(callback)


@binary_data_available
def test_report_bare(solved_bare_res_transport, session_context):
    """Run MESSAGE-Transport–specific reporting."""
    register(callback)

    rep, key = prepare_reporter(
        solved_bare_res_transport,
        session_context.get_config_file('report', 'global'),
        'out::transport',
    )

    # The key is added, can be computed and retrieved, and has a specific
    # aggregate calculated
    rep.get(key).sel(t='freight truck')

    # Get the catch-all key, including plots etc.
    rep.get("transport")
