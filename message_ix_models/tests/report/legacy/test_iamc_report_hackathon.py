import pytest
from message_ix_models import testing
from message_ix_models.util import MESSAGE_DATA_PATH

from message_data.tools.post_processing import iamc_report_hackathon


@pytest.mark.parametrize("set_out_dir", [False, True])
def test_report(caplog, request, test_context, set_out_dir):
    """Invoke :func:`.iamc_report_hackathon.report` directly."""
    # Mandatory arguments to report(): a scenario and its platform
    scenario = testing.bare_res(request, test_context, solved=False)

    # Set dry_run = True to not actually perform any calculations or modifications
    test_context.dry_run = True

    if set_out_dir:
        expected = test_context.get_local_path("report", "foo")
        args = dict(out_dir=expected)
    else:
        # Do not provide out_dir/no arguments
        args = dict()
        expected = MESSAGE_DATA_PATH.joinpath("reporting_output")

    # Call succeeds
    iamc_report_hackathon.report(
        scenario.platform, scenario, context=test_context, **args
    )

    # Dry-run message is logged
    assert "DRY RUN" in caplog.messages[-1]
    # Output directory is set
    assert caplog.messages[-1].endswith(str(expected))
