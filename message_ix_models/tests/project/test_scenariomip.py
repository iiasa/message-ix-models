import pytest

from message_ix_models import Context
from message_ix_models.project.scenariomip import workflow as w
from message_ix_models.project.scenariomip.workflow import generate
from message_ix_models.testing import bare_res


@pytest.mark.parametrize(
    "step_order, last_log",
    (
        # Default steps
        (None, "Not implemented: step_16(…)"),
        # Some other order
        ([w.step_16, w.step_01], "Not implemented: step_01(…)"),
    ),
)
def test_generate(
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
    test_context: Context,
    step_order: list | None,
    last_log: str,
) -> None:
    # Create a empty RES scenario for testing
    s = bare_res(request, test_context)

    # Set the workflow to start from this scenario
    test_context.set_scenario(s)

    # Function runs, returns a workflow
    wf = generate(test_context, step_order)

    # Workflow runs, does nothing
    wf.run("all")

    # Final log message is as expected
    assert last_log == caplog.messages[-1]
