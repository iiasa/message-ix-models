import pytest

from message_ix_models import Context
from message_ix_models.project.scenariomip import workflow as w
from message_ix_models.project.scenariomip.workflow import generate_workflow


@pytest.mark.parametrize(
    "step_order",
    (
        None,
        [
            w.step_01,
            w.step_02,
            w.step_03,
            w.step_04,
            w.step_05,
            w.step_10,
            w.step_12,
            w.step_06,
            w.step_07,
            w.step_08,
            w.step_09,
            w.step_11,
            w.step_13,
            w.step_14,
            w.step_15,
            w.step_16,
        ],
    ),
)
def test_generate_workflow(test_context: Context, step_order) -> None:
    wf = generate_workflow(test_context, step_order)

    print(wf.describe("all"))

    # Fails, all steps NotImplemented
    wf.run("all")
