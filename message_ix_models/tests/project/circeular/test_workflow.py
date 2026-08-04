from message_ix_models import Context
from message_ix_models.project.circeular.workflow import generate


def test_generate(test_context: Context) -> None:
    wf = generate(test_context)

    # Workflow contains expected step names
    for x in "RCNSAE":
        assert f"{x} BMTX baseline macro reported" in wf

    # print(wf.describe("A BMTX baseline macro reported"))
