import re

from message_ix_models import Context
from message_ix_models.model.bmt.workflow import (
    add_macro,
    generate,
    prep_for_macro,
    report,
)


@generate.minimum_version
def test_generate(test_context: Context) -> None:
    # Workflow can be generated successfully
    wf = generate(test_context)

    # Get a text representation of what the workflow does
    result = wf.describe("BMT built")

    # print(result)  # DEBUG

    assert re.match(
        r"""'BMT built':
- <Step main\(\) -> MESSAGEix-GLOBIOM-GAINS 2\.1-BMT-R12/baseline_BMT>
- 'context':
  - <Context object at .* with \w+ keys>
- 'MT solved':
  - <Step solve\(\)>
  - 'context' \(above\)
  - 'MT built':
    - <Step _set_as_default\(\) -> MESSAGEix-GLOBIOM-GAINS 2\.1-BMT-R12/baseline_MT>
    - 'context' \(above\)
    - 'SSP2 T incu adjusted':
      - <Step <lambda>\(\)>
      - 'context' \(above\)
      - 'SSP2 T built':
        - <Step main\(\) -> MESSAGEix-GLOBIOM 1\.1-T-R12/SSP_2024\.2 baseline>
        - 'context' \(above\)
        - 'M reported'""",
        result,
    )


@generate.minimum_version
def test_generate_includes_macro_steps(test_context: Context) -> None:
    """Macro-prep and macro-report steps are present with expected actions."""
    wf = generate(test_context)

    for step_name in (
        "BMT reported",
        "BMTX prep macro",
        "BMTX baseline macro",
        "BMTX baseline macro reported",
    ):
        assert step_name in wf

    prep_task = wf.graph["BMTX prep macro"]
    prep_step = prep_task[0] if isinstance(prep_task, tuple) else prep_task
    assert prep_step.action is prep_for_macro

    macro_task = wf.graph["BMTX baseline macro"]
    macro_step = macro_task[0] if isinstance(macro_task, tuple) else macro_task
    assert macro_step.action is add_macro

    report_task = wf.graph["BMTX baseline macro reported"]
    report_step = report_task[0] if isinstance(report_task, tuple) else report_task
    assert report_step.action is report
