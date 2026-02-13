import re

from message_ix_models import Context
from message_ix_models.model.bmt.workflow import generate


def test_generate(test_context: Context) -> None:
    # Workflow can be generated successfully
    wf = generate(test_context)

    # Get a text representation of what the workflow does
    result = wf.describe("BMT built")

    # print(result)  # DEBUG

    assert re.match(
        r"""'BMT built':
- <Step load -> MESSAGEix-GLOBIOM 2.2-BMT-R12/baseline_BMT>
- 'context':
  - <Context object at .* with \w+ keys>
- 'M SSP2 T incu adjusted':
  - <Step <lambda>\(\)>
  - 'context' \(above\)
  - 'M SSP2 T built':
    - <Step main\(\) -> MESSAGEix-GLOBIOM 1.1-MT-R12/SSP_2024.2 baseline>
    - 'context' \(above\)
    - 'BM reported'""",
        result,
    )
