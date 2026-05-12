import re

from message_ix_models import Context
from message_ix_models.model.bmt.workflow import generate


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
