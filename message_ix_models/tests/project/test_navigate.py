import re

from message_data.projects.navigate import workflow


def test_generate_workflow(test_context):
    # Code runs
    wf = workflow.generate(test_context)

    # Workflow has the expected scenarios in it
    assert re.match(
        r"""'FINAL':
- <Step <function report at [^>]*>>
- 'MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/baseline':
  - <Step <function build_buildings at [^>]*>>
  - 'MESSAGEix-GLOBIOM 1.1-MT-R12 \(NAVIGATE\)/baseline':
    - <Step <function build_transport at [^>]*>>
    - 'MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE_test':
      - <Step <function build_materials at [^>]*>>
      - 'Base/base':
        - <Step <function base_scenario at [^>]*>>
        - None""",
        wf._computer.describe("FINAL"),
    )
