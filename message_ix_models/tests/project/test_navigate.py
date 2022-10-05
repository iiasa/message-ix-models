import re


def test_generate_workflow(mix_models_cli):
    """Test :func:`.navigate.workflow.generate` and associated CLI."""

    result = mix_models_cli.invoke(
        [
            "navigate",
            "--scenario=NPi-act",
            "run",
            "--from=M built",
            "--dry-run",
            "report",
        ]
    )

    # Workflow has the expected scenarios in it
    assert re.match(
        r""".* Execute workflow:
'report':
- <Step report\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'BMT solved':
  - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-act>
  - 'context' \(above\)
  - 'MT solved':
    - <Step solve\(\)>
    - 'context' \(above\)
    - 'MT built':
      - <Step build_transport\(\) -> MESSAGEix-GLOBIOM 1.1-MT-R12 \(NAVIGATE\)/NPi-act>
      - 'context' \(above\)
      - 'M built':
        - <Step load -> MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE>
        - 'context' \(above\)
        - None""",
        result.output,
    ), result.output
