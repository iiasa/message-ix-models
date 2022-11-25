import re
from pathlib import Path

import ixmp

# Chunks of text to look for in the --dry-run output. The text cannot be matched exactly
# because the order of traversing the graph is non-deterministic, i.e. which step
# displays "MT solved" and its subtree may vary.
context = r"'context' \(above\)"
BLOCKS = [
    "Truncate workflow at 'M built'",
    rf"""
    - 'MT solved':
      - <Step solve\(\)>
      - {context}
      - 'MT built':
        - <Step build_transport\(\) -> MESSAGEix-GLOBIOM 1.1-MT-R12 \(NAVIGATE\)/baseline>
        - {context}
        - 'M built':
          - <Step load -> MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE>
          - {context}
          - None""",  # noqa: E501
    r"""
- 'report NPi-act':
  - <Step report\(\)>
  - 'context'.*
  - 'BMT NPi-act solved':
    - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-act>
    """,
    r"""
- 'report NPi-all':
  - <Step report\(\)>
  - 'context'.*
  - 'BMT NPi-all solved':
    - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-all>
    """,
    r"""
- 'report NPi-ele':
  - <Step report\(\)>
  - 'context'.*
  - 'BMT NPi-ele solved':
    - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-ele>
    """,
    r"""
- 'report NPi-ref':
  - <Step report\(\)>
  - 'context'.*
  - 'BMT NPi-ref solved':
    - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-ref>
    """,
    r"""
- 'report NPi-tec':
  - <Step report\(\)>
  - 'context'.*
  - 'BMT NPi-tec solved':
    - <Step build_buildings\(\) -> MESSAGEix-GLOBIOM 1.1-BMT-R12 \(NAVIGATE\)/NPi-tec>
    """,
]


def test_generate_workflow(mix_models_cli):
    """Test :func:`.navigate.workflow.generate` and associated CLI."""

    # CLI command to run
    cmd = ["navigate", "run", "--from=M built", "--dry-run", "report all"]

    # Create the expected buildings repo directory for .buildings.Config, even if it
    # does not exist
    code_dir = Path(ixmp.config.get("message buildings dir")).expanduser().resolve()
    if not code_dir.exists():
        code_dir.mkdir(parents=True, exist_ok=True)

    result = mix_models_cli.invoke(cmd)

    # Workflow has the expected scenarios in it
    for b in BLOCKS:
        assert re.search(b, result.output, flags=re.DOTALL), result.output
