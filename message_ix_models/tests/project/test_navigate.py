import re
from pathlib import Path

import ixmp
import pytest


@pytest.fixture(scope="session")
def message_buildings_dir():
    """Create :attr:`.buildings.Config.code_dir, if it does not exist."""
    code_dir = Path(ixmp.config.get("message buildings dir")).expanduser().resolve()
    if not code_dir.exists():
        code_dir.mkdir(parents=True, exist_ok=True)


@pytest.mark.usefixtures("message_buildings_dir")
def test_generate_workflow(test_context):
    from message_data.projects.navigate.workflow import generate

    # Set an empty value
    test_context["navigate_scenario"] = None

    # Same as in test_generate_workflow_cli
    wf = generate(test_context)
    wf.truncate("M built")

    # Check the pre-requisite steps of some workflow steps. This is the 2nd entry in the
    # dask task tuple in wf.graph.
    assert wf.graph["MT solved"][2] == "MT built"
    assert wf.graph["MT built"][2] == "M built"

    # Workflow is truncated at "M built"
    assert wf.graph["M built"][2] is None
    assert wf.graph["M built"][0].scenario_info == dict(
        model="MESSAGEix-Materials", scenario="baseline_DEFAULT_NAVIGATE"
    )

    for s in ("act", "all", "ele", "ref", "tec"):
        # Depends on the corresponding solved BMT model
        assert wf.graph[f"NPi-{s} reported"][2] == f"BMT NPi-{s} solved"

        # Scenario name as expected
        assert wf.graph[f"BMT NPi-{s} solved"][0].scenario_info == dict(
            model="MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)", scenario=f"NPi-{s}"
        )


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
]


@pytest.mark.usefixtures("message_buildings_dir")
def test_generate_workflow_cli(mix_models_cli):
    """Test :func:`.navigate.workflow.generate` and associated CLI."""

    # CLI command to run
    cmd = ["navigate", "run", "--from=M built", "--dry-run", "report all"]
    result = mix_models_cli.invoke(cmd)

    # Workflow has the expected scenarios in it
    for b in BLOCKS:
        assert re.search(b, result.output, flags=re.DOTALL), result.output
