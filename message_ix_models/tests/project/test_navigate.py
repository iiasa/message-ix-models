import re
from pathlib import Path

import ixmp
import pytest

from message_data.projects.navigate.report import _scenario_name


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
    assert wf.graph["MT NPi-ref solved"][2] == "MT NPi-ref built"
    assert wf.graph["MT NPi-ref built"][2] == "M built"

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
    - 'MT NPi-ref solved':
      - <Step solve\(\)>
      - {context}
      - 'MT NPi-ref built':
        - <Step build_transport\(\) -> MESSAGEix-GLOBIOM 1.1-MT-R12 \(NAVIGATE\)/NPi-ref>
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


@pytest.mark.parametrize(
    "input, expected",
    (
        ("NPi-ref", "NAV_Dem-NPi-ref"),
        ("NPi-ref_ENGAGE_20C_step-3+B", "NAV_Dem-20C-ref"),
        ("NPi-tec_ENGAGE_20C_step-3+B", "NAV_Dem-20C-tec_u"),
        ("baseline", None),
    ),
)
def test_scenario_name(test_context, input, expected):
    assert expected == _scenario_name(test_context, input)
