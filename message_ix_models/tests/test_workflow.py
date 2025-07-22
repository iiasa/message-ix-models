import re
from typing import TYPE_CHECKING, Optional

import pytest
from message_ix import make_df

from message_ix_models import Workflow, testing
from message_ix_models.testing import MARK
from message_ix_models.workflow import WorkflowStep, make_click_command, solve

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context
    from message_ix_models.util.click import CliRunner

_REQUEST: Optional["pytest.FixtureRequest"] = None

# Functions for WorkflowSteps


def changes_a(c: "Context", s: "Scenario") -> None:
    """Change a scenario by modifying structure data, but not data."""
    with s.transact():
        s.add_set("technology", "test_tech")


def changes_b(c: "Context", s: "Scenario", value: Optional[float] = None) -> None:
    """Change a scenario by modifying parameter data, but not structure."""
    with s.transact():
        s.add_par(
            "technical_lifetime",
            make_df(
                "technical_lifetime",
                node_loc=s.set("node")[0],
                year_vtg=s.set("year")[0],
                technology="test_tech",
                value=value,
                unit="y",
            ),
        )


class TestWorkflowStep:
    def test_call(self, test_context: "Context") -> None:
        def action(c, s):
            pass  # pragma: no cover

        ws = WorkflowStep(action=action)

        with pytest.raises(RuntimeError):
            ws(test_context, None)

    def test_repr(self) -> None:
        assert "<Step load>" == repr(WorkflowStep(None))


@pytest.fixture(scope="function")
def wf(request: "pytest.FixtureRequest", test_context: "Context") -> Workflow:
    return _wf(test_context, request=request)


def _wf(
    context: "Context",
    *,
    base_url: Optional[str] = None,
    base_platform: Optional[str] = None,
    request: Optional["pytest.FixtureRequest"] = None,
) -> "Workflow":
    request = request or _REQUEST
    assert request is not None

    if base_url is base_platform is None:
        base_scenario = testing.bare_res(request, context, solved=False)
        base_platform = base_scenario.platform.name
        base_url = f"ixmp://{base_platform}/{base_scenario.url}"

    """A function that generates a Workflow."""
    # Create the workflow
    wf = Workflow(context)

    # Model/base is created from nothing by calling base_scenario
    wf.add_step("base", None, target=base_url)
    # Model/A is created from Model/base by calling changes_a
    wf.add_step("A", "base", changes_a)
    # Model/B is created from Model/A by calling changes_b
    wf.add_step("B", "A", changes_b, value=100.0)

    # Store extra info
    wf.graph.update({"_base_platform": base_platform})

    return wf


@MARK[1]
def test_make_click_command(
    request: "pytest.FixtureRequest", mix_models_cli: "CliRunner"
) -> None:
    import click

    from message_ix_models.cli import cli_test_group
    from message_ix_models.util.click import temporary_command

    # Allow _wf() to access request.node.name
    global _REQUEST
    _REQUEST = request

    # make_click_command() runs and generates a command
    name = "make-click-command"
    cmd = make_click_command(f"{__name__}._wf", name=name, slug="test")
    assert isinstance(cmd, click.Command)

    # Add this into the hidden CLI test group
    with temporary_command(cli_test_group, cmd):
        # Invoke the command with various parameters
        for params, output in (
            (["--go", "B"], "nothing returned, workflow will continue with"),
            (["B"], "Workflow diagram written to"),
        ):
            # Command runs and exits with 0
            result = mix_models_cli.assert_exit_0(["_test", "run"] + params)
            # Expected log messages or output were printed
            assert output in result.output

        # Invalid usage
        for params, output in (
            (["--go", "C"], "Error: No step(s) matched"),
            (["--go"], "Error: No target step provided and no default for"),
            # Step changes_b() fails if changes_a() is not first run
            (["--go", "--from=[AX]", "B"], "Execute <function changes_b"),
        ):
            result = mix_models_cli.invoke(["_test", "run"] + params)
            assert 0 != result.exit_code
            assert output in result.output


def test_workflow(
    caplog, request: "pytest.FixtureRequest", test_context: "Context", wf: "Workflow"
) -> None:
    # Retrieve some information from the fixture
    mp = wf.graph.pop("_base_platform")

    caplog.clear()

    # "B solved" is created from "Model/B" by clone and running solve()
    # clone=True without target= raises an exception
    with pytest.raises(TypeError, match="target= must be supplied"):
        wf.add_step("B solved", "B", solve, clone=True)

    wf.add_step("B solved", "B", solve, clone=True, target="foo/bar")

    # Trigger the creation and solve of Model/B and all required precursor scenarios
    s = wf.run("B solved")

    # Scenario contains changes from the first and second step
    assert "test_tech" in set(s.set("technology"))
    assert 1 == len(s.par("technical_lifetime"))
    # Scenario was solved
    assert s.has_solution()

    # Log messages reflect workflow steps executed
    start_index = 1 if caplog.messages[0].startswith("Cull") else 0
    # Expression for the model name:
    # - The setting obtains different values on different GHA jobs
    # - The suffix after YB is a random Base32 or Base32hex string, in lower case,
    #   length 5.
    m = f"MESSAGEix-GLOBIOM {test_context.model.regions} YB [0-9a-f]{{5}}"
    messages = [
        f"Loaded ixmp://{mp}/{m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "Execute <function changes_a at [^>]*>",
        f"…nothing returned, workflow will continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "Execute <function changes_b at [^>]*>",
        f"…nothing returned, workflow will continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "  with context.dest_scenario={(('model': 'foo'|'scenario': 'bar')(, )?){2}}",
        "Clone to foo/bar",
        "Execute <function solve at [^>]*>",
    ]
    for expr, message in zip(messages, caplog.messages[start_index:]):
        assert re.match(expr, message)

    assert re.match(
        rf"""'B':
- <Step changes_b\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'A':
  - <Step changes_a\(\)>
  - 'context' \(above\)
  - 'base':
    - <Step load -> {m}/test_workflow>
    - 'context' \(above\)
    - None""",
        wf.describe("B"),
    )

    # Now truncate the workflow at "Model/A"
    wf.truncate("A")

    # Description reflects that changes_a() will no longer be called
    assert re.match(
        rf"""'B':
- <Step changes_b\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'A':
  - <Step load -> {m}/test_workflow>
  - 'context' \(above\)
  - None""",
        wf.describe("B"),
    )
