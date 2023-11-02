import re
from typing import Optional

import ixmp
import pytest
from genno import KeyExistsError
from message_ix import make_df

from message_ix_models import Workflow, testing
from message_ix_models.workflow import WorkflowStep, make_click_command, solve

# Functions for WorkflowSteps


def changes_a(c, s) -> None:
    """Change a scenario by modifying structure data, but not data."""
    with s.transact():
        s.add_set("technology", "test_tech")


def changes_b(c, s, value=None) -> None:
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
    def test_call(self, test_context) -> None:
        def action(c, s):
            pass  # pragma: no cover

        ws = WorkflowStep(action=action)

        with pytest.raises(RuntimeError):
            ws(test_context, None)

    def test_repr(self) -> None:
        assert "<Step load>" == repr(WorkflowStep(None))


@pytest.fixture
def wf(request, test_context) -> Workflow:
    return _wf(test_context, request=request)


def _wf(
    context,
    *,
    base_url: Optional[str] = None,
    base_platform: Optional[str] = None,
    request=None,
):
    if base_url is base_platform is None:
        base_scenario = testing.bare_res(request, context, solved=False)
        base_platform = base_scenario.platform.name
        base_url = f"ixmp://{base_platform}/{base_scenario.url}"
        del base_scenario

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
    wf.graph.update({"_base_url": base_url, "_base_platform": base_platform})

    return wf


def test_make_click_command(monkeypatch: pytest.MonkeyPatch, mix_models_cli) -> None:
    import click

    import message_ix_models.cli

    # make_click_command() runs and generates a command
    cmd = make_click_command(f"{__name__}._wf", name="test", slug="test")
    assert isinstance(cmd, click.Command)

    # Monkey-patch this command into the CLI temporarily
    _name = "_test-make-click-command"
    monkeypatch.setitem(message_ix_models.cli.main.commands, _name, cmd)

    # Invoke the command with various parameters
    for params, output in (
        (["--go", "B"], "nothing returned, workflow will continue with"),
        (["--go", "--from=[AX]", "B"], "nothing returned, workflow will continue with"),
        (["B"], "Workflow diagram written to"),
    ):
        # Command runs and exits with 0
        result = mix_models_cli.assert_exit_0([_name] + params)
        # Expected log messages or output were printed
        assert output in result.output

    # Invalid usage
    for params, output in (
        (["--go", "C"], "Error: No step(s) matched"),
        (["--go"], "Error: No target step provided and no default for"),
    ):
        result = mix_models_cli.invoke([_name] + params)
        assert 0 != result.exit_code
        assert output in result.output


@pytest.mark.skipif(
    condition=ixmp.__version__ < "3.5",
    reason="ixmp.TimeSeries.url not available prior to ixmp 3.5.0",
)
def test_workflow(caplog, request, test_context, wf) -> None:
    # FIXME disentangle this to fewer tests of atomic behaviour

    # Retrieve some information from the fixture
    base_url = wf.graph.pop("_base_url")
    base_platform = wf.graph.pop("_base_platform")

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
    mp = base_platform
    m = "MESSAGEix-GLOBIOM R14 YB"
    messages = [
        f"Loaded ixmp://{mp}/{m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "Execute <function changes_a at [^>]*>",
        f"…nothing returned, workflow will continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "Execute <function changes_b at [^>]*>",
        f"…nothing returned, workflow will continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "  with context.dest_scenario={'model': 'foo', 'scenario': 'bar'}",
        "Clone to foo/bar",
        "Execute <function solve at [^>]*>",
    ]
    for expr, message in zip(messages, caplog.messages[start_index:]):
        assert re.match(expr, message)

    assert re.match(
        r"""'B':
- <Step changes_b\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'A':
  - <Step changes_a\(\)>
  - 'context' \(above\)
  - 'base':
    - <Step load -> MESSAGEix-GLOBIOM R14 YB/test_workflow>
    - 'context' \(above\)
    - None""",
        wf.describe("B"),
    )

    # Now truncate the workflow at "Model/A"
    with pytest.raises(RuntimeError, match="Unable to locate platform info for"):
        wf.truncate("A")

    # Add a full URL including platform info
    with pytest.raises(KeyExistsError):
        wf.add_step("base", None, target=f"ixmp://{base_platform}/{base_url}")

    wf.add_step("base", None, target=f"ixmp://{base_platform}/{base_url}", replace=True)
    wf.truncate("A")

    # Description reflects that changes_a() will no longer be called
    assert re.match(
        r"""'B':
- <Step changes_b\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'A':
  - <Step load -> MESSAGEix-GLOBIOM R14 YB/test_workflow>
  - 'context' \(above\)
  - None""",
        wf.describe("B"),
    )
