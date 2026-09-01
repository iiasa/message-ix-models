import re
from typing import TYPE_CHECKING

import pytest
from message_ix import Scenario, make_df

from message_ix_models import Workflow, testing
from message_ix_models.workflow import WorkflowStep, make_click_command, solve

if TYPE_CHECKING:
    from message_ix_models import Context
    from message_ix_models.util.click import CliRunner


# Fixtures and test utility functions


def changes_a(c: "Context", s: "Scenario") -> None:
    """Change a scenario by modifying structure data, but not data."""
    with s.transact():
        s.add_set("technology", "test_tech")


def changes_b(c: "Context", s: "Scenario", value: float | None = None) -> None:
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
    """Tests of :class:`.WorkflowStep`."""

    def test_call(self, test_context: "Context") -> None:
        """Raises :class:`RuntimeError` if no Scenario is passed to __call__()."""

        def action(c, s):
            pass  # pragma: no cover

        ws = WorkflowStep(action=action)

        with pytest.raises(RuntimeError):
            ws(test_context, None)

    def test_repr(self) -> None:
        assert "<Load>" == repr(WorkflowStep(None))

    @pytest.mark.parametrize("value", (False, True))
    def test_set_as_default(
        self, request: "pytest.FixtureRequest", test_context: "Context", value: bool
    ) -> None:
        """Test :attr:`WorkflowStep.set_as_default`."""
        # Create a Scenario
        s = testing.bare_res(request, test_context, solved=False)

        # A step that clones to a new version of the same (model name, scenario name),
        # with set_as_default either True or False
        ws = WorkflowStep(
            None, clone=True, target=f"{s.model}/{s.scenario}", set_as_default=value
        )

        # Workflow step runs
        result = ws(test_context, s)

        # The resulting scenario is a clone of `s`, with greater version number
        assert result.version and s.version and result.version > s.version

        # Reload the *default version* of the same scenario
        s_default = Scenario(s.platform, s.model, s.scenario)

        # - set_as_default=True → the version of `result` is the default.
        # - set_as_default=False → the version of `s` is the default.
        assert (result.version if value is True else s.version) == s_default.version


class TestWorkflow:
    """Tests of :class:`.Workflow`."""

    _request: pytest.FixtureRequest | None = None

    @classmethod
    def _wf(
        cls,
        context: "Context",
        *,
        base_url: str | None = None,
        base_platform: str | None = None,
        request: pytest.FixtureRequest | None = None,
    ) -> Workflow:
        """Generate a Workflow for testing."""
        request = request or cls._request
        assert request is not None

        if base_url is base_platform is None:
            # Create a bare RES scenario for testing
            base_scenario = testing.bare_res(request, context, solved=False)
            base_platform = base_scenario.platform.name
            base_url = f"ixmp://{base_platform}/{base_scenario.url}"

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

    @pytest.fixture
    @classmethod
    def wf(cls, request: "pytest.FixtureRequest", test_context: "Context") -> Workflow:
        """A complete Workflow."""
        return cls._wf(test_context, request=request)

    def test_general(
        self,
        caplog: pytest.LogCaptureFixture,
        request: pytest.FixtureRequest,
        test_context: "Context",
        wf: Workflow,
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
        ms = (
            f"MESSAGEix-GLOBIOM {test_context.model.regions} YB [0-9a-f]{{5}}/"
            "test_general"
        )
        messages = [
            f"Loaded ixmp://{mp}/{ms}#1",
            f"Step runs on ixmp://{mp}/{ms}#1",
            "Execute <function changes_a at [^>]*>",
            "…nothing returned",
            f"Workflow continues with {ms}#1",
            f"Step runs on ixmp://{mp}/{ms}#1",
            "Execute <function changes_b at [^>]*>",
            "…nothing returned",
            f"Workflow continues with {ms}#1",
            f"Step runs on ixmp://{mp}/{ms}#1",
            "  with "
            "context.dest_scenario={(('model': 'foo'|'scenario': 'bar')(, )?){2}}",
            "Clone to foo/bar",
            "Execute <function solve at [^>]*>",
        ]
        for expr, message in zip(messages, caplog.messages[start_index:]):
            assert re.match(expr, message), messages

        assert re.match(
            rf"""'B':
- <Step changes_b\(\)>
- 'context':
  - <Context object at \w+ with \d+ keys>
- 'A':
  - <Step changes_a\(\)>
  - 'context' \(above\)
  - 'base':
    - <Load -> {ms}>
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
  - <Load -> {ms}>
  - 'context' \(above\)
  - None""",
            wf.describe("B"),
        )


def test_make_click_command(
    request: pytest.FixtureRequest, mix_models_cli: "CliRunner"
) -> None:
    """:func:`make_click_command` generates a CLI interface to a workflow."""
    import click

    from message_ix_models.cli import cli_test_group
    from message_ix_models.util.click import temporary_command

    # Update the TestWorkflow class variable to allow _wf() to access request.node.name
    TestWorkflow._request = request

    # make_click_command() runs and generates a command
    name = "make-click-command"
    cmd = make_click_command(f"{__name__}.TestWorkflow._wf", name=name, slug="test")
    assert isinstance(cmd, click.Command)

    # Add this into the hidden CLI test group
    with temporary_command(cli_test_group, cmd):
        # Invoke the command with various parameters
        for params, output in (
            (["--go", "B"], "Workflow continues with"),
            (["B"], "Write workflow diagram to"),
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
