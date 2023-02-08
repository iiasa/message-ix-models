import re

import ixmp
import pytest
from genno import KeyExistsError
from message_ix import make_df

from message_ix_models import Workflow, testing
from message_ix_models.workflow import WorkflowStep, solve


def changes_a(c, s):
    """Change a scenario by modifying structure data, but not data."""
    with s.transact():
        s.add_set("technology", "test_tech")


def changes_b(c, s, value=None):
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
    def test_call(self, test_context):
        def action(c, s):
            pass  # pragma: no cover

        ws = WorkflowStep(action=action)

        with pytest.raises(RuntimeError):
            ws(test_context, None)

    def test_repr(self):
        assert "<Step load>" == repr(WorkflowStep(None))


@pytest.mark.skipif(
    condition=ixmp.__version__ < "3.5",
    reason="ixmp.TimeSeries.url not available prior to ixmp 3.5.0",
)
def test_workflow(caplog, request, test_context):
    # FIXME disentangle this to fewer tests of atomic behaviour
    base_scenario = testing.bare_res(request, test_context, solved=False)
    base_url = base_scenario.url
    base_platform = base_scenario.platform.name
    del base_scenario

    caplog.clear()

    # Create the workflow
    wf = Workflow(test_context)

    # Model/base is created from nothing by calling base_scenario
    wf.add_step("base", None, target=base_url)
    # Model/A is created from Model/base by calling changes_a
    wf.add_step("A", "base", changes_a)
    # Model/B is created from Model/A by calling changes_b
    wf.add_step("B", "A", changes_b, value=100.0)

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
