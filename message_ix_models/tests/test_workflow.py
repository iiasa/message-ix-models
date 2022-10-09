import re

import pytest
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
    def test_repr(self):
        assert "<Step load>" == repr(WorkflowStep(None))


def test_workflow(caplog, request, test_context):
    base_scenario = testing.bare_res(request, test_context, solved=False)
    caplog.clear()

    # Create the workflow
    wf = Workflow(test_context)

    # Model/base is created from nothing by calling base_scenario
    wf.add_step("Model/base", None, target=base_scenario.url)
    # Model/A is created from Model/base by calling changes_a
    wf.add_step("Model/A", "Model/base", changes_a)
    # Model/B is created from Model/A by calling changes_b
    wf.add_step("Model/B", "Model/A", changes_b, value=100.0)

    wf.add_step("B solved", "Model/B", solve)

    # Trigger the creation and solve of Model/B and all required precursor scenarios
    s = wf.run("B solved")
    print(s)

    # Scenario contains changes from the first and second step
    assert "test_tech" in set(s.set("technology"))
    assert 1 == len(s.par("technical_lifetime"))
    # Scenario was solved
    assert s.has_solution()

    mp = "message-ix-models"
    m = "MESSAGEix-GLOBIOM R14 YB"
    messages = [
        f"Loaded ixmp://{mp}/{m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "  with context.dest_scenario={}",
        "Execute <function changes_a at [^>]*>",
        f"…nothing returned, continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "  with context.dest_scenario={}",
        "Execute <function changes_b at [^>]*>",
        f"…nothing returned, continue with {m}/test_workflow#1",
        f"Step runs on ixmp://{mp}/{m}/test_workflow#1",
        "  with context.dest_scenario={}",
        "Execute <function solve at [^>]*>",
    ]
    for expr, message in zip(messages, caplog.messages[1:]):
        assert re.match(expr, message)

    # Now truncate the workflow at "Model/A"
    with pytest.raises(RuntimeError, match="Unable to locate platform info for"):
        wf.truncate("Model/A")
