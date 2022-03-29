from message_ix import make_df

from message_ix_models import Workflow, testing


def changes_a(s):
    """Change a scenario by modifying structure data, but not data."""
    with s.transact():
        s.add_set("technology", "test_tech")


def changes_b(s):
    """Change a scenario by modifying parameter data, but not structure."""
    with s.transact():
        s.add_par(
            "technical_lifetime",
            make_df(
                "technical_lifetime",
                node_loc=s.set("node")[0],
                year_vtg=s.set("year")[0],
                technology="test_tech",
                value=100.0,
                unit="y",
            ),
        )


def test_workflow(request, test_context):
    def base_scenario(arg):
        """Generate a base scenario."""
        return testing.bare_res(request, test_context, solved=False)

    # Create the workflow
    wf = Workflow(test_context)

    # Model/base is created from nothing by calling base_scenario
    wf.add("Model/base", None, base_scenario)
    # Model/A is created from Model/base by calling changes_a
    wf.add("Model/A", "Model/base", changes_a)
    # Model/B is created from Model/A by calling changes_b
    wf.add("Model/B", "Model/A", changes_b)

    # Trigger the creation and solve of Model/B and all required precursor scenarios
    s = wf.run("Model/B")

    # Scenario contains changes from the first and second step
    assert "test_tech" in set(s.set("technology"))
    assert 1 == len(s.par("technical_lifetime"))
    # Scenario was solved
    assert s.has_solution()

    # Same test, except with solve=False
    wf = Workflow(test_context, solve=False)
    wf.add("Model/base", None, base_scenario)
    wf.add("Model/A", "Model/base", changes_a)
    wf.add("Model/B", "Model/A", changes_b)
    s = wf.run("Model/B")

    # Same assertions, except scenario was NOT solved
    assert "test_tech" in set(s.set("technology"))
    assert 1 == len(s.par("technical_lifetime"))
    assert not s.has_solution()
