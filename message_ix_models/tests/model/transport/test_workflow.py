import pytest

from message_ix_models.model.transport.workflow import SOLVE_CONFIG, generate
from message_ix_models.project.digsy.structure import CL_SCENARIO_DIGSY
from message_ix_models.project.edits.structure import CL_SCENARIO_EDITS_MCE


@generate.minimum_version
@pytest.mark.parametrize(
    "base_scenario",
    (
        "auto",
        pytest.param(
            "bare",
            marks=pytest.mark.skip(reason="Slow; generates copies of the bare RES"),
        ),
    ),
)
def test_generate(test_context, base_scenario) -> None:
    test_context.model.regions = "R12"

    # Workflow is generated
    wf = generate(test_context, base_scenario=base_scenario)

    # SOLVE_CONFIG is stored to be used for "… solve" steps
    ctx = wf.graph["context"]
    assert ctx.solve == SOLVE_CONFIG

    # The default reporting key is set to "transport all"
    assert "transport all" == ctx.report.key

    # Workflow contains some expected steps
    assert "EDITS-HA reported" in wf
    assert "LED-SSP1 reported" in wf

    # Separate steps for tax and GHG pricing policies are included
    assert "SSP5 tax reported" in wf
    assert "SSP5 exo price 5cab reported" in wf

    # WorkflowStep objects store expected configuration for certain projects
    step = wf.graph["DIGSY-BEST-C T built"][0]
    assert (
        CL_SCENARIO_DIGSY.get()["BEST-C"] == step.kwargs["config"].project_scenario_code
    )
    step = wf.graph["EDITS-HA T built"][0]
    assert (
        CL_SCENARIO_EDITS_MCE.get()["HA"] == step.kwargs["config"].project_scenario_code
    )

    # wf.run("LED-SSP1 reported")  # NB Only works with base_scenario="bare"
