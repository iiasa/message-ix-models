import pytest

from message_ix_models.model.transport.workflow import generate
from message_ix_models.project.digsy.structure import SCENARIO as DIGSY
from message_ix_models.project.edits.structure import SCENARIO as EDITS


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

    # Workflow contains some expected steps
    assert "EDITS-HA reported" in wf
    assert "LED-SSP1 reported" in wf

    # Separate steps for tax and GHG pricing policies are included
    assert "SSP5 tax reported" in wf
    assert "SSP5 exo price reported" in wf

    # WorkflowStep objects store expected configuration for certain projects
    assert (
        DIGSY["BEST"]
        is wf.graph["DIGSY-BEST built"][0].kwargs["config"].project["DIGSY"]
    )
    assert (
        EDITS["HA"] is wf.graph["EDITS-HA built"][0].kwargs["config"].project["EDITS"]
    )

    # wf.run("LED-SSP1 reported")  # NB Only works with base_scenario="bare"
