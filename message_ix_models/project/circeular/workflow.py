from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix_models.util.context import Context
    from message_ix_models.workflow import Workflow


def generate(context: "Context", **options) -> "Workflow":
    """Generate the CircEUlar :class:`.Workflow`."""
    from message_ix_models import Workflow

    from .structure import CL_TRANSPORT_SCENARIO

    # Create the workflow
    wf = Workflow(context)

    # Iterate over all scenarios in IIASA_ECE:CL_CIRCEULAR_TRANSPORT_SCENARIO
    for scenario_code in CL_TRANSPORT_SCENARIO.get():
        pass

    return wf
