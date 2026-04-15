from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix_models.util.context import Context
    from message_ix_models.workflow import Workflow


def generate(
    context: "Context", *, report_key: str = "transport_all", **options
) -> "Workflow":
    """Generate the CircEUlar scenario workflow."""
    from message_ix_models.model.transport.workflow import SOLVE_CONFIG
    from message_ix_models.model.workflow import from_codelist

    from .structure import CL_TRANSPORT_SCENARIO

    # Set the default .report.Config key for ".* reported" steps
    context.report.key = report_key

    # Set options for solving
    context.solve = SOLVE_CONFIG

    return from_codelist(context, CL_TRANSPORT_SCENARIO)
