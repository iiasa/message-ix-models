from typing import TYPE_CHECKING

from message_ix_models.model.workflow import step_0
from message_ix_models.testing import bare_res
from message_ix_models.tools import (
    add_AFOLU_CO2_accounting,
    add_alternative_TCE_accounting,
)

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from message_ix_models import Context


def test_step_0(request: "FixtureRequest", test_context: "Context") -> None:
    """Test :func:`.model.workflow.step_0`."""
    test_context.model.regions = "R12"
    scenario = bare_res(request, test_context, solved=False)

    # Add to `scenario` minimal data/structure needed by tools to be used
    add_AFOLU_CO2_accounting.test_data(scenario)
    add_alternative_TCE_accounting.test_data(scenario)

    step_0(test_context, scenario)

    # TODO Add assertions about modified structure & data
