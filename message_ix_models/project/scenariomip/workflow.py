"""Generate the ScenarioMIP workflow."""

import logging
from typing import TYPE_CHECKING, Optional

from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context

log = logging.getLogger(__name__)


def step_01(context: "Context", scenario: "Scenario") -> "Scenario":
    """Clean Timeseries."""
    log.info("Not implemented: step_01(…)")
    return scenario


def step_02(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Land-use Emulator."""
    log.info("Not implemented: step_02(…)")
    return scenario


def step_03(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Biomass Trade."""
    log.info("Not implemented: step_03(…)")
    return scenario


def step_04(context: "Context", scenario: "Scenario") -> "Scenario":
    """Solve for Historic Reporting."""
    log.info("Not implemented: step_04(…)")
    return scenario


def step_05(context: "Context", scenario: "Scenario") -> "Scenario":
    """Build Materials."""
    log.info("Not implemented: step_05(…)")
    return scenario


def step_06(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add DAC (Direct Air Capture)."""
    log.info("Not implemented: step_06(…)")
    return scenario


def step_07(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Non-CO2 GHGs."""
    log.info("Not implemented: step_07(…)")
    return scenario


def step_08(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Water."""
    log.info("Not implemented: step_08(…)")
    return scenario


def step_09(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Techno-economic Parameters."""
    log.info("Not implemented: step_09(…)")
    return scenario


def step_10(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Balance Equalities (some petrochemical trade balances?)."""
    log.info("Not implemented: step_10(…)")
    return scenario


def step_11(context: "Context", scenario: "Scenario") -> "Scenario":
    """OBSOLETE."""
    log.info("Not implemented: step_11(…)")
    return scenario


def step_12(context: "Context", scenario: "Scenario") -> "Scenario":
    """Gas Mix Limitations (constrain gas use in industries)."""
    log.info("Not implemented: step_12(…)")
    return scenario


def step_13(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Slack and Constraints (so we need to update the slacks)."""
    log.info("Not implemented: step_13(…)")
    return scenario


def step_14(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add Shipping."""
    log.info("Not implemented: step_14(…)")
    return scenario


def step_15(context: "Context", scenario: "Scenario") -> "Scenario":
    log.info("Not implemented: step_15(…)")
    return scenario


def step_16(context: "Context", scenario: "Scenario") -> "Scenario":
    """Calibrate Macro."""
    log.info("Not implemented: step_16(…)")
    return scenario


#: Order of steps in the workflow.
STEP_ORDER = [
    step_01,
    step_02,
    step_03,
    step_04,
    step_05,
    step_06,
    step_07,
    step_08,
    step_09,
    step_10,
    step_11,
    step_12,
    step_13,
    step_14,
    step_15,
    step_16,
]


def generate(context: "Context", step_order: Optional[list] = None) -> "Workflow":
    """Generate the ScenarioMIP workflow.

    Parameters
    ----------
    step_order
        Order of steps in the workflow. If not given, :data:`STEP_ORDER` is used.
    """
    wf = Workflow(context)

    prev = wf.add_step("base", None)

    step_order = step_order or STEP_ORDER

    for i, func in enumerate(step_order, start=1):
        prev = wf.add_step(f"step_{i}", prev, func)

    # "all" is an alias for the last step, whatever it is
    wf.add("all", prev)

    return wf
