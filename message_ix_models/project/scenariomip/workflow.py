from typing import TYPE_CHECKING, Optional

from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix_models import Context


def step_01() -> None:
    """Clean Timeseries."""
    raise NotImplementedError


def step_02() -> None:
    """Add Land-use Emulator."""
    raise NotImplementedError


def step_03() -> None:
    """Add Biomass Trade."""
    raise NotImplementedError


def step_04() -> None:
    """Solve for Historic Reporting."""
    raise NotImplementedError


def step_05() -> None:
    """Build Materials."""
    raise NotImplementedError


def step_06() -> None:
    """Add DAC (Direct Air Capture)."""
    raise NotImplementedError


def step_07() -> None:
    """Add Non-CO2 GHGs."""
    raise NotImplementedError


def step_08() -> None:
    """Add Water."""
    raise NotImplementedError


def step_09() -> None:
    """Add Techno-economic Parameters."""
    raise NotImplementedError


def step_10() -> None:
    """Add Balance Equalities (some petrochemical trade balances?)."""
    raise NotImplementedError


def step_11() -> None:
    """OBSOLETE."""
    raise NotImplementedError


def step_12() -> None:
    """Gas Mix Limitations (constrain gas use in industries)."""
    raise NotImplementedError


def step_13() -> None:
    """Add Slack and Constraints (so we need to update the slacks)."""
    raise NotImplementedError


def step_14() -> None:
    """Add Shipping."""
    raise NotImplementedError


def step_15() -> None:
    """Prepare for Macro Calibration."""
    raise NotImplementedError


def step_16() -> None:
    """Calibrate Macro."""
    raise NotImplementedError


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


def generate_workflow(
    context: "Context", step_order: Optional[list] = None
) -> "Workflow":
    """Generate the ScenarioMIP workflow."""
    wf = Workflow(context)

    prev = wf.add_step("base", None)

    step_order = step_order or STEP_ORDER

    for i, func in enumerate(step_order, start=1):
        prev = wf.add_step(f"step_{i}", prev, func)

    # "all" is an alias for the last step, whatever it is
    wf.add("all", prev)

    return wf
