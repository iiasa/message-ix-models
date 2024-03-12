"""Iteration of MESSAGE-MACRO solve and MESSAGEix-Transport's demand calculation."""

import logging

from message_ix_models import Context, ScenarioInfo

log = logging.getLogger(__name__)


def main(scenario):
    """Callback for :meth:`ixmp.Scenario.solve`."""
    log.info(f"Executing callback on {scenario!r}")

    from .build import get_computer

    if not ScenarioInfo(scenario).is_message_macro:
        log.info("Not a MESSAGE-MACRO scenario; cannot iterate.")
        return True

    c = get_computer(Context.get_instance(), scenario)

    # Convergence criterion. If not True, the model is run again
    # TODO compute using `c`
    converged = True

    if converged:
        return converged

    # TODO input data to the scenario for next iteration
    del c
