"""Iteration of MESSAGE-MACRO solve and MESSAGEix-Transport's demand calculation."""

import logging

from message_data.tools import ScenarioInfo

log = logging.getLogger(__name__)


def main(scenario):
    """Callback for :meth:`ixmp.Scenario.solve`."""
    log.info('Executing callback on {!r}'.format(scenario))

    from .demand import from_scenario

    if not ScenarioInfo(scenario).is_message_macro:
        log.info('Not a MESSAGE-MACRO scenario; cannot iterate.')
        return True

    result = from_scenario(scenario)

    # Convergence criterion. If not True, the model is run again
    converged = True

    if converged:
        return converged

    # TODO input data to the scenario for iteration
    del result
