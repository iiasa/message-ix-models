"""DAC (Direct Air Capture) tool placeholder.

This module should provide functions for adding DAC technologies to MESSAGEix scenarios.
Currently this is waiting to be merged from another branch.
This file should be removed when the full functionality is merged.
"""

import logging
from typing import Any

log = logging.getLogger(__name__)


def add_tech(*args, **kwargs) -> Any:
    """Add DAC technology to a scenario.

    This is a stub implementation. The full functionality is available in
    another branch.

    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments

    Returns:
        Any: Placeholder return value

    Raises:
        NotImplementedError: Always raises this error as the function is not implemented
    """
    log.warning(
        "add_tech function is not implemented in this branch. "
        "The full DAC functionality is available in another branch."
    )
    raise NotImplementedError(
        "add_tech function is not implemented in this branch. "
        "Please use the branch that contains the full DAC implementation."
    )
