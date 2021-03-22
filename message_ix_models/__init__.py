import sys

from pkg_resources import DistributionNotFound, get_distribution

from message_ix_models.util._logging import setup as setup_logging
from message_ix_models.util.context import Context
from message_ix_models.util.importlib import MessageDataFinder
from message_ix_models.util.scenarioinfo import ScenarioInfo

# Expose utility classes
__all__ = ["Context", "ScenarioInfo"]

try:
    # Version string for reference in other code
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    # Package is not installed
    __version__ = "999"

# No logging to stdout (console) by default
setup_logging(console=False)

# Ensure at least one Context instance is created
Context()

# Use this finder only if others fail
sys.meta_path.append(MessageDataFinder())
