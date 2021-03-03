import sys

from message_ix_models.util.context import Context
from message_ix_models.util.importlib import MessageDataFinder
from message_ix_models.util.logging import setup as setup_logging
from message_ix_models.util.scenarioinfo import ScenarioInfo

# Expose utility classes
__all__ = ["Context", "ScenarioInfo"]

# No logging to stdout (console) by default
setup_logging(console=False)

# Ensure at least one Context instance is created
Context()

# Use this finder only if others fail
sys.meta_path.append(MessageDataFinder())
