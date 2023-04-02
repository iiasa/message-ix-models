import sys
from importlib.metadata import PackageNotFoundError, version

import pint
from iam_units import registry

from message_ix_models.util._logging import setup as setup_logging
from message_ix_models.util.config import Config
from message_ix_models.util.context import Context
from message_ix_models.util.importlib import MessageDataFinder
from message_ix_models.util.scenarioinfo import ScenarioInfo, Spec
from message_ix_models.workflow import Workflow

# Expose utility classes
__all__ = ["Config", "Context", "ScenarioInfo", "Spec", "Workflow"]

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    # Package is not installed
    __version__ = "999"

# No logging to stdout (console) by default
setup_logging(console=False)

# Use iam_units.registry as the default pint.UnitsRegistry
pint.set_application_registry(registry)

# Ensure at least one Context instance is created
Context()

# Use this finder only if others fail
sys.meta_path.append(MessageDataFinder())
