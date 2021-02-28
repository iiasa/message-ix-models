import sys

from message_ix_models.util.context import Context
from message_ix_models.util.importlib import MessageDataFinder

# Ensure at least one Context instance is created
Context()

# Use this finder only if others fail
sys.meta_path.append(MessageDataFinder())
