import sys

from message_ix_models.util.importlib import MessageDataFinder

# Use this finder only if others fail
sys.meta_path.append(MessageDataFinder())
