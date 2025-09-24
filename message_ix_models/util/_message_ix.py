try:
    from message_ix.macro import MACRO
except ImportError:
    from message_ix.models import MACRO  # type: ignore [no-redef]

__all__ = ["MACRO"]
