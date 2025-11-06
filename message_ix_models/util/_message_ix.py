try:
    from message_ix.macro import MACRO
    from message_ix.message import MESSAGE
except ImportError:
    from message_ix.models import MACRO, MESSAGE  # type: ignore [no-redef]

__all__ = [
    "MACRO",
    "MESSAGE",
]
