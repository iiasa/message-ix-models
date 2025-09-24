try:
    from message_ix.macro import MACRO
except ImportError:
    from message_ix.models import MACRO

__all__ = ["MACRO"]
