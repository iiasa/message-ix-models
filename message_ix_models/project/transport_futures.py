"""Transport Futures project."""

from enum import Enum, auto


class SCENARIO(Enum):
    """Identifiers of Transport Futures scenarios."""

    BASE = 0
    A___ = auto()  # NB use underscores because "-" is invalid in Python names
    AS__ = auto()
    ASI_ = auto()
    ASIF = auto()
    DEBUG = auto()

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls[(value or "BASE").upper().replace("-", "_")]
        except KeyError:
            raise ValueError(f"Unknown Transport Futures scenario {value!r}")

    def id(self) -> str:
        return (
            self.name.replace("_", "-")
            if self.name not in ("BASE", "DEBUG")
            else self.name.lower()
        )
