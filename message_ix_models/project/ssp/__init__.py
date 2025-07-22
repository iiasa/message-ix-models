import logging
import re
from typing import TYPE_CHECKING, Union

from .structure import SSP, SSP_2017, SSP_2024, generate

if TYPE_CHECKING:
    from message_ix_models.util.sdmx import URNLookupEnum

__all__ = [
    "SSP",
    "SSP_2017",
    "SSP_2024",
    "generate",
    "parse",
    "ssp_field",
]

log = logging.getLogger(__name__)


def parse(value: Union[str, SSP_2017, SSP_2024]) -> Union[SSP_2017, SSP_2024]:
    """Parse `value` to a member of :data:`SSP_2017` or :data:`SSP_2024`."""
    if isinstance(value, (SSP_2017, SSP_2024)):
        return value

    log.debug(f"Assume {value!r} is from {SSP_2017}")

    if isinstance(value, str):
        return SSP_2017[re.sub("SSP([12345])", r"\1", value)]
    else:
        return SSP_2017(value)


class ssp_field:
    """SSP field for use in data classes."""

    def __init__(self, default: Union[SSP_2017, SSP_2024]):
        self._default = default

    def __set_name__(self, owner, name):
        self._name = "_" + name

    def __get__(self, obj, type) -> "URNLookupEnum":
        if obj is None:
            return None  # type: ignore [return-value]

        try:
            return obj.__dict__[self._name]
        except KeyError:
            return obj.__dict__.setdefault(self._name, self._default)

    def __set__(self, obj, value):
        if value is None:
            value = self._default
        setattr(obj, self._name, parse(value))
