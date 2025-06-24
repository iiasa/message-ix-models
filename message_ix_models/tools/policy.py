"""Policies."""

from abc import ABC
from collections.abc import Collection
from typing import TYPE_CHECKING, Optional, cast

if TYPE_CHECKING:
    from typing import TypeVar

    T = TypeVar("T", bound="Policy")


class Policy(ABC):
    """Base class for policies.

    This class has no attributes or public methods. Other modules in
    :mod:`message_ix_models`:

    - **should** subclass Policy to represent different kinds of policy.
    - **may** add attributes, methods, etc. to aid with the *implementation* of those
      policies in concrete scenarios.
    - in contrast, **may** use minimal subclasses as mere flags to be interpreted by
      other code.

    The default implementation of :func:`hash` returns a value the same for every
    instance of a subclass. This means that two instances of the same subclass hash
    equal. See :attr:`.Config.policy`.
    """

    def __hash__(self) -> int:
        return hash(type(self))


def single_policy_of_type(
    collection: Collection[Policy], cls: type["T"]
) -> Optional["T"]:
    """Return a single member of `collection` of type `cls`."""
    if matches := list(filter(lambda p: isinstance(p, cls), collection)):
        if len(matches) > 1:
            raise ValueError(f"Ambiguous: {len(matches)} instance of {cls}")
        return cast("T", matches[0])

    return None
