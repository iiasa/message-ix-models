"""Utilities for working with :mod:`.ixmp`.

Code here should be either:

1. For backwards compatibility to earlier versions of ixmp supported according to the
   :ref:`policy-upstream-versions`, or
2. Temporary, pending migration upstream to ixmp itself.
"""

import pickle
from dataclasses import dataclass, field
from hashlib import blake2s

import ixmp


@dataclass
class Platforms:
    """Collection of references to :class:`ixmp.Platform` objects.

    References to :class:`ixmp.Platform` from |Scenario| instances are weak references.
    These allow the former to be garbage collected and reduce memory use, but in some
    cases we wish the Platform instance be kept alive. Platform instances referenced
    from Platforms are kept alive until they are :meth:`pop` 'd.
    """

    _platforms: dict[str, ixmp.Platform] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self._platforms)

    def get(self, *args, **kwargs) -> ixmp.Platform:
        """Get a Platform according to its connection `args` and `kwargs`.

        If an existing instance exists, it is returned. Otherwise, a new instance is
        created.
        """
        key = self.to_key(args, kwargs)

        try:
            return self._platforms[key]
        except KeyError:
            return self._platforms.setdefault(key, ixmp.Platform(*args, **kwargs))

    def pop(self, *args, **kwargs) -> None:
        """Pop a Platform by its `args`/`kwargs` and close its database connection."""
        key = self.to_key(args, kwargs)

        try:
            mp = self._platforms.pop(key)
        except KeyError:
            return None
        else:
            mp.close_db()
            del mp

    def popitem(self) -> None:
        """:meth:`pop` an arbitrary Platform."""
        try:
            key = next(iter(self._platforms.keys()))
        except StopIteration:
            return
        else:
            self._platforms.pop(key)

    @staticmethod
    def to_key(args, kwargs) -> str:
        """Convert `args`, `kwargs` to a hashable key."""
        data = (args, sorted(kwargs.items()))

        # Pickle the data, using a fixed protocol version for all Pythons, then hash
        hashed = blake2s(pickle.dumps(data, protocol=5))

        # Return the whole digest
        return hashed.hexdigest()
