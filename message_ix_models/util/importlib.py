"""Load model and project code from :mod:`message_data`."""

import functools
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import import_module, util
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.metadata import version
from itertools import chain
from logging import INFO, getLogger
from platform import python_version
from typing import TYPE_CHECKING, Optional

from packaging.version import parse

from ._logging import once

if TYPE_CHECKING:
    from typing import Protocol

    class Condition(Protocol):
        def __call__(self) -> str: ...

    class MinimumVersionDecorated(Protocol):
        def __call__(self, *args, **kwargs): ...
        def minimum_version(self, to_wrap): ...


class MessageDataFinder(MetaPathFinder):
    """Load model and project code from :mod:`message_data`.

    This class allows for future-proof import statements. For example, if there is a
    module :py:`message_data.project.foo.bar`, code can be written like:

    .. code-block:: python

       from message_ix_models.project.foo import bar

    The :meth:`find_spec` method locates the corresponding submodule in
    :mod:`message_data` and imports it as if it were in :mod:`message_ix_models`. Later,
    if the module is migrated to :py:`message_ix_models.project.foo.bar`, the import
    statement will work directly, without changes or use of MessageDataFinder.

    Where the *same* module names exist within both packages, the submodule of
    :mod:`message_ix_models` will be found directly and MessageDataFinder will not be
    invoked.

    This behaviour also allows to mix model and project code in the two packages,
    although this **should** be avoided where possible.
    """

    #: Expression for supported module names.
    expr = re.compile(r"message_ix_models\.(?P<name>(model|project)\..*)")

    @classmethod
    def find_spec(cls, name: str, path, target=None):
        from .common import HAS_MESSAGE_DATA

        if not HAS_MESSAGE_DATA:
            return None

        if match := cls.expr.match(name):
            # Construct the name for the actual module to load
            new_name = f"message_data.{match.group('name')}"
        else:
            return None

        try:
            # Get an import spec for the message_data submodule
            spec = util.find_spec(new_name)
        except ImportError:
            # `new_name` does not exist as a submodule of message_data
            return None
        else:  # pragma: no cover
            # NB Coverage ignored because message_data is not installed on GHA
            assert spec is not None and spec.origin is not None

            once(getLogger(__name__), INFO, f"Import {new_name!r} as {name!r}")

            # - Create a new spec that loads message_data.model.foo as if it were
            #   message_ix_models.model.foo
            # - Create a new loader that loads from the actual file with the desired
            #   name
            new_spec = ModuleSpec(
                name=name,
                loader=SourceFileLoader(fullname=name, path=spec.origin),
                origin=spec.origin,
            )
            # These can't be passed through the constructor
            new_spec.submodule_search_locations = spec.submodule_search_locations

            return new_spec


@dataclass(frozen=True)
class _PackageVersion:
    """Condition that the version of package `name` is at least `v_min`."""

    name: str
    v_min: str

    @functools.cache
    def __call__(self) -> str:
        v = python_version() if self.name == "python" else version(self.name)
        return f"{self.name} {v} < {self.v_min}" if parse(v) < parse(self.v_min) else ""


@dataclass(frozen=True)
class _Recurse:
    """Recurse to the minimum version decorator of `fully_qualified_name`."""

    fully_qualified_name: str

    @functools.cache
    def __call__(self) -> str:
        # Split the fully qualified name into a module name and item name
        module_name, _, name = self.fully_qualified_name.strip().rpartition(".")
        # Import the module → retrieve the item → retrieve its MVD
        return getattr(import_module(module_name), name)._mvd.check_versions()


class MinimumVersionDecorator:
    """Mark callable objects as requiring minimum version(s) of upstream packages.

    If the decorated object is called and any of condition(s) in `expr` is not met,
    :class:`.NotImplementedError` is raised with an informative message.

    The decorated object gains an attribute :py:`.minimum_version`, which can be used
    like :py:`pytest.mark.xfail()` to decorate test functions. This marks the test as
    XFAIL, raising :class:`.NotImplementedError` directly; indirectly
    :class:`.RuntimeError` or :class:`.AssertionError` (for instance, via :mod:`.click`
    test utilities or :mod:`genno`), or any of the classes given by the `raises`
    argument.

    See :func:`.prepare_reporter` / :func:`.test_prepare_reporter` for a usage example.

    Parameters
    ----------
    expr :
        Zero or more conditions like:

        1. "pkgA 1.2.3.post0; pkgB 2025.2", specifying minimum version of 1 or more
           packages.
        2. "python 3.10", specifying a minimum version of python.
        3. "message_ix_models.foo.bar.baz", recursively referring to the minimum version
           required by :py:`baz` in the module :py:`message_ix_models.foo.bar`. This
           object **must** also have been decorated with
           :class:`MinimumVersionDecorator`.
    """

    name: str
    conditions: list["Condition"]
    raises: list[type[Exception]]

    def __init__(
        self, *expr: str, raises: Optional[Iterable[type[Exception]]] = None
    ) -> None:
        self.raises = [NotImplementedError, AssertionError, RuntimeError]
        self.raises.extend(raises or ())

        # Assemble a list of Condition instances to be checked
        self.conditions = []
        for spec in chain(*[e.split(";") for e in expr]):
            try:
                # Split a string like "pkgA 1.2.3.post0"
                package, v_min = spec.strip().split(" ")
            except ValueError:
                # Failed → something like "message_ix_models.foo.bar.baz" → recurse
                c: "Condition" = _Recurse(spec)
            else:
                c = _PackageVersion(package, v_min)
            self.conditions.append(c)

    def check_versions(self) -> str:
        """Evaluate all the :attr:`conditions.

        Return :py:`""` if all pass, else a :class:`str` describing failed conditions.
        """
        return ", ".join(filter(None, [cond() for cond in self.conditions]))

    def raise_for_version(self) -> None:
        """Raise :class:`.NotImplementedError` if :meth:`check_versions` fails."""
        if result := self.check_versions():
            raise NotImplementedError(f"{self.name} with {result}.")

    def mark_test(self, obj):
        """Apply a pytest XFAIL mark to test function or class `obj`."""
        import pytest

        # Evaluate the conditions
        msg = self.check_versions()

        # Create the Mark
        mark = pytest.mark.xfail(
            condition=bool(msg),
            raises=tuple(self.raises),
            reason=f"Not supported with {msg}",
        )

        # Apply the mark to obj; return the result
        return mark(obj)

    def __call__(self, to_wrap: Callable) -> "MinimumVersionDecorated":
        """Wrap `to_wrap`."""
        # Store name for raise_for_version()
        self.name = f"{to_wrap.__module__}.{to_wrap.__name__}()"

        # Create a wrapper around `to_wrap`
        def wrapper(*args, **kwargs):
            self.raise_for_version()  # MinimumVersionDecorator
            return to_wrap(*args, **kwargs)

        # Apply update_wrapper() from the standard library
        functools.update_wrapper(wrapper, to_wrap)
        # Set property minimum_version that can be used to mark test functions/classes
        setattr(wrapper, "minimum_version", self.mark_test)
        assert hasattr(wrapper, "minimum_version")
        # Store a reference to the current MinimumVersionDecorator for use by _Recurse
        setattr(wrapper, "_mvd", self)

        return wrapper


#: Alias for :class:`.MinimumVersionDecorator`.
minimum_version = MinimumVersionDecorator
