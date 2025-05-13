"""Load model and project code from :mod:`message_data`."""

import re
from collections.abc import Callable, Iterable
from functools import update_wrapper
from importlib import util
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.metadata import version
from logging import INFO, getLogger
from typing import Optional

from ._logging import once


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


def minimum_version(
    expr: str, raises: Optional[Iterable[type[Exception]]] = None
) -> Callable:
    """Decorator for functions that require a minimum version of some upstream package.

    If the decorated function is called and the condition in `expr` is not met,
    :class:`.NotImplementedError` is raised with an informative message.

    The decorated function gains an attribute :py:`.minimum_version`, a pytest
    MarkDecorator that can be used on associated test code. This marks the test as
    XFAIL, raising :class:`.NotImplementedError` (directly); :class:`.RuntimeError` or
    :class:`.AssertionError` (for instance, via :mod:`.click` test utilities), or any
    of the classes given in the `raises` argument.

    See :func:`.prepare_reporter` / :func:`.test_prepare_reporter` for a usage example.

    Parameters
    ----------
    expr :
        Like "pkgA 1.2.3.post0; pkgB 2025.2". The condition for the decorated function
        is that the installed version must be equal to or greater than this version.
    """
    from platform import python_version

    from packaging.version import parse

    # Handle `expr`, updating `condition` and `message`
    condition, message = False, " with "
    for spec in expr.split(";"):
        package, v_min = spec.strip().split(" ")
        v_package = python_version() if package == "python" else version(package)
        if parse(v_package) < parse(v_min):
            condition = True
            message += f"{package} {v_package} < {v_min}"

    # Create the decorator
    def decorator(func):
        name = f"{func.__module__}.{func.__name__}()"

        # Wrap `func`
        def wrapper(*args, **kwargs):
            if condition:
                raise NotImplementedError(f"{name}{message}.")
            return func(*args, **kwargs)

        update_wrapper(wrapper, func)

        try:
            import pytest

            # Create a MarkDecorator and store as an attribute of "wrapper"
            setattr(
                wrapper,
                "minimum_version",
                pytest.mark.xfail(
                    condition=condition,
                    raises=(
                        NotImplementedError,  # Raised directly, above
                        AssertionError,  # e.g. through CliRunner.assert_exit_0()
                        RuntimeError,  # e.g. through genno.Computer
                    )
                    + tuple(raises or ()),  # Other exception classes
                    reason=f"Not supported{message}",
                ),
            )
        except ImportError:
            pass  # Pytest not present; testing is not happening

        return wrapper

    return decorator
