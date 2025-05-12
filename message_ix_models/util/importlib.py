"""Load model and project code from :mod:`message_data`."""

import re
from importlib import util
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from logging import INFO, getLogger

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
