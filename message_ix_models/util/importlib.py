import re
from importlib import util
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader


class MessageDataFinder(MetaPathFinder):
    """Load model and project code from :mod:`message_data`."""

    # Expression for supported module names
    expr = re.compile(r"message_ix_models\.(?P<name>(model|project)\..*)")

    # NB coverage is excluded, because the message-ix-models test suite does not
    #    install/use message-data.
    @classmethod
    def find_spec(cls, name, path, target=None):  # pragma: no cover
        match = cls.expr.match(name)
        try:
            # Construct the name for the actual module to load
            new_name = f"message_data.{match.group('name')}"
        except AttributeError:
            # `match` was None; unsupported. Let the importlib defaults take over.
            return None

        # Get an import spec for the message_data submodule
        spec = util.find_spec(new_name)
        if not spec:
            return None

        # Create a new spec that loads message_data.model.foo as if it were
        # message_ix_models.model.foo
        new_spec = ModuleSpec(
            name=name,
            # Create a new loader that loads from the actual file with the desired name
            loader=SourceFileLoader(fullname=name, path=spec.origin),
            origin=spec.origin,
        )
        # These can't be passed through the constructor
        new_spec.submodule_search_locations = spec.submodule_search_locations

        return new_spec
