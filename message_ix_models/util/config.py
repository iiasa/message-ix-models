import logging
import os
from dataclasses import dataclass, field, fields, is_dataclass, replace
from pathlib import Path
from typing import Any, Hashable, Mapping, MutableMapping, Optional, Sequence, Set

import ixmp

log = logging.getLogger(__name__)

ixmp.config.register("message local data", Path, Path.cwd())


def _local_data_factory():
    """Default values for :attr:`.Config.local_data."""
    return (
        Path(
            os.environ.get("MESSAGE_LOCAL_DATA", None)
            or ixmp.config.get("message local data")
        )
        .expanduser()
        .resolve()
    )


@dataclass
class ConfigHelper:
    """Mix-in for :class:`dataclass`-based configuration classes.

    This provides 3 methods—:meth:`read_file`, :meth:`replace`, and :meth:`from_dict`—
    that help to use :class:`dataclass` classes for handling :mod:`message_ix_models`
    configuration.

    All 3 methods take advantage of name manipulations: the characters "-" and " " are
    replaced with underscores ("_"). This allows to write the names of attributes in
    legible ways—e.g. "attribute name" or “attribute-name” instead of "attribute_name"—
    in configuration files and/or code.
    """

    @classmethod
    def _fields(cls) -> Set[str]:
        """Names of fields in `cls`."""
        result = set(dir(cls))
        if is_dataclass(cls):
            result |= set(map(lambda f: f.name, fields(cls)))
        return result

    @classmethod
    def _canonical_name(cls, name: Hashable) -> Optional[str]:
        """Canonicalize a name into a valid Python attribute name."""
        _name = str(name).replace(" ", "_").replace("-", "_")
        return _name if _name in cls._fields() else None

    @classmethod
    def _munge_dict(cls, data: Mapping[Hashable, Any], fail: str, kind: str):
        for key, value in data.items():
            name = cls._canonical_name(key)

            if name:
                yield name, value
            else:
                msg = f"{cls.__name__} has no attribute for {kind} {key!r}"
                if fail == "raise":
                    raise ValueError(msg)
                else:
                    log.info(f"{msg}; ignored")

    def read_file(self, path: Path, fail="raise") -> None:
        """Update configuration from file.

        Parameters
        ----------
        path
            to a :file:`.yaml` file containing a top-level mapping.
        fail : str
            if "raise" (the default), any names in `path` which do not match attributes
            of the dataclass raise a ValueError. Ottherwise, a message is logged.
        """
        if path.suffix == ".yaml":
            import yaml

            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
        elif path.suffix == ".json":
            import json

            with open(path) as f:
                data = json.load(f)
        else:
            raise NotImplementedError(f"Read from {path.suffix}")

        for key, value in self._munge_dict(data, fail, "file section"):
            existing = getattr(self, key, None)
            if is_dataclass(existing) and not isinstance(existing, type):
                # Attribute value is also a dataclass; update it recursively
                if isinstance(existing, ConfigHelper):
                    # Use name manipulation on the attribute value also
                    value = existing.replace(**value)
                elif not isinstance(existing, type):
                    value = replace(existing, **value)
            setattr(self, key, value)

    def replace(self, **kwargs):
        """Like :func:`dataclasses.replace` with name manipulation."""
        return replace(
            self,
            **{k: v for k, v in self._munge_dict(kwargs, "raise", "keyword argument")},
        )

    @classmethod
    def from_dict(cls, data: Mapping):
        """Construct an instance from `data` with name manipulation."""
        return cls(**{k: v for k, v in cls._munge_dict(data, "raise", "mapping key")})


@dataclass
class Config:
    """Top-level configuration for :mod:`message_ix_models` and :mod:`message_data`."""

    #: Base path for :ref:`system-specific data <local-data>`, i.e. as given by the
    #: :program:`--local-data` CLI option or `message local data` key in the ixmp
    #: configuration file.
    local_data: Path = field(default_factory=_local_data_factory)

    #: Keyword arguments—especially `name`—for the :class:`ixmp.Platform` constructor,
    #: from the :program:`--platform` or :program:`--url` CLI option.
    platform_info: MutableMapping[str, str] = field(default_factory=dict)

    #: Keyword arguments—`model`, `scenario`, and optionally `version`—for the
    #: :class:`ixmp.Scenario` constructor, as given by the :program:`--model`/
    #: :program:`--scenario` or :program:`--url` CLI options.
    scenario_info: MutableMapping[str, str] = field(default_factory=dict)

    #: Like :attr:`platform_info`, used by e.g. :meth:`.clone_to_dest`.
    dest_platform: MutableMapping[str, str] = field(default_factory=dict)

    #: Like :attr:`scenario_info`, used by e.g. :meth:`.clone_to_dest`.
    dest_scenario: MutableMapping[str, str] = field(default_factory=dict)

    #: A scenario URL, e.g. as given by the :program:`--url` CLI option.
    url: Optional[str] = None

    #: Like :attr:`url`, used by e.g. :meth:`.clone_to_dest`.
    dest: Optional[str] = None

    #: Base path for cached data, e.g. as given by the :program:`--cache-path` CLI
    #: option. Default: :file:`{local_data}/cache/`.
    cache_path: Optional[str] = None

    #: Paths of files containing debug outputs. See :meth:`Context.write_debug_archive`.
    debug_paths: Sequence[str] = field(default_factory=list)

    #: Whether an operation should be carried out, or only previewed. Different modules
    #: will respect :attr:`dry_run` in distinct ways, if at all, and **should** document
    #: behaviour.
    dry_run: bool = False

    #: Flag for causing verbose output to logs or stdout. Different modules will respect
    #: :attr:`verbose` in distinct ways.
    verbose: bool = False
