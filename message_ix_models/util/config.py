import logging
import os
import pickle
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass, replace
from hashlib import blake2s
from pathlib import Path
from typing import TYPE_CHECKING, Any, Hashable, Optional, Union, cast

import ixmp
from ixmp.util import parse_url

from ._dataclasses import asdict
from .scenarioinfo import ScenarioInfo

if TYPE_CHECKING:
    import message_ix
    from ixmp.types import PlatformInfo

log = logging.getLogger(__name__)

ixmp.config.register("no message_data", bool, False)
ixmp.config.register("message local data", Path, Path.cwd())


def _cache_path_factory() -> Path:
    """Default value for :attr:`.Config.cache_path."""
    from platformdirs import user_cache_path

    return (
        Path(
            os.environ.get("MESSAGE_MODELS_CACHE", "")
            or user_cache_path("message-ix-models", ensure_exists=True)
        )
        .expanduser()
        .resolve()
    )


def _local_data_factory() -> Path:
    """Default value for :attr:`.Config.local_data."""
    from platformdirs import user_data_path

    return (
        Path(
            os.environ.get("MESSAGE_LOCAL_DATA", "")
            or ixmp.config.get("message local data")
            or user_data_path("message-ix-models")
        )
        .expanduser()
        .resolve()
    )


@dataclass
class ConfigHelper:
    """Mix-in for :class:`dataclass`-based configuration classes.

    This provides methods :meth:`read_file`, :meth:`replace`, and :meth:`from_dict` that
    help to use :class:`dataclass` classes for handling :mod:`message_ix_models`
    configuration.

    All 3 methods take advantage of name manipulations: the characters "-" and " " are
    replaced with underscores ("_"). This allows to write the names of attributes in
    legible ways—e.g. "attribute name" or “attribute-name” instead of "attribute_name"—
    in configuration files and/or code.

    It also add :meth:`hexdigest`.
    """

    @classmethod
    def _fields(cls) -> set[str]:
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
            of the dataclass raise a ValueError. Otherwise, a message is logged.
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
                    # https://github.com/python/mypy/issues/15843
                    # TODO Check that fix is available in mypy 1.7.x; remove
                    value = replace(existing, **value)  # type: ignore [misc]
            setattr(self, key, value)

    def replace(self, **kwargs):
        """Like :func:`dataclasses.replace` with name manipulation."""
        return replace(
            self,
            **{k: v for k, v in self._munge_dict(kwargs, "raise", "keyword argument")},
        )

    def update(self, **kwargs):
        """Update attributes in-place.

        Raises
        ------
        AttributeError
            Any of the `kwargs` are not fields in the data class.
        """
        # TODO use _munge_dict(); allow a positional argument
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise AttributeError(k)
            setattr(self, k, v)

    @classmethod
    def from_dict(cls, data: Mapping):
        """Construct an instance from `data` with name manipulation."""
        return cls(**{k: v for k, v in cls._munge_dict(data, "raise", "mapping key")})

    def hexdigest(self, length: int = -1) -> str:
        """Return a hex digest that is unique for distinct settings on the instance.

        Uses :func:`dataclasses.asdict`. This means that if the names of fields defined
        by a subclass change—including if fields are added or removed—the result will
        differ.

        Returns
        -------
        str
            If `length` is greater than 0, a string of this length; otherwise a
            32-character string from :meth:`.blake2s.hexdigest`.
        """
        # - Dump the dataclass instance to nested, sorted tuples. This is used instead
        #   of dataclass.astuple() which allows e.g. units to pass as a (possibly
        #   unsorted) dict.
        # - Pickle this collection.
        # - Hash.
        h = blake2s(
            pickle.dumps(asdict(self, dict_factory=lambda kv: tuple(sorted(kv))))
        )
        # Return the whole digest or a part
        return h.hexdigest()[0 : length if length > 0 else h.digest_size]


@dataclass
class Config:
    """Core/top-level settings for :mod:`message_ix_models` and :mod:`message_data`."""

    # See cache_path(), below
    _cache_path: Path = field(default_factory=_cache_path_factory)

    # See cache_skip(), below
    _cache_skip: bool = False

    #: Paths of files containing debug outputs. See
    #: :meth:`.Context.write_debug_archive`.
    debug_paths: Sequence[Path] = field(default_factory=list)

    #: Like :attr:`url`, used by e.g. :meth:`.clone_to_dest`.
    dest: Optional[str] = None

    # NB the below works around python/mypy#5723
    #: Like :attr:`platform_info`, used by e.g. :meth:`.clone_to_dest`.
    dest_platform: "PlatformInfo" = field(
        default_factory=lambda: cast("PlatformInfo", dict())
    )

    #: Like :attr:`scenario_info`, used by e.g. :meth:`.clone_to_dest`.
    dest_scenario: MutableMapping[str, str] = field(default_factory=dict)

    #: Whether an operation should be carried out, or only previewed. Different modules
    #: will respect :attr:`dry_run` in distinct ways, if at all, and **should** document
    #: behaviour.
    dry_run: bool = False

    #: Base path for :ref:`system-specific data <local-data>`, i.e. as given by the
    #: :program:`--local-data` CLI option or `message local data` key in the ixmp
    #: configuration file.
    local_data: Path = field(default_factory=_local_data_factory)

    # NB the below works around python/mypy#5723
    #: Keyword arguments—especially `name`—for the :class:`ixmp.Platform` constructor,
    #: from the :program:`--platform` or :program:`--url` CLI option.
    platform_info: "PlatformInfo" = field(
        default_factory=lambda: cast("PlatformInfo", dict())
    )

    # Private reference to an ixmp.Platform
    _mp: Optional["ixmp.Platform"] = None

    #: Keyword arguments—`model`, `scenario`, and optionally `version`—for the
    #: :class:`ixmp.Scenario` constructor, as given by the :program:`--model`/
    #: :program:`--scenario` or :program:`--url` CLI options.
    scenario_info: MutableMapping[str, Optional[Union[int, str]]] = field(
        default_factory=dict
    )

    #: Like `scenario_info`, but a list for operations affecting multiple scenarios.
    scenarios: list[ScenarioInfo] = field(default_factory=list)

    #: A scenario URL, e.g. as given by the :program:`--url` CLI option.
    url: Optional[str] = None

    #: Flag for causing verbose output to logs or stdout. Different modules will respect
    #: :attr:`verbose` in distinct ways.
    verbose: bool = False

    def __deepcopy__(self, memo):
        # Hide "_mp" from the copy
        mp = self._mp
        self._mp = None

        # Create a copy of other field values; invokes deepcopy() on each
        data = asdict(self)

        # Restore reference
        self._mp = mp

        # Create the copy
        return type(self)(**data)

    @property
    def cache_path(self) -> Path:
        """Base path for cached data.

        By default, the directory :file:`message-ix-models` within the directory given
        by :func:`.platformdirs.user_cache_path`. This may be something like
        :file:`$HOME/.cache/message-ix-models/`. The :program:`--cache-path` CLI option
        modifies this value.

        See also :ref:`cache-data`.
        """
        return self._cache_path

    @cache_path.setter
    def cache_path(self, value: Path) -> None:
        from . import cache

        cache.COMPUTER.graph["config"]["cache_path"] = self._cache_path = value

    @property
    def cache_skip(self) -> bool:
        """:any:`True` to skip caching. See :func:`.cached`."""
        return self._cache_skip

    @cache_skip.setter
    def cache_skip(self, value: bool) -> None:
        from . import cache

        cache.COMPUTER.graph["config"]["cache_skip"] = self._cache_skip = value

    def close_db(self) -> None:
        """Close the database connection for the Platform given by :meth:`get_platform`.

        If no such Platform exists or the connection is already closed, does nothing.
        """
        if self._mp:
            self._mp.close_db()
            self._mp = None

    def get_cache_path(self, *parts) -> Path:
        """Return a path to a local cache file, i.e. within :attr:`cache_path`.

        The directory containing the resulting path is created if it does not already
        exist.
        """
        assert self.cache_path
        result = self.cache_path.joinpath(*parts)
        result.parent.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        return result

    def get_local_path(self, *parts: str, suffix=None) -> Path:
        """Return a path under :attr:`local_data`.

        Parameters
        ==========
        parts :
            Path fragments, for instance directories, passed to
            :meth:`~.pathlib.PurePath.joinpath`.
        suffix :
            File name suffix including a "."—for instance, ".csv"—passed to
            :meth:`~.pathlib.PurePath.with_suffix`.
        """
        result = self.local_data.joinpath(*parts)
        return result.with_suffix(suffix) if suffix else result

    def get_platform(self, reload: bool = False) -> "ixmp.Platform":
        """Return a :class:`.Platform` from :attr:`platform_info`.

        When used through the CLI, :attr:`platform_info` is a 'base' platform as
        indicated by the --url or --platform  options.

        If a Platform has previously been instantiated with :meth:`get_platform`, the
        same object is returned unless `reload` is :any:`True`.
        """
        # Return an existing Platform, if any
        if not reload and self._mp:
            return self._mp

        # Close any existing Platform, e.g. to reload it
        if self._mp:
            self._mp.close_db()
            self._mp = None

        # Create a Platform
        self._mp = ixmp.Platform(**self.platform_info)
        return self._mp

    def get_scenario(self) -> "message_ix.Scenario":
        """Return a :class:`.Scenario` from :attr:`scenario_info`.

        When used through the CLI, :attr:`scenario_info` is a ‘base’ scenario for an
        operation, indicated by the ``--url`` or ``--platform/--model/--scenario``
        options.
        """
        import message_ix

        return message_ix.Scenario(self.get_platform(), **self.scenario_info)

    def handle_cli_args(
        self,
        url: Optional[str] = None,
        platform: Optional[str] = None,
        model_name: Optional[str] = None,
        scenario_name: Optional[str] = None,
        version: Optional[str] = None,
        local_data: Optional[str] = None,
        verbose: bool = False,
        _store_as: tuple[str, str] = ("platform_info", "scenario_info"),
    ):
        """Handle command-line arguments.

        May update the :attr:`local_data`, :attr:`platform_info`,
        :attr:`scenario_info`, and/or :attr:`url` settings.
        """
        from click import BadOptionUsage

        self.verbose = verbose

        # Store the path to command-specific data and metadata
        if local_data:
            self.local_data = Path(local_data)

        # References to the Context settings to be updated
        platform_info = getattr(self, _store_as[0])
        scenario_info = getattr(self, _store_as[1])

        # Store information for the target Platform
        if url:
            if platform or model_name or scenario_name or version:
                raise BadOptionUsage(
                    "--platform --model --scenario and/or --version",
                    " redundant with --url",
                )

            self.url = url
            urlinfo = parse_url(url)
            platform_info.update(urlinfo[0])
            scenario_info.update(urlinfo[1])
        elif platform:
            platform_info["name"] = platform

        # Store information about the target Scenario
        if model_name:
            scenario_info["model"] = model_name
        if scenario_name:
            scenario_info["scenario"] = scenario_name
        if version:
            scenario_info["version"] = version

    def set_scenario(self, scenario: "message_ix.Scenario") -> None:
        """Update :attr:`scenario_info` to match an existing `scenario`.

        :attr:`url` is also updated.
        """
        self.scenario_info.update(
            model=scenario.model, scenario=scenario.scenario, version=scenario.version
        )
        try:
            url = scenario.url
        except AttributeError:
            # Compatibility with ixmp <3.5
            url = f"{scenario.model}/{scenario.scenario}/{scenario.version}"
        self.url = f"ixmp://{scenario.platform.name}/{url}"

    def write_debug_archive(self) -> None:
        """Write an archive containing the files listed in :attr:`debug_paths`.

        The archive file name is constructed using :func:`.unique_id` and appears in a
        :file:`debug` subdirectory under the :ref:`local data path <local-data>`.

        The archive also contains a file :file:`command.txt` that gives the full
        command-line used to invoke :program:`mix-models`.
        """
        from zipfile import ZIP_DEFLATED, ZipFile

        from .click import format_sys_argv, unique_id

        # Output file
        target = self.local_data.joinpath("debug", f"{unique_id()}.zip")
        log.info(f"Write to: {target}")

        target.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(target, mode="w", compression=ZIP_DEFLATED) as zf:
            # Write a file that contains the CLI invocation
            zf.writestr("command.txt", format_sys_argv())

            # Write the identified files
            for dp in self.debug_paths:
                if not dp.exists():
                    log.info(f"Not found: {dp}")
                    continue

                zf.write(dp, arcname=dp.relative_to(self.local_data))
                # log.info(debug_path)
