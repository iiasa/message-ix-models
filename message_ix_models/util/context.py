"""Context and settings for :mod:`message_ix_models` code."""

import logging
from copy import deepcopy
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import ixmp
import message_ix
from click import BadOptionUsage

from .ixmp import parse_url

if TYPE_CHECKING:
    import message_ix_models.model.config
    import message_ix_models.report.config
    import message_ix_models.util.config

log = logging.getLogger(__name__)

#: List of Context instances, from first created to last.
_CONTEXTS: list["Context"] = []

#: Tuples containing:
#:
#: 1. Full name of module that contains a dataclass named :py:`Config`.
#: 2. :class:`Context` key where an instance of the Config class is stored.
#: 3. :any:`True` if such an instance should be created by default for every
#:    :class:`Context` instance.
#:
#:    This **should** be :any:`False` if creation of the Config instance is slow or has
#:    side effects, as this will occur even where the module (1) is not in use.
#: 4. :any:`True` if the dataclass fields of the Config class should be ‘aliased’, or
#:    directly available on Context instances.
#:
#:    This **should** be :any:`False` for all new modules/Config classes. Aliasing is
#:    provided only for backwards-compatible support of earlier code.
MODULE_WITH_CONFIG_DATACLASS: tuple[tuple[str, Optional[str], bool, bool], ...] = (
    ("message_ix_models.util.config", "core", True, True),
    ("message_ix_models.model.config", "model", True, True),
    ("message_ix_models.report.config", "report", True, False),
    ("message_ix_models.transport.config", "transport", False, False),
)


@lru_cache
def _alias() -> dict[str, str]:
    """Mapping from aliased keys to keys for the configuration dataclass.

    For instance, an entry :py`"regions": "model"` indicates that the key "regions"
    should be stored as :py:`Context.model.regions`.
    """
    from dataclasses import fields

    result = dict()
    for module_name, key, _, aliased in MODULE_WITH_CONFIG_DATACLASS:
        if not aliased:
            continue  # No aliases for this module/class
        # Retrieve the Config class given the module name
        cls = getattr(import_module(module_name), "Config")
        # Alias each of the fields of `cls` to the `key`
        result.update({f.name: key for f in fields(cls)})

    return result


class Context:
    """Context and settings for :mod:`message_ix_models` code."""

    # NB the docs contain a table of settings

    __slots__ = ("_values",)
    # Internal storage of keys and values
    _values: dict

    @classmethod
    def get_instance(cls, index=0) -> "Context":
        """Return a Context instance; by default, the first created.

        Parameters
        ----------
        index : int, optional
            Index of the Context instance to return, e.g. ``-1`` for the most recently
            created.
        """
        return _CONTEXTS[index]

    @classmethod
    def only(cls) -> "Context":
        """Return the only :class:`.Context` instance.

        Raises
        ------
        IndexError
            If there is more than one instance.
        """
        if len(_CONTEXTS) > 1:
            raise IndexError(f"ambiguous: {len(_CONTEXTS)} Context instances")
        return _CONTEXTS[0]

    def __init__(self, *args, **kwargs):
        if len(_CONTEXTS) == 0:
            log.info("Create root Context")

        # Create default instances of config dataclasses and handle associated keyword
        # arguments
        for module_name, key, default, aliased in MODULE_WITH_CONFIG_DATACLASS:
            if not default:
                continue  # Do not create this class by default
            # Retrieve the Config class given the module name
            cls = getattr(import_module(module_name), "Config")
            # Collect any kwargs aliased to attributes of this class
            values = self._collect_aliased_kw(key, kwargs) if aliased else {}
            # Create and store the class instance
            kwargs[key] = cls(**values)

        # Store keyword arguments on _values
        object.__setattr__(self, "_values", dict(*args, **kwargs))

        # Store a reference for get_instance()
        _CONTEXTS.append(self)

    @staticmethod
    def _collect_aliased_kw(base: str, data: dict) -> dict:
        """Return values from `data` which belong on `base` according to func:`_alias`.

        The returned values are removed from `data`.
        """
        # Collect values where the aliased key is in `data` AND the targeted config
        # class is `base`
        result = {
            k: data.pop(k)
            for k, _ in filter(
                lambda x: x[0] in data and x[1] == base, _alias().items()
            )
        }

        if result:
            log.warning(
                f"Create a Config instance instead of passing {list(result.keys())} to "
                "Context()"
            )

        return result

    def _dealias(self, key: str) -> Any:
        """De-alias `key`.

        If `key` (per :func:`_alias`) is an alias for an attribute of a configuration
        dataclass, return the instance of that class. Otherwise, return :attr:`_values`.
        """
        base_key = _alias().get(key, "_values")

        if base_key == "_values":
            return self._values
        else:
            # Warn about direct reference to aliased attributes
            if base_key not in {"core", "model"}:  # pragma: no cover
                log.warnings(f"Use Context.{base_key}.{key} instead of Context.{key}")
            return self._values[base_key]

    # General item access
    def get(self, key: str, default: Optional = None):
        """Retrieve the value for `key`."""
        target = self._dealias(key)
        if isinstance(target, dict):
            return target[key]
        else:
            return getattr(target, key, default)

    def set(self, key: str, value: Any) -> None:
        """Change the stored value for `key`."""
        target = self._dealias(key)
        if isinstance(target, dict):
            target[key] = value
        else:
            setattr(target, key, value)

    # Typed access to particular items
    # These SHOULD include all the keys from MODULE_WITH_CONFIG_DATACLASS
    @property
    def core(self) -> "message_ix_models.util.config.Config":
        return self._values["core"]

    @property
    def model(self) -> "message_ix_models.model.config.Config":
        return self._values["model"]

    @property
    def report(self) -> "message_ix_models.report.config.Config":
        return self._values["report"]

    # Dict-like behaviour
    def __contains__(self, name: str) -> bool:
        return name in self._values

    def __delitem__(self, name: str) -> None:
        del self._values[name]

    def __getitem__(self, name):
        return self.get(name)

    def __len__(self) -> int:
        return len(self._values)

    def __setitem__(self, name, value) -> None:
        self.set(name, value)

    _Missing = object()

    def pop(self, name, default=_Missing):
        return (
            self._values.pop(name)
            if default is self._Missing
            else self._values.pop(name, default)
        )

    def setdefault(self, name, value):
        return self._values.setdefault(name, value)

    def update(self, arg=None, **kwargs):
        # Force update() to use set(), above
        for k, v in dict(*filter(None, [arg]), **kwargs).items():
            self.set(k, v)

    def __deepcopy__(self, memo):
        # Hide "_mp" from the copy
        result = Context()
        result.update((k, deepcopy(v)) for k, v in self._values.items() if k != "_mp")

        return result

    def __eq__(self, other) -> bool:
        # Don't compare contents, only identity, for _CONTEXTS.index()
        if not isinstance(other, Context):
            return NotImplemented
        return id(self) == id(other)

    def __getattr__(self, name):
        if name == "_values":
            return object.__getattribute__(self, name)
        try:
            return self.get(name)
        except KeyError:
            raise AttributeError(name) from None

    def __repr__(self):
        return f"<{self.__class__.__name__} object at {id(self)} with {len(self)} keys>"

    def __setattr__(self, name, value):
        self.set(name, value)

    def delete(self):
        """Hide the current Context from future :meth:`.get_instance` calls."""
        # Index of the *last* matching instance
        index = len(_CONTEXTS) - 1 - list(reversed(_CONTEXTS)).index(self)

        if index > 0:
            _CONTEXTS.pop(index)
        else:  # pragma: no cover
            # The `session_context` fixture means this won't occur during tests
            log.warning("Won't delete the only Context instance")

        self.close_db()

    def write_debug_archive(self) -> None:
        """Write an archive containing the files listed in :attr:`.debug_paths`.

        The archive file name is constructed using :func:`.unique_id` and appears in a
        :file:`debug` subdirectory under the :ref:`local data path <local-data>`.

        The archive also contains a file :file:`command.txt` that gives the full
        command-line used to invoke :program:`mix-models`.
        """
        from zipfile import ZIP_DEFLATED, ZipFile

        from .click import format_sys_argv, unique_id

        # Output file
        target = self.core.local_data.joinpath("debug", f"{unique_id()}.zip")
        log.info(f"Write to: {target}")

        target.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(target, mode="w", compression=ZIP_DEFLATED) as zf:
            # Write a file that contains the CLI invocation
            zf.writestr("command.txt", format_sys_argv())

            # Write the identified files
            for dp in self.core.debug_paths:
                if not dp.exists():
                    log.info(f"Not found: {dp}")
                    continue

                zf.write(dp, arcname=dp.relative_to(self.core.local_data))
                # log.info(debug_path)

    # Particular methods of Context
    def asdict(self) -> dict:
        """Return a :func:`.deepcopy` of the Context's values as a :class:`dict`."""
        return {k: deepcopy(v) for k, v in self._values.items()}

    def clone_to_dest(self, create=True) -> message_ix.Scenario:
        """Return a scenario based on the ``--dest`` command-line option.

        Parameters
        ----------
        create : bool, optional
            If :obj:`True` (the default) and the base scenario does not exist, a bare
            RES scenario is created. Otherwise, an exception is raised.

        Returns
        -------
        Scenario
            To prevent the scenario from being garbage collected, keep a reference to
            its Platform:

            .. code-block: python

               s = context.clone_to_dest()
               mp = s.platform

        See also
        --------
        create_res
        """
        cfg = self.core
        if not cfg.dest_scenario:
            # No information on the destination; try to parse a URL, storing the keys
            # dest_platform and dest_scenario.
            self.handle_cli_args(
                url=cfg.dest, _store_as=("dest_platform", "dest_scenario")
            )

        try:
            # Get the base scenario, e.g. from the --url CLI argument
            scenario_base = self.get_scenario()

            # By default, clone to the same platform
            mp_dest = scenario_base.platform

            try:
                if cfg.dest_platform["name"] != mp_dest.name:
                    # Different platform
                    # Not tested; current test fixtures make it difficult to create
                    # *two* temporary platforms simultaneously
                    mp_dest = ixmp.Platform(**cfg.dest_platform)  # pragma: no cover
            except KeyError:
                pass
        except Exception as e:
            log.info("Base scenario not given or found")
            log.debug(f"{type(e).__name__}: {e}")

            if not create:
                log.error("and create=False")
                raise

            # Create a bare RES to be the base scenario

            from message_ix_models.model.bare import create_res

            # Create on the destination platform
            ctx = deepcopy(self)
            ctx.core.platform_info.update(cfg.dest_platform)
            scenario_base = create_res(ctx)

            # Clone to the same platform
            mp_dest = scenario_base.platform

        # Clone
        log.info(f"Clone to {repr(cfg.dest_scenario)}")
        return scenario_base.clone(platform=mp_dest, **cfg.dest_scenario)

    def close_db(self):
        try:
            mp = self.pop("_mp")
            mp.close_db()
        except KeyError:
            pass

    def get_cache_path(self, *parts) -> Path:
        """Return a path to a local cache file, i.e. within :attr:`.Config.cache_path`.

        The directory containing the resulting path is created if it does not already
        exist.
        """
        result = self.core.cache_path.joinpath(*parts)
        result.parent.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        return result

    def get_local_path(self, *parts: str, suffix=None) -> Path:
        """Return a path under :attr:`.Config.local_data`.

        Parameters
        ==========
        parts :
            Path fragments, for instance directories, passed to
            :meth:`~.pathlib.PurePath.joinpath`.
        suffix :
            File name suffix including a "."—for instance, ".csv"—passed to
            :meth:`~.pathlib.PurePath.with_suffix`.
        """
        result = self.core.local_data.joinpath(*parts)
        return result.with_suffix(suffix) if suffix else result

    def get_platform(self, reload=False) -> ixmp.Platform:
        """Return a :class:`.Platform` from :attr:`.Config.platform_info`.

        When used through the CLI, :attr:`.Config.platform_info` is a 'base' platform as
        indicated by the --url or --platform  options.

        If a Platform has previously been instantiated with :meth:`get_platform`, the
        same object is returned unless `reload=True`.
        """
        if not reload:
            # Return an existing Platform, if any
            try:
                return self["_mp"]
            except KeyError:
                pass

        # Close any existing Platform, e.g. to reload it
        try:
            self["_mp"].close_db()
            del self["_mp"]
        except KeyError:
            pass

        # Create a Platform
        self["_mp"] = ixmp.Platform(**self.core.platform_info)
        return self["_mp"]

    def get_scenario(self) -> message_ix.Scenario:
        """Return a :class:`.Scenario` from :attr:`~.Config.scenario_info`.

        When used through the CLI, :attr:`~.Config.scenario_info` is a ‘base’ scenario
        for an operation, indicated by the ``--url`` or
        ``--platform/--model/--scenario`` options.
        """
        return message_ix.Scenario(self.get_platform(), **self.core.scenario_info)

    def set_scenario(self, scenario: message_ix.Scenario) -> None:
        """Update :attr:`.Config.scenario_info` to match an existing `scenario`.

        :attr:`.Config.url` is also updated.
        """
        self.core.scenario_info.update(
            model=scenario.model, scenario=scenario.scenario, version=scenario.version
        )
        try:
            url = scenario.url
        except AttributeError:
            # Compatibility with ixmp <3.5
            url = f"{scenario.model}/{scenario.scenario}/{scenario.version}"
        self.core.url = f"ixmp://{scenario.platform.name}/{url}"

    def handle_cli_args(
        self,
        url=None,
        platform=None,
        model_name=None,
        scenario_name=None,
        version=None,
        local_data=None,
        verbose=False,
        _store_as=("platform_info", "scenario_info"),
    ):
        """Handle command-line arguments.

        May update the :attr:`.Config.local_data`, :attr:`~.Config.platform_info`,
        :attr:`~.Config.scenario_info`, and/or :attr:`~.Config.url` settings.
        """
        self.core.verbose = verbose

        # Store the path to command-specific data and metadata
        if local_data:
            self.core.local_data = local_data

        # References to the Context settings to be updated
        platform_info = getattr(self.core, _store_as[0])
        scenario_info = getattr(self.core, _store_as[1])

        # Store information for the target Platform
        if url:
            if platform or model_name or scenario_name or version:
                raise BadOptionUsage(
                    "--platform --model --scenario and/or --version",
                    " redundant with --url",
                )

            self.core.url = url
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

    def use_defaults(self, settings):
        """Update from `settings`."""
        for setting, info in settings.items():
            if setting not in self:
                log.info(f"Use default {setting}={info[0]}")

            value = self.setdefault(setting, info[0])

            if value not in info:
                raise ValueError(f"{setting} must be in {info}; got {value}")
