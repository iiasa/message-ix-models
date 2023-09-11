"""Context and settings for :mod:`message_ix_models` code."""
import logging
from copy import deepcopy
from dataclasses import fields
from pathlib import Path
from typing import List

import ixmp
import message_ix
from click import BadOptionUsage

from .config import Config

log = logging.getLogger(__name__)

#: List of Context instances, from first created to last.
_CONTEXTS: List["Context"] = []


# Configuration keys which can be accessed directly on Context.
_ALIAS = dict()
_ALIAS.update({f.name: "core" for f in fields(Config)})


def _dealiased(base: str, data: dict) -> dict:
    """Separate values from `data` which belong on `base` according to `_ALIAS`."""
    result = {}
    for name, path in filter(lambda ap: ap[1] == base, _ALIAS.items()):
        try:
            result[name] = data.pop(name)
        except KeyError:
            pass

    if len(result):
        log.warning(
            f"Create a Config instance instead of passing {list(result.keys())} to"
            " Context()"
        )

    return result


class Context(dict):
    """Context and settings for :mod:`message_ix_models` code."""

    # NB the docs contain a table of settings

    @classmethod
    def get_instance(cls, index=0) -> "Context":
        """Return a Context instance; by default, the first created.

        Parameters
        ----------
        index : int, *optional*
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
        from message_ix_models.model import Config as ModelConfig

        if len(_CONTEXTS) == 0:
            log.info("Create root Context")

        # Handle keyword arguments going to known config dataclasses
        kwargs["core"] = Config(**_dealiased("core", kwargs))
        kwargs["model"] = ModelConfig(**_dealiased("model", kwargs))

        # Store any keyword arguments
        super().__init__(*args, **kwargs)

        # Store a reference for get_instance()
        _CONTEXTS.append(self)

    def _dealias(self, name):
        base = _ALIAS[name]

        # Warn about direct reference to aliased attributes
        if base not in {"core", "model"}:  # pragma: no cover
            log.warnings(f"Use Context.{base}.{name} instead of Context.{name}")

        return getattr(self, base), name

    # Item access
    def __getitem__(self, name):
        try:
            return getattr(*self._dealias(name))
        except KeyError:
            return super().__getitem__(name)

    def __setitem__(self, name, value):
        try:
            return setattr(*self._dealias(name), value)
        except KeyError:
            super().__setitem__(name, value)

    def update(self, arg=None, **kwargs):
        # Force update() to use the __setitem__ above
        for k, v in dict(*filter(None, [arg]), **kwargs).items():
            self.__setitem__(k, v)

    # Attribute access
    def __setattr__(self, name, value):
        self[name] = value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __deepcopy__(self, memo):
        mp = self.pop("_mp", None)

        result = deepcopy(super(), memo)

        if mp is not None:
            self._mp = mp

        _CONTEXTS.append(result)

        return result

    def __repr__(self):
        return f"<{self.__class__.__name__} object at {id(self)} with {len(self)} keys>"

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

    def clone_to_dest(self, create=True) -> message_ix.Scenario:
        """Return a scenario based on the ``--dest`` command-line option.

        Parameters
        ----------
        create : bool, *optional*
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
        # Construct relative to local_data if cache_path is not defined
        base = self.core.cache_path or self.core.local_data.joinpath("cache")

        result = base.joinpath(*parts)

        # Ensure the directory exists
        result.parent.mkdir(parents=True, exist_ok=True)

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
        """Return a |Platform| from :attr:`.Config.platform_info`.

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
        """Return a |Scenario| from :attr:`~.Config.scenario_info`.

        When used through the CLI, :attr:`~.Config.scenario_info` is a ‘base’ scenario
        for an operation, indicated by the ``--url`` or
        ``--platform/--model/--scenario`` options.
        """
        return message_ix.Scenario(self.get_platform(), **self.core.scenario_info)

    def set_scenario(self, scenario: message_ix.Scenario) -> None:
        """Update :attr:`.Config.scenario_info` to match an existing `scenario`."""
        self.core.scenario_info.update(
            model=scenario.model, scenario=scenario.scenario, version=scenario.version
        )

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

        May update the :attr:`.Config.local_data`, :attr:`.Config.platform_info`,
        :attr:`.Config.scenario_info`, and/or :attr:`.url` settings.
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
            urlinfo = ixmp.utils.parse_url(url)
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
