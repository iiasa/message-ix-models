from copy import deepcopy
from pathlib import Path
import logging
import os

from click import BadOptionUsage
import ixmp
import message_ix
import pint
import xarray as xr
import yaml

log = logging.getLogger(__name__)

_CONTEXTS = []


class Context(dict):
    """Context and settings for :mod:`message_data` code.

    Context is a subclass of :class:`dict`, so common methods like :meth:`~dict.copy`
    and :meth:`~dict.setdefault` may be used to handle settings. To be forgiving, it
    also provides attribute access; ``context.foo`` is equivalent to ``context["foo"]``.

    Context provides additional methods to do common tasks that depend on configurable
    settings:

    .. autosummary::
       get_config_file
       get_path
       get_platform
       get_scenario
       load_config
    """

    # NB the docs contain a table of settings

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

        super().__init__(*args, **kwargs)

        # Set default values, only if they are not already set
        metadata_path = Path(__file__).parents[2] / "data"

        for key, value in (
            ("data", xr.Dataset()),
            ("platform_info", dict()),
            ("scenario_info", dict()),
            ("metadata_path", Path(os.environ.get("MESSAGE_DATA_PATH", metadata_path))),
            ("message_data_path", metadata_path),
            ("cache_path", metadata_path / "cache"),
        ):
            self.setdefault(key, value)

        # Store a reference for get_context()
        _CONTEXTS.append(self)

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

    def delete(self):
        """Hide the current Context from future :func:`get_context` calls."""
        index = _CONTEXTS.index(self)

        if index > 0:
            _CONTEXTS.pop(index)
        else:
            log.warning("Delete the only Context instance")

        self.close_db()

    def close_db(self):
        try:
            mp = self.pop("_mp")
            mp.close_db()
        except KeyError:
            pass

    def get_cache_path(self, *parts) -> Path:
        """Return a path to a local cache file."""
        result = self.cache_path.joinpath(*parts)

        # Ensure the directory exists
        result.parent.mkdir(parents=True, exist_ok=True)

        return result

    def get_config_file(self, *parts, ext="yaml") -> Path:
        """Return a path under :attr:`metadata_path`.

        The suffix ".{ext}" is added; defaulting to ".yaml".
        """
        return self.metadata_path.joinpath(*parts).with_suffix("." + ext)

    def get_path(self, *parts) -> Path:
        """Return a path under :attr:`message_data_path` by joining *parts*.

        *parts* may include directory names, or a filename with extension.
        """
        return Path(self.message_data_path, *parts)

    def get_platform(self, reload=False) -> ixmp.Platform:
        """Return a :class:`ixmp.Platform` from :attr:`platform_info`.

        When used through the CLI, :attr:`platform_info` is a 'base' platform
        as indicated by the --url or --platform  options.

        If a Platform has previously been instantiated with
        :meth:`get_platform`, the same object is returned unless `reload=True`.
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
        self["_mp"] = ixmp.Platform(**self.platform_info)
        return self["_mp"]

    def get_scenario(self) -> message_ix.Scenario:
        """Return a :class:`message_ix.Scenario` from :attr:`scenario_info`.

        When used through the CLI, :attr:`scenario_info` is a 'base' scenario
        as indicated by the --url or --platform/--model/--scenario options.
        """
        return message_ix.Scenario(self.get_platform(), **self.scenario_info)

    def handle_cli_args(
        self,
        url=None,
        platform=None,
        model_name=None,
        scenario_name=None,
        version=None,
        data_path=None,
    ):
        """Handle command-line arguments.

        May update the :attr:`data_path`, :attr:`platform_info`,
        :attr:`scenario_info`, and/or :attr:`url` attributes.
        """
        # Store the path to command-specific data and metadata
        self.data_path = data_path

        # Store information for the target Platform
        if url:
            if platform or model_name or scenario_name or version:
                raise BadOptionUsage(
                    "--platform --model --scenario and/or --version redundant with "
                    "--url"
                )

            self.url = url
            urlinfo = ixmp.utils.parse_url(url)
            self.platform_info.update(urlinfo[0])
            self.scenario_info.update(urlinfo[1])
        elif platform:
            self.platform_info["name"] = platform

        # Store information about the target Scenario
        if model_name:
            self.scenario_info["model"] = model_name
        if scenario_name:
            self.scenario_info["scenario"] = scenario_name
        if version:
            self.scenario_info["version"] = version

    def load_config(self, *parts, suffix=None):
        """Load a config file, attach it to the Context, and return.

        Example
        -------

        The single call:

        >>> info = context.load_config("transport", "set")

        1. loads the metadata file :file:`data/transport/set.yaml`, parsing its
           contents,
        2. stores those values at ``context["transport set"]`` for use by other
           code, and
        3. returns the loaded values.

        Parameters
        ----------
        parts : iterable of str
            Used to construct a path under :attr:`metadata_path`.
        suffix : str, optional
            File suffix.

        Returns
        -------
        dict
            Configuration values that were loaded.
        """
        key = " ".join(parts)
        if key in self:
            log.debug(f"{repr(key)} already loaded; skip")
            return self[key]

        path = self.metadata_path.joinpath(*parts)
        path = path.with_suffix(suffix or path.suffix or ".yaml")

        if path.suffix == ".yaml":
            with open(path, encoding="utf-8") as f:
                self[key] = yaml.safe_load(f)
        else:
            raise ValueError(suffix)

        return self[key]

    @property
    def units(self):
        return pint.get_application_registry()

    def use_defaults(self, settings):
        """Update from `settings`."""
        for setting, info in settings.items():
            if setting not in self:
                log.info(f"Use default {setting}={info[0]}")

            value = self.setdefault(setting, info[0])

            if value not in info:
                raise ValueError(f"{setting} must be in {info}; got {value}")


# Ensure at least one context is created
Context()
