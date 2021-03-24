"""Context and settings for :mod:`message_ix_models` code."""

import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import List, Tuple
from warnings import warn

import ixmp
import message_ix
from click import BadOptionUsage

from message_ix_models.util import (
    load_package_data,
    package_data_path,
    private_data_path,
)

log = logging.getLogger(__name__)

#: List of Context instances, from first created to last.
_CONTEXTS: List["Context"] = []


class Context(dict):
    """Context and settings for :mod:`message_ix_models` code."""

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

        # Store any keyword arguments
        super().__init__(*args, **kwargs)

        # Default paths for local data
        default_local_data = Path(
            os.environ.get("MESSAGE_LOCAL_DATA", None)
            or ixmp.config.values.get("message local data", None)
            or Path.cwd()
        ).resolve()

        for key, value in (
            ("platform_info", dict()),
            ("scenario_info", dict()),
            ("local_data", default_local_data),
        ):
            self.setdefault(key, value)

        # Store a reference for get_instance()
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
        """Hide the current Context from future :meth:`.get_instance` calls."""
        index = _CONTEXTS.index(self)

        if index > 0:
            _CONTEXTS.pop(index)
        else:
            log.warning("Won't delete the only Context instance")

        self.close_db()

    def clone_to_dest(self) -> Tuple[message_ix.Scenario, ixmp.Platform]:
        """Return a scenario based on the ``--dest`` command-line option.

        Returns
        -------
        Scenario
            To prevent the scenario from being garbage collected. Keep a reference to
            its Platform:

            .. code-block: python

               s = context.clone_to_dest()
               mp = s.platform

        See also
        --------
        create_res
        """
        if "dest_scenario" not in self:
            # No information on the destination; try to parse a URL, storing the keys
            # dest_platform and dest_scenario.
            self.handle_cli_args(
                url=self["dest"], _store_as=("dest_platform", "dest_scenario")
            )

        try:
            # Get the base scenario, e.g. from the --url CLI argument
            scenario_base = self.get_scenario()

            # By default, clone to the same platform
            mp_dest = scenario_base.platform

            try:
                # Get information about a destination platform
                info = self["dest_platform"]
            except KeyError:
                pass  # dest_platform not set; use the same as scenario_base
            else:  # pragma: no cover
                # Not tested; current test fixtures make it difficult to create *two*
                # temporary platforms simultaneously
                if info["name"] != mp_dest.name:
                    # Different platform
                    mp_dest = ixmp.Platform(**info)

        except Exception:
            log.info("No base scenario given")

            # Create a bare RES to be the base scenario

            from message_ix_models.model.bare import create_res

            # Create on the destination platform
            c = deepcopy(self)
            c.platform_info.update(self.get("dest_platform", {}))

            scenario_base = create_res(c)

            # Clone to the same platform
            mp_dest = scenario_base.platform

        # Clone
        log.info(f"Clone to {repr(self.dest_scenario)}")
        return scenario_base.clone(platform=mp_dest, **self["dest_scenario"])

    def close_db(self):
        try:
            mp = self.pop("_mp")
            mp.close_db()
        except KeyError:
            pass

    def get_cache_path(self, *parts) -> Path:
        """Return a path to a local cache file."""
        base = self.get("cache_path", self.local_data.joinpath("cache"))
        result = base.joinpath(*parts)

        # Ensure the directory exists
        result.parent.mkdir(parents=True, exist_ok=True)

        return result

    def get_local_path(self, *parts, suffix=None):
        """Return a path under ``local_data``."""
        result = self.local_data.joinpath(*parts)
        return result.with_suffix(suffix or result.suffix)

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

        When used through the CLI, :attr:`scenario_info` is a ‘base’ scenario for an
        operation, indicated by the ``--url`` or ``--platform/--model/--scenario``
        options.
        """
        return message_ix.Scenario(self.get_platform(), **self.scenario_info)

    def handle_cli_args(
        self,
        url=None,
        platform=None,
        model_name=None,
        scenario_name=None,
        version=None,
        local_data=None,
        _store_as=("platform_info", "scenario_info"),
    ):
        """Handle command-line arguments.

        May update the :attr:`data_path`, :attr:`platform_info`, :attr:`scenario_info`,
        and/or :attr:`url` settings.
        """
        # Store the path to command-specific data and metadata
        if local_data:
            self.local_data = local_data

        # References to the Context settings to be updated
        platform_info = self.setdefault(_store_as[0], dict())
        scenario_info = self.setdefault(_store_as[1], dict())

        # Store information for the target Platform
        if url:
            if platform or model_name or scenario_name or version:
                raise BadOptionUsage(
                    "--platform --model --scenario and/or --version",
                    " redundant with --url",
                )

            self.url = url
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

    # Deprecated methods

    def get_config_file(self, *parts, ext="yaml") -> Path:
        """Return a path under :attr:`metadata_path`.

        The suffix ".{ext}" is added; defaulting to ".yaml".

        .. deprecated:: 2021.2.28
           Use :func:`.package_data_path` instead.
           Will be removed on or after 2021-05-28.
        """
        # TODO remove on or after 2021-05-28
        warn(
            "Context.get_config_file(). Instead use:\n"
            "from message_ix_models import package_data_path",
            DeprecationWarning,
            stacklevel=2,
        )
        return package_data_path(*parts).with_suffix(f".{ext}")

    def get_path(self, *parts) -> Path:  # pragma: no cover  (needs message_data)
        """Return a path under :attr:`message_data_path` by joining *parts*.

        *parts* may include directory names, or a filename with extension.

        .. deprecated:: 2021.2.28
           Use :func:`.private_data_path` instead.
           Will be removed on or after 2021-05-28.
        """
        # TODO remove on or after 2021-05-28
        warn(
            "Context.get_path(). Instead use: \n"
            "from message_ix_models import private_data_path",
            DeprecationWarning,
            stacklevel=2,
        )
        return private_data_path(*parts)

    def load_config(self, *parts, suffix=None):
        """Load configuration from :mod:`message_ix_models`.

        .. deprecated:: 2021.2.28
           Use :func:`.load_package_data` instead.
           Will be removed on or after 2021-05-28.
        """
        # TODO remove on or after 2021-05-28
        warn(
            "Context.load_config(). Instead use:\n"
            "from message_ix_models.util import load_package_data",
            DeprecationWarning,
            stacklevel=2,
        )
        result = load_package_data(*parts, suffix=suffix)
        self[" ".join(parts)] = result
        return result

    @property
    def units(self):
        """Access the unit registry.

        .. deprecated:: 2021.2.28
           Instead, use:

           .. code-block:: python

              from iam_units import registry

           Will be removed on or after 2021-05-28.
        """
        # TODO remove on or after 2021-05-28
        warn(
            "Context.units attribute. Instead use:\nfrom iam_units import registry",
            DeprecationWarning,
            stacklevel=2,
        )
        from iam_units import registry

        return registry
