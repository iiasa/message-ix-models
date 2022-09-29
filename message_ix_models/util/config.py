import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import MutableMapping, Optional

import ixmp

ixmp.config.register("message local data", Path, Path.cwd())


def _local_data_factory():
    return (
        Path(
            os.environ.get("MESSAGE_LOCAL_DATA", None)
            or ixmp.config.get("message local data")
        )
        .expanduser()
        .resolve()
    )


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

    #: Like :attr:`platform_info`, used by e.g. :meth:`clone_to_dest`.
    dest_platform: MutableMapping[str, str] = field(default_factory=dict)

    #: Like :attr:`scenario_info`, used by e.g. :meth:`clone_to_dest`.
    dest_scenario: MutableMapping[str, str] = field(default_factory=dict)

    #: A scenario URL, e.g. as given by the :program:`--url` CLI option.
    url: Optional[str] = None

    #: Like :attr:`url`, used by e.g. :meth:`clone_to_dest`.
    dest: Optional[str] = None

    #: Base path for cached data, e.g. as given by the :program:`--cache-path` CLI
    #: option. Default: :file:`{local_data}/cache/`.
    cache_path: Optional[str] = None

    #: Whether an operation should be carried out, or only previewed. Different modules
    #: will respect :attr:`dry_run` in distinct ways, if at all, and **should** document
    #: behaviour.
    dry_run: bool = False

    #: Flag for causing verbose output to logs or stdout. Different modules will respect
    #: :attr:`verbose` in distinct ways.
    verbose: bool = False
