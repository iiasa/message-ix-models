import logging
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, cast

log = logging.getLogger(__name__)

try:
    import message_data
except ImportError:
    log.warning("message_data is not installed or cannot be imported")
    MESSAGE_DATA_PATH: Optional[Path] = None
else:  # pragma: no cover  (needs message_data)
    # Root directory of the message_data repository.
    MESSAGE_DATA_PATH = Path(message_data.__file__).parents[1]

# Directory containing message_ix_models.__init__
MESSAGE_MODELS_PATH = Path(__file__).parents[1]

#: Package data already loaded with :func:`load_package_data`.
PACKAGE_DATA: Dict[str, Any] = dict()

#: Data already loaded with :func:`load_private_data`.
PRIVATE_DATA: Dict[str, Any] = dict()


def _load(
    var: Dict, base_path: Path, *parts: str, default_suffix: Optional[str] = None
) -> Any:
    """Helper for :func:`.load_package_data` and :func:`.load_private_data`."""
    key = " ".join(parts)
    if key in var:
        log.debug(f"{repr(key)} already loaded; skip")
        return var[key]

    path = _make_path(base_path, *parts, default_suffix=default_suffix)

    if path.suffix == ".yaml":
        import yaml

        with open(path, encoding="utf-8") as f:
            var[key] = yaml.safe_load(f)
    else:
        raise ValueError(path.suffix)

    return var[key]


def _make_path(
    base_path: Path, *parts: str, default_suffix: Optional[str] = None
) -> Path:
    p = base_path.joinpath(*parts)
    return p.with_suffix(p.suffix or default_suffix) if default_suffix else p


def load_package_data(*parts: str, suffix: Optional[str] = ".yaml") -> Any:
    """Load a :mod:`message_ix_models` package data file and return its contents.

    Data is re-used if already loaded.

    Example
    -------

    The single call:

    >>> info = load_package_data("node", "R11")

    1. loads the metadata file :file:`data/node/R11.yaml`, parsing its contents,
    2. stores those values at ``PACKAGE_DATA["node R11"]`` for use by other code, and
    3. returns the loaded values.

    Parameters
    ----------
    parts : iterable of str
        Used to construct a path under :file:`message_ix_models/data/`.
    suffix : str, optional
        File name suffix, including, the ".", e.g. :file:`.yaml`.

    Returns
    -------
    dict
        Configuration values that were loaded.
    """
    return _load(
        PACKAGE_DATA,
        MESSAGE_MODELS_PATH / "data",
        *parts,
        default_suffix=suffix,
    )


def load_private_data(*parts: str) -> Mapping:  # pragma: no cover (needs message_data)
    """Load a private data file from :mod:`message_data` and return its contents.

    Analogous to :mod:`load_package_data`, but for non-public data.

    Parameters
    ----------
    parts : iterable of str
        Used to construct a path under :file:`data/` in the :mod:`message_data`
        repository.

    Returns
    -------
    dict
        Configuration values that were loaded.

    Raises
    ------
    RuntimeError
        if :mod:`message_data` is not installed.
    """
    if MESSAGE_DATA_PATH is None:
        raise RuntimeError("message_data is not installed")

    return _load(PRIVATE_DATA, MESSAGE_DATA_PATH / "data", *parts)


def local_data_path(*parts) -> Path:
    """Construct a path for local data.

    The setting ``message local data`` in the user's :ref:`ixmp configuration file
    <ixmp:configuration>` is used as a base path. If this is not configured, the
    current working directory is used.

    Parameters
    ----------
    parts : sequence of str or Path
        Joined to the base path using :meth:`.Path.joinpath`.

    See also
    --------
    :ref:`Choose locations for data <local-data>`
    """
    import ixmp

    # The default value, Path.cwd(), is set in context.py
    return _make_path(Path(ixmp.config.get("message local data")), *parts)


def package_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`message_ix_models/data/`.

    Use this function to access data packaged and installed with
    :mod:`message_ix_models`.

    Parameters
    ----------
    parts : sequence of str or Path
        Joined to the base path using :meth:`.Path.joinpath`.

    See also
    --------
    :ref:`Choose locations for data <package-data>`
    """
    return _make_path(MESSAGE_MODELS_PATH / "data", *parts)


def private_data_path(*parts) -> Path:  # pragma: no cover (needs message_data)
    """Construct a path to a file under :file:`data/` in :mod:`message_data`.

    Use this function to access non-public (e.g. embargoed or proprietary) data stored
    in the :mod:`message_data` repository.

    Parameters
    ----------
    parts : sequence of str or Path
        Joined to the base path using :meth:`.Path.joinpath`.

    See also
    --------
    :ref:`Choose locations for data <private-data>`
    """
    return _make_path(cast(Path, MESSAGE_DATA_PATH) / "data", *parts)
