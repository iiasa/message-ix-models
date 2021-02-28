import logging
from copy import copy
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, cast

from sdmx.model import Annotation, Code

log = logging.getLogger(__name__)

try:
    import message_data

    # Root directory of the message_data repository.
    MESSAGE_DATA_PATH: Optional[Path] = Path(message_data.__file__).parents[1]
except ImportError:  # pragma: no cover
    log.warning("message_data is not installed")
    MESSAGE_DATA_PATH = None


# Directory containing message_ix_models.__init__
MESSAGE_MODELS_PATH = Path(__file__).parents[1]

#: Package data already loaded with :func:`load_package_data`.
PACKAGE_DATA: Dict[str, Any] = dict()

#: Data already loaded with :func:`load_private_data`.
PRIVATE_DATA: Dict[str, Any] = dict()


def as_codes(data) -> List[Code]:
    """Convert *data* to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`.Code.id` and values are further
      :class:`dict` with keys matching other :class:`.Code` attributes.
    """
    # Assemble results as a dictionary
    result: Dict[str, Code] = {}

    if isinstance(data, list):
        data = dict(zip(data, data))
    elif not isinstance(data, Mapping):
        raise TypeError(data)

    for id, info in data.items():
        if isinstance(info, str):
            info = dict(name=info)
        elif isinstance(info, Mapping):
            info = copy(info)
        else:
            raise TypeError(info)

        code = Code(
            id=str(id),
            name=info.pop("name", str(id).title()),
            desc=info.pop("description", None),
        )

        # Associate with a parent
        try:
            parent_id = info.pop("parent")
        except KeyError:
            pass  # No parent
        else:
            result[parent_id].append_child(code)

        # Associate with any children
        for id in info.pop("child", []):
            try:
                code.append_child(result[id])
            except KeyError:
                pass  # Not parsed yet

        # Convert other dictionary (key, value) pairs to annotations
        for id, value in info.items():
            code.annotations.append(
                Annotation(id=id, text=value if isinstance(value, str) else repr(value))
            )

        result[code.id] = code

    return list(result.values())


def _load(
    var: Dict, base_path: Path, *parts: str, default_suffix: Optional[str] = None
) -> Mapping:
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


def load_package_data(*parts: str, suffix: Optional[str] = ".yaml") -> Mapping:
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


def load_private_data(*parts: str) -> Mapping:  # pragma: no cover
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


def package_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`message_ix_models/data/`."""
    return _make_path(MESSAGE_MODELS_PATH / "data", *parts)


def private_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`data/` in :mod:`message_data`."""
    return _make_path(cast(Path, MESSAGE_DATA_PATH) / "data", *parts)
