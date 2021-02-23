import logging
from copy import copy
from pathlib import Path
from typing import Any, Dict, Mapping

from sdmx.model import Annotation, Code

log = logging.getLogger(__name__)

PACKAGE_DATA: Dict[str, Any] = dict()


def as_codes(data):
    """Convert *data* to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`.Code.id` and values are further
      :class:`dict` with keys matching other :class:`.Code` attributes.
    """
    # Assemble results as a dictionary
    result = {}

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
            id=id,
            name=info.pop("name", id.title()),
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
            code.annotations.append(Annotation(id=id, text=repr(value)))

        result[code.id] = code

    return list(result.values())


def load_package_data(*parts, suffix=None):
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
    if key in PACKAGE_DATA:
        log.debug(f"{repr(key)} already loaded; skip")
        return PACKAGE_DATA[key]

    path = Path(__file__).parents[1].joinpath("data", *parts)
    path = path.with_suffix(suffix or path.suffix or ".yaml")

    if path.suffix == ".yaml":
        import yaml

        with open(path, encoding="utf-8") as f:
            PACKAGE_DATA[key] = yaml.safe_load(f)
    else:
        raise ValueError(suffix)

    return PACKAGE_DATA[key]
