import logging
from copy import copy
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union, cast

import message_ix
import pandas as pd
from message_ix.models import MESSAGE_ITEMS
from sdmx.model import AnnotableArtefact, Annotation, Code

log = logging.getLogger(__name__)

try:
    import message_data
except ImportError:
    log.warning("message_data is not installed")
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


def add_par_data(
    scenario: message_ix.Scenario,
    data: Mapping[str, pd.DataFrame],
    dry_run: bool = False,
):
    """Add `data` to `scenario`.

    Parameters
    ----------
    data
        Dict with keys that are parameter names, and values are pd.DataFrame or other
        arguments
    dry_run : optional
        Only show what would be done.

    See also
    --------
    strip_par_data
    """
    total = 0

    for par_name, values in data.items():
        N = values.shape[0]
        log.info(f"{N} rows in {repr(par_name)}")
        log.debug(str(values))

        total += N

        if dry_run:
            continue

        scenario.add_par(par_name, values)

    return total


def as_codes(data: Union[List[str], Dict[str, Dict]]) -> List[Code]:
    """Convert *data* to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`.Code.id` and values are further
      :class:`dict` with keys matching other :class:`.Code` attributes.
    """
    # Assemble results as a dictionary
    result: Dict[str, Code] = {}

    if isinstance(data, list):
        # FIXME typing ignored temporarily for PR#9
        data = dict(zip(data, data))  # type: ignore [arg-type]
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
        )

        # Store the description, if any
        try:
            code.description = info.pop("description")
        except KeyError:
            pass

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


def eval_anno(obj: AnnotableArtefact, id: str):
    """Retrieve the annotation `id` from `obj`, run :func:`eval` on its contents.

    This can be used for unpacking Python values (e.g. :class:`dict`) stored as an
    annotation on a :class:`~sdmx.model.Code`.

    Returns :obj:`None` if no attribute exists with the given `id`.
    """
    try:
        value = str(obj.get_annotation(id=id).text)
    except KeyError:
        # No such attribute
        return None

    try:
        return eval(value)
    except Exception:
        # Something that can't be eval()'d, e.g. a string
        return value


def iter_parameters(set_name):
    """Iterate over MESSAGEix parameters with *set_name* as a dimension.

    Parameters
    ----------
    set_name : str
        Name of a set.

    Yields
    ------
    str
        Names of parameters that have `set_name` indexing ≥1 dimension.
    """
    # TODO move upstream. See iiasa/ixmp#402 and iiasa/message_ix#444
    for name, info in MESSAGE_ITEMS.items():
        if info["ix_type"] == "par" and set_name in info["idx_sets"]:
            yield name


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


def package_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`message_ix_models/data/`."""
    return _make_path(MESSAGE_MODELS_PATH / "data", *parts)


def private_data_path(*parts) -> Path:  # pragma: no cover (needs message_data)
    """Construct a path to a file under :file:`data/` in :mod:`message_data`."""
    return _make_path(cast(Path, MESSAGE_DATA_PATH) / "data", *parts)


def strip_par_data(
    scenario, set_name, value, dry_run=False, dump: Dict[str, pd.DataFrame] = None
):
    """Remove data from parameters of *scenario* where *value* in *set_name*.

    Returns
    -------
    Total number of rows removed across all parameters.

    See also
    --------
    add_par_data
    """
    par_list = scenario.par_list()
    no_data = []
    total = 0

    for par_name in iter_parameters(set_name):
        if par_name not in par_list:
            continue

        # Check for contents of par_name that include *value*
        par_data = scenario.par(par_name, filters={set_name: value})
        N = len(par_data)

        if N == 0:
            # No data; no need to do anything further
            no_data.append(par_name)
            continue
        elif dump is not None:
            dump[par_name] = pd.concat(
                [
                    dump.get(par_name, pd.DataFrame()),
                    par_data,
                ]
            )

        log.info(f"Remove {N} rows in {par_name!r}.")

        # Show some debug info
        for col in "commodity level technology".split():
            if col == set_name or col not in par_data.columns:
                continue

            log.info("  with {}={}".format(col, sorted(par_data[col].unique())))

        if not dry_run:
            # Actually remove the data
            scenario.remove_par(par_name, key=par_data)

            # # NB would prefer to do the following, but raises an exception:
            # scenario.remove_par(par_name, key={set_name: [value]})

        total += N

    level = logging.INFO if total > 0 else logging.DEBUG
    log.log(level, f"{total} rows removed.")
    log.debug(f"No data removed from {len(no_data)} other parameters.")

    return total
