import logging
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

import pandas as pd
from genno import Quantity
from genno.operator import concat

from ._logging import once

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from .context import Context

log = logging.getLogger(__name__)

try:
    import message_data
except ImportError:
    MESSAGE_DATA_PATH: Optional[Path] = None
    HAS_MESSAGE_DATA = False
else:  # pragma: no cover  (needs message_data)
    # Root directory of the message_data repository.
    MESSAGE_DATA_PATH = Path(message_data.__file__).parents[1]
    HAS_MESSAGE_DATA = True

__all__ = [
    "HAS_MESSAGE_DATA",
    "Adapter",
    "MappingAdapter",
]

# Directory containing message_ix_models.__init__
MESSAGE_MODELS_PATH = Path(__file__).parents[1]

#: Package data already loaded with :func:`load_package_data`.
PACKAGE_DATA: dict[str, Any] = dict()

#: Data already loaded with :func:`load_private_data`.
PRIVATE_DATA: dict[str, Any] = dict()


class Adapter:
    """Adapt `data`.

    Adapter is an abstract base class for tools that adapt data in any way, e.g.
    between different code lists for certain dimensions. An instance of an Adapter can
    be called with any of the following as `data`:

    - :class:`genno.Quantity`,
    - :class:`pandas.DataFrame`, or
    - :class:`dict` mapping :class:`str` parameter names to values (either of the above
      types).

    …and will return data of the same type.

    Subclasses can implement different adapter logic by overriding the abstract
    :meth:`adapt` method.
    """

    def __call__(self, data):
        if isinstance(data, Quantity):
            return self.adapt(data)
        elif isinstance(data, pd.DataFrame):
            # Convert to Quantity
            idx_cols = list(filter(lambda c: c not in ("value", "unit"), data.columns))
            qty = Quantity.from_series(data.set_index(idx_cols)["value"])

            # Store units
            if "unit" in data.columns:
                units = data["unit"].unique()
                assert 1 == len(units), f"Non-unique units {units}"
                unit = units[0]
            else:
                unit = ""  # dimensionless

            # Adapt, convert back to pd.DataFrame, return
            return self.adapt(qty).to_dataframe().assign(unit=unit).reset_index()
        elif isinstance(data, Mapping):
            return {par: self(value) for par, value in data.items()}
        else:
            raise TypeError(type(data))

    @abstractmethod
    def adapt(self, qty: "AnyQuantity") -> "AnyQuantity":
        """Adapt data."""


class MappingAdapter(Adapter):
    """Adapt data using mappings for 1 or more dimension(s).

    Parameters
    ----------
    maps : dict of sequence of tuple
        Keys are names of dimensions. Values are sequences of 2-tuples; each tuple
        consists of an original label and a target label.

    Examples
    --------
    >>> a = MappingAdapter({"foo": [("a", "x"), ("a", "y"), ("b", "z")]})
    >>> df = pd.DataFrame(
    ...     [["a", "m", 1], ["b", "n", 2]], columns=["foo", "bar", "value"]
    ... )
    >>> a(df)
      foo  bar  value
    0   x    m      1
    1   y    m      1
    2   z    n      2
    """

    maps: Mapping

    def __init__(self, maps: Mapping[str, Sequence[tuple[str, str]]]):
        self.maps = maps

    def adapt(self, qty: "AnyQuantity") -> "AnyQuantity":
        result = qty
        coords = qty.coords

        for dim, labels in self.maps.items():
            if dim not in qty.dims:  # type: ignore [attr-defined]
                continue
            result = concat(
                *[
                    result.sel({dim: a}, drop=True).expand_dims({dim: [b]})
                    for (a, b) in labels
                    if a in coords[dim]  # Skip `label` if not in `dim` of `qty`
                ]
            )

        return result


def _load(
    var: dict, base_path: Path, *parts: str, default_suffix: Optional[str] = None
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

    Analogous to :func:`load_package_data`, but for non-public data.

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


def local_data_path(*parts, context: Optional["Context"] = None) -> Path:
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
    from .context import Context

    ctx = context or Context.get_instance(-1)

    return ctx.core.local_data.joinpath(*parts)


def package_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`message_ix_models/data/`.

    Use this function to access data packaged and installed with
    :mod:`message_ix_models`.

    Parameters
    ----------
    parts : sequence of str or Path
        Joined to the base path using :meth:`~pathlib.PurePath.joinpath`.

    See also
    --------
    :ref:`Choose locations for data <package-data>`
    """
    return _make_path(MESSAGE_MODELS_PATH / "data", *parts)


def private_data_path(*parts) -> Path:
    """Construct a path to a file under :file:`data/` in :mod:`message_data`.

    Use this function to access non-public (for instance, embargoed or proprietary) data
    stored in the :mod:`message_data` repository.

    If the repository is not available, the function falls back to
    :meth:`.Context.get_local_path`, where users may put files obtained through other
    messages.

    Parameters
    ----------
    parts : sequence of str or Path
        Joined to the base path using :meth:`~pathlib.PurePath.joinpath`.

    See also
    --------
    :ref:`Choose locations for data <private-data>`
    """
    if HAS_MESSAGE_DATA:
        return _make_path(cast(Path, MESSAGE_DATA_PATH) / "data", *parts)
    else:
        from .context import Context

        base = Context.get_instance(-1).get_local_path()
        once(log, logging.WARNING, f"message_data not installed; fall back to {base}")
        return base.joinpath(*parts)
