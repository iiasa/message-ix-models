import logging
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from functools import cache
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, cast
from warnings import warn

import pandas as pd
from genno import Quantity
from genno.operator import concat

from ._logging import once

if TYPE_CHECKING:
    from genno.types import TQuantity

    from .context import Context

log = logging.getLogger(__name__)

#: :any:`True` if :mod:`message_data` is installed.
HAS_MESSAGE_DATA = False

#: Root directory of the :mod:`message_data` repository. This package is always
#: installed from source.
MESSAGE_DATA_PATH: Optional[Path] = None

if _spec := find_spec("message_data"):  # pragma: no cover
    HAS_MESSAGE_DATA = True
    assert _spec.origin is not None
    MESSAGE_DATA_PATH = Path(_spec.origin).parents[1]

#: Directory containing message_ix_models.__init__.
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
    def adapt(self, qty: "TQuantity") -> "TQuantity":
        """Adapt data."""


class MappingAdapter(Adapter):
    """Adapt data using mappings for 1 or more dimension(s).

    Parameters
    ----------
    maps : dict of sequence of tuple
        Keys are names of dimensions. Values are sequences of 2-tuples; each tuple
        consists of an original label and a target label.
    on_missing :
        If provided (default :any:`None`), perform the given action if `maps` do not
        contain all the labels in the respective dimensions of each Quantity passed to
        :meth:`adapt`:

        - "log": log a message on level :data:`logging.WARNING`.
        - "raise": raise :class:`RuntimeError`.
        - "warn": emit :class:`RuntimeWarning`.

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
    on_missing: Optional[Literal["log", "raise", "warn"]]

    def __init__(
        self,
        maps: Mapping[str, Sequence[tuple[str, str]]],
        *,
        on_missing: Optional[Literal["log", "raise", "warn"]] = None,
    ) -> None:
        self.maps = maps
        self.on_missing = on_missing

    @classmethod
    def from_dicts(
        cls,
        *values: dict[str, Sequence[str]],
        dims: Sequence[str],
        map_leaves: bool = True,
        # Passed to __init__
        on_missing: Optional[Literal["log", "raise", "warn"]],
    ) -> "MappingAdapter":
        """Construct a MappingAdapter from sequences of :class:`dict` and dimensions."""
        maps: dict[str, list[tuple[str, str]]] = dict()
        for dim, v in zip(dims, values):
            maps[dim] = []
            dim_all = set()
            for group, labels in v.items():
                maps[dim].extend((group, label) for label in labels)
                dim_all |= set(labels)
            if map_leaves:
                maps[dim].extend((label, label) for label in dim_all)

        return cls(maps, on_missing=on_missing)

    def adapt(self, qty: "TQuantity") -> "TQuantity":
        result = qty
        coords = qty.coords

        for dim, labels in self.maps.items():
            if dim not in qty.dims:
                continue

            dim_coords = set(coords[dim].data)

            # Check for coords in the data that are missing from `maps`.
            # Skip if on_missing is None.
            if (
                missing := (dim_coords - {a for (a, b) in labels})
                if self.on_missing
                else set()
            ):
                msg = (
                    f"Original coords {dim}={missing} not mapped to any coords and "
                    f"{'would be' if self.on_missing == 'raise' else 'are'} dropped"
                )
                if self.on_missing == "log":
                    log.warning(msg)
                elif self.on_missing == "raise":
                    raise RuntimeError(msg)
                elif self.on_missing == "warn":
                    warn(msg, RuntimeWarning, stacklevel=2)

            result = concat(
                *[
                    result.sel({dim: a}, drop=True).expand_dims({dim: [b]})
                    for (a, b) in labels
                    if a in dim_coords  # Skip `label` if not in `dim` of `qty`
                ]
            )

        return result


class WildcardAdapter(Adapter):
    """Adapt data using by broadcasting wildcard ("*") entries for 1 dimension."""

    dim: str
    coords: set[str]

    def __init__(self, dim: str, coords: Sequence[str]) -> None:
        self.dim = dim
        self.coords = set(coords)

    @cache
    def _coord_map(self, labels: tuple[str]) -> pd.DataFrame:
        """Cached helper returning a data frame with two columns:

        - :py:`self.dim` with original coords along :attr:`dim`, including the wildcard
          "*".
        - "__new" with the full set of :attr:`coords`.
        """
        _l = set(labels) - {"*"}
        # Missing coords to be filled using wildcard
        to_wildcard = self.coords - _l
        # Mapping from existing labels to labels to appear in result
        return pd.DataFrame(
            [[c, c] for c in _l] + [["*", c] for c in to_wildcard],
            columns=[self.dim, "__new"],
        )

    def adapt(self, qty: "TQuantity") -> "TQuantity":
        # Identify the dimensions to group on
        groupby_dims = list(qty.dims) + ["__preserve"]
        groupby_dims.remove(self.dim)

        def wildcard_group(x: pd.DataFrame) -> pd.DataFrame:
            """Apply the wildcard operation to group data frame `x`."""
            # Coordinates for `self.dim` appearing in the group
            c_in_group = tuple(sorted(x[self.dim].unique()))

            if "*" not in c_in_group:
                # Nothing to wildcard in this group
                result = x
            else:
                # - Retrieve a _coord_map for this group.
                # - (Outer) merge with `x`.
                # - Replace original `self.dim` column with "__new".
                result = (
                    x.merge(self._coord_map(c_in_group), on=self.dim)
                    .drop(self.dim, axis=1)
                    .rename(columns={"__new": self.dim})
                )

            # Make `self.dim` an index level
            return result.set_index(self.dim)

        # - Convert to pd.DataFrame, reset index to columns.
        # - Group on `groupby_dims`.
        # - Apply wildcard_group().
        # - Convert back to Quantity.
        q_result = type(qty)(
            qty.to_frame()
            .reset_index()
            .assign(__preserve="")
            .groupby(groupby_dims)
            .apply(wildcard_group, include_groups=False)
            .droplevel("__preserve")
            .iloc[:, 0]
        )
        return qty._keep(q_result, attrs=True, name=True, units=True)


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
