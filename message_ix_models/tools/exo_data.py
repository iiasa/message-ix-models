"""Generic tools for working with exogenous data sources."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from operator import itemgetter
from typing import Optional

from genno import Computer, Key, Quantity, quote

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes

__all__ = [
    "MEASURES",
    "SOURCES",
    "DemoSource",
    "ExoDataSource",
    "prepare_computer",
    "register_source",
]

log = logging.getLogger(__name__)

#: Supported measures. Subclasses of :class:`.ExoDataSource` may provide support for
#: other measures.
#:
#: .. todo:: Store this in a separate code list or concept scheme.
MEASURES = ("GDP", "POP")

#: Known sources for data. Use :func:`register_source` to add to this collection.
SOURCES: dict[str, type["ExoDataSource"]] = {}


class ExoDataSource(ABC):
    """Base class for sources of exogenous data."""

    #: Identifier for this particular source.
    id: str = ""

    #: Key for the returned :class:`.Quantity`. Optional. See :meth:`get_keys`.
    key: Optional[Key] = None

    #: Name for the returned :class:`.Key`/:class:`.Quantity`. Optional. See
    # :meth:`get_keys`.
    name: str = ""

    #: Optional additional dimensions for the returned :class:`.Key`/:class:`.Quantity`.
    #: If not set by :meth:`.__init__`, the dimensions are :math:`(n, y)`.
    extra_dims: tuple[str, ...] = ()

    #: :any:`True` if :meth:`.transform` should aggregate data on the |n| dimension.
    aggregate: bool = True

    #: :any:`True` if :meth:`.transform` should interpolate data on the |y| dimension.
    interpolate: bool = True

    @abstractmethod
    def __init__(self, source: str, source_kw: Mapping) -> None:
        """Handle `source` and `source_kw`.

        An implementation **must**:

        - Raise :class:`ValueError` if it does not recognize or cannot handle the
          arguments in `source` or `source_kw`.
        - Recognize and handle (if possible) a "measure" keyword in `source_kw` from
          :data:`MEASURES`.

        It **may**:

        - Transform these into other values, for instance by mapping certain values to
          others, applying regular expressions, or other operations.
        - Store those values as instance attributes for use in :meth:`__call__`.
        - Set :attr:`name` and/or :attr:`extra_dims` to control the behaviour of
          :func:`.prepare_computer`.
        - Log messages that give information that may help to debug a
          :class:`ValueError` for `source` or `source_kw` that cannot be handled.

        It **should not** actually load data or perform any time- or memory-intensive
        operations; these should only be triggered by :meth:`.__call__`.
        """

        raise ValueError

    @abstractmethod
    def __call__(self) -> Quantity:
        r"""Return the data.

        The Quantity returned by this method **must** have dimensions
        :math:`(n, y) \cup \text{extra_dims}`. If the original/upstream/raw data has
        different dimensionality (fewer or more dimensions; different dimension IDs),
        the code **must** transform these, make appropriate selections, etc.
        """
        raise NotImplementedError

    def get_keys(self) -> tuple[Key, Key]:
        """Return the target keys for the (1) raw and (2) transformed data.

        Subclasses **may** override this method to provide different targets keys. In
        the default implementation, the key for the transformed data is:

        1. :attr:`.key`, if any, or
        2. Constructed from:

           - :attr:`.name` or :attr:`.measure` in lower-case.
           - The dimensions :math:`(n, y)`, plus any :attr:`.extra_dims`.

        The key for the raw data is the same, with :attr`.id` as an extra tag.
        """
        k = self.key or Key(
            self.name or self.measure.lower(), ("n", "y") + self.extra_dims
        )
        return k + self.id, k

    def transform(self, c: "Computer", base_key: Key) -> Key:
        """Prepare `c` to transform raw data from `base_key`.

        `base_key` identifies the :class:`.Quantity` that is returned by
        :meth:`.__call__`. Before the data is returned, :meth:`.transform` allows the
        data source to add additional tasks or computations to `c` that further
        transform the data. (These operations **may** be done in :meth:`.__call__`
        directly, but :meth:`.transform` allows use of other :mod:`genno` operators and
        conveniences.)

        The default implementation:

        1. If :attr:`.aggregate` is :any:`True`, aggregates the data (
           :func:`.genno.operator.aggregate`) on the |n| dimension using the key
           "n::groups".
        2. If :attr:`.interpolate` is :any:`True`, interpolates the data (
           :func:`.genno.operator.interpolate`) on the |y| dimension using "y::coords".
        """
        k = base_key
        # Aggregate
        if self.aggregate:
            k = c.add(k + "1", "aggregate", k, "n::groups", keep=False)

        # Interpolate to the desired set of periods
        if self.interpolate:
            kw = dict(fill_value="extrapolate")
            k = c.add(k + "2", "interpolate", k, "y::coords", kwargs=kw)

        return k

    def raise_on_extra_kw(self, kwargs) -> None:
        """Helper for subclasses to handle the `source_kw` argument.

        1. Store :attr:`.aggregate` and :attr:`.interpolate`, if they remain in
           `kwargs`.
        2. Raise :class:`ValueError` if there are any other, unhandled keyword arguments
           in `kwargs`.
        """
        self.aggregate = kwargs.pop("aggregate", self.aggregate)
        self.interpolate = kwargs.pop("interpolate", self.interpolate)
        self.name = kwargs.pop("name", self.name)

        if len(kwargs):
            log.error(
                f"Unhandled extra keyword arguments for {type(self).__name__}: "
                + repr(kwargs)
            )
            raise ValueError(kwargs)


def prepare_computer(
    context,
    c: "Computer",
    source="test",
    source_kw: Optional[Mapping] = None,
    *,
    strict: bool = True,
) -> tuple[Key, ...]:
    """Prepare `c` to compute GDP, population, or other exogenous data.

    Check each :class:`ExoDataSource` in :data:`SOURCES` to determine whether it
    recognizes and can handle `source` and `source_kw`. If a source is identified, add
    tasks to `c` that retrieve and process data into a :class:`.Quantity` with, at
    least, dimensions :math:`(n, y)`.

    Parameters
    ----------
    source : str
        Identifier of the source, possibly with other information to be handled by a
        :class:`ExoDataSource`.
    source_kw : dict, optional
        Keyword arguments for a Source class. These can include indexers, selectors, or
        other information needed by the source class to identify the data to be
        returned.

        If the key "measure" is present, it **should** be one of :data:`MEASURES`.
    strict : bool, optional
        Raise an exception if any of the keys to be added already exist.

    Returns
    -------
    tuple of .Key

    Raises
    ------
    ValueError
        if no source is registered which can handle `source` and `source_kw`.
    """
    # Handle arguments
    source_kw = source_kw or dict()
    if measure := source_kw.get("measure"):
        # commented: quiet logging; MEASURES is not automatically updated to include the
        # IDs of measures recognized by additional SOURCES
        # TODO Remove use of this variable, below, by ensuring that source_obj.name is
        #      always present
        # if measure not in MEASURES:
        #     log.debug(f"source keyword {measure = } not in recognized {MEASURES}")
        del measure

    # Look up input data flow
    source_obj = None
    for cls in SOURCES.values():
        try:
            # Instantiate a Source object to provide this data
            source_obj = cls(source, deepcopy(source_kw or dict()))
        # except Exception as e:  # For debugging
        #     log.debug(repr(e))
        except Exception:
            pass  # Class does not recognize the arguments

    if source_obj is None:
        raise ValueError(f"No source found that can handle {source!r}")

    # Add structural information to the Computer
    c.require_compat("message_ix_models.report.operator")
    c.graph.setdefault("context", context)

    # Retrieve the node codelist
    c.add("n::codes", quote(get_codes(f"node/{context.model.regions}")), strict=strict)

    # Convert the codelist into a nested dict for aggregate()
    c.add("n::groups", "codelist_to_groups", "n::codes", strict=strict)

    # Add information about the list of periods
    if "y" not in c:
        info = ScenarioInfo()
        info.year_from_codes(get_codes(f"year/{context.model.years}"))

        c.add("y", quote(info.Y))

    if "y0" not in c:
        c.add("y0", itemgetter(0), "y")

    # Above as coords/indexers
    c.add("y::coords", lambda years: dict(y=years), "y")
    c.add("y0::coord", lambda year: dict(y=year), "y0")

    # Retrieve the keys that will refer to the raw and transformed data
    k_raw, k = source_obj.get_keys()

    # Keys to return
    keys = [k]

    # Retrieve the raw data by invoking ExoDataSource.__call__
    c.add(k_raw, source_obj)

    # Allow the class to add further tasks that transform the data. See
    # ExoDataSource.transform() for the default: aggregate, then interpolate.
    key = source_obj.transform(c, k_raw)

    # Alias `key` -> `k`
    c.add(k, key)

    # Index to y0
    k_y0 = k + "y0_indexed"
    c.add(k_y0, "index_to", k, "y0::coord")
    keys.append(k_y0)

    # TODO Index to a particular label on the "n" dimension
    # TODO Index on both "n" and "y"

    return tuple(keys)


def register_source(cls: type[ExoDataSource]) -> type[ExoDataSource]:
    """Register :class:`.ExoDataSource` `cls` as a source of exogenous data."""
    if cls.id in SOURCES:
        raise ValueError(f"{SOURCES[cls.id]} already registered for id {cls.id!r}")
    SOURCES[cls.id] = cls
    return cls


@register_source
class DemoSource(ExoDataSource):
    """Example source of exogenous population and GDP data.

    Parameters
    ----------
    source : str
        **Must** be like ``test s1``, where "s1" is a scenario ID from ("s0"…"s4").
    source_kw : dict
        **Must** contain an element "measure", one of :data:`MEASURES`.
    """

    id = "DEMO"

    def __init__(self, source, source_kw):
        if not source.startswith("test "):
            # Don't recognize this `source` string → can't provide data
            raise ValueError

        # Select the data according to the `source`; in this case, scenario
        *parts, scenario = source.partition("test ")
        self.indexers = dict(s=scenario)

        # Map from the measure ID to a variable name
        self.measure = source_kw["measure"]
        self.indexers.update(v={"POP": "Population", "GDP": "GDP"}[self.measure])

    def __call__(self) -> Quantity:
        from genno.operator import select

        # - Retrieve the data.
        # - Apply the prepared indexers.
        return self.random_data().pipe(select, self.indexers, drop=True)

    @staticmethod
    def random_data():
        """Generate some random data with n, y, s, and v dimensions."""
        from genno.operator import relabel
        from genno.testing import random_qty
        from pycountry import countries

        return random_qty(dict(n=len(countries), y=2, s=5, v=2), units="kg").pipe(
            relabel,
            n={f"n{i}": c.alpha_3 for i, c in enumerate(countries)},
            v={"v0": "Population", "v1": "GDP"},
            y={"y0": 2010, "y1": 2050},
        )
