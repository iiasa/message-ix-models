"""Generic tools for working with exogenous data sources."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from operator import itemgetter
from typing import TYPE_CHECKING, Optional, Union
from warnings import warn

from genno import Key, quote
from genno.core.key import iter_keys, single_key

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes

if TYPE_CHECKING:
    from pathlib import Path

    from genno import Computer
    from genno.types import AnyQuantity

    from message_ix_models import Context

__all__ = [
    "MEASURES",
    "SOURCES",
    "BaseOptions",
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


@dataclass
class BaseOptions:
    """Options for a ExoDataSource subclass instance.

    See :attr:`ExoDataSource.Options`.
    """

    #: :any:`True` if :meth:`ExoDataSource.transform` should aggregate data on the |n|
    #: dimension.
    aggregate: bool = True

    #: :any:`True` if :meth:`ExoDataSource.transform` should interpolate data on the |y|
    #: dimension.
    interpolate: bool = True

    #: Name for the returned :class:`.Key`/:class:`.Quantity`. Optional. See
    #: :meth:`ExoDataSource.get_keys`.
    name: str = ""

    @classmethod
    def from_args(cls, source_id: str, *args, **kwargs):
        """Helper for old-style configuration."""
        if 2 == len(args) and not kwargs:
            # Old-style source, source_kw as positional args
            assert source_id == args[0]
            return cls(**args[1])
        elif set(kwargs) == {"source", "source_kw"} and not args:
            # Old-style source, source_kw as keyword args
            assert source_id == kwargs["source"]
            return cls(**kwargs["source_kw"])
        else:
            assert 0 == len(args)
            return cls(**kwargs)


class ExoDataSource(ABC):
    """Base class for sources of exogenous data."""

    #: Subclass of :class:`BaseOptions` that contains per-instance options for this data
    #: source.
    Options: type[BaseOptions] = BaseOptions

    #: Instance of the :attr:`Options` class.
    options: BaseOptions

    #: Key for the returned :class:`.Quantity`. See :meth:`get_keys`. Optional in
    #: subclasses.
    key: Optional[Key] = None

    #: Primary measure. See :meth:`get_keys`. Optional in subclasses.
    measure = ""

    #: Additional dimensions for the returned :class:`.Key`/:class:`.Quantity`.
    #: If not set by :meth:`.__init__`, the dimensions are :math:`(n, y)`. Optional in
    #: subclasses.
    extra_dims: tuple[str, ...] = ()

    #: :any:`True` to allow the class to look up and use test data. If no test data
    #: exists, this setting has no effect.
    use_test_data: bool = False

    #: :py:`where` keyword argument to :func:`.path_fallback`.
    where: list[Union[str, "Path"]] = []

    #: Deprecated: identifier for this particular source.
    id: str = ""

    @abstractmethod
    def __init__(self, *args, **kwargs) -> None:
        """Create an instance and prepare.

        An implementation **must**:

        1. Raise an exception if it does not recognize or cannot handle the arguments.

        It **may**:

        2. Populate :attr:`options` with an instance of :attr:`Options`.
        3. Recognize and handle (if possible) a "measure" `kwarg` from :data:`MEASURES`.
        4. Transform arguments into other values, for instance by mapping certain values
           to others, applying regular expressions, or other operations.
        5. Store those values as instance attributes for use in :meth:`__call__`.
        6. Set :attr:`~.BaseOptions.name` and/or :attr:`extra_dims` to control the
           behaviour of :meth:`get_keys`.
        7. Log messages that give information that helps to debug an exception per (1).

        It **should not** perform any time- or memory-intensive operations, such as
        actually loading or fetching data. These should only be triggered by
        :meth:`.__call__`.
        """
        raise ValueError

    @abstractmethod
    def __call__(self) -> "AnyQuantity":
        r"""Return the data.

        Concrete implementations in subclasses **may** load data from file, generate
        data, or anything else.

        The Quantity returned by this method **must** have dimensions
        :math:`(n, y) \cup \text{extra_dims}`. If the original/upstream/raw data has
        different dimensionality (fewer or more dimensions; different dimension IDs),
        the code **must** transform these, make appropriate selections, etc.
        """
        raise NotImplementedError

    @classmethod
    def _where(self) -> list[Union[str, "Path"]]:
        """Helper for :py:`__init__()` methods in subclasses.

        Return :attr:`where`, but if :attr:`use_test_data` is :any:`True`, also append
        :py:`["test"]`.
        """
        return self.where + (["test"] if self.use_test_data else [])

    @classmethod
    def add_tasks(
        cls, c: "Computer", *args, context: "Context", strict: bool = True, **kwargs
    ) -> tuple:
        """Add tasks to `c` to provide and transform the data.

        .. todo:: Add option/steps to index to a particular label on the |n| dimension.

        Returns
        -------
        tuple
            with 2 keys:

            1. Key that retrieves the transformed data.
            2. Key with the data indexed to |y0|.
        """
        source = cls(*args, **kwargs)

        add_structure(c, context=context, strict=strict)

        # Retrieve the keys that will refer to the raw and transformed data
        k_raw, k = source.get_keys()

        # Keys to return
        keys = [k]

        # Retrieve the raw data by invoking ExoDataSource.__call__
        c.add(k_raw, source.__call__)

        # Allow the transform() or a subclass override to add further tasks that
        # transform the data. The default implementation aggregates, then interpolates.
        key = source.transform(c, k_raw)

        # Alias `key` -> `k`
        if k != key:
            c.add(k, key)

        # Index to y0
        k_y0 = k + "y0_indexed"
        c.add(k_y0, "index_to", k, "y0::coord")
        keys.append(k_y0)

        return tuple(keys)

    def get_keys(self) -> tuple[Key, Key]:
        """Return the target keys for the (1) raw and (2) transformed data.

        Subclasses **may** override this method to provide different targets keys. In
        the default implementation, the key for the transformed data is:

        1. :attr:`.key`, if any, or
        2. Constructed from:

           - :attr:`~BaseOptions.name` or :attr:`.measure` in lower-case.
           - The dimensions :math:`(n, y)`, plus any :attr:`.extra_dims`.

        The key for the raw data is the same, with :attr`.id` as an extra tag.
        """
        k = self.key or Key(
            self.options.name or self.measure.lower(), ("n", "y") + self.extra_dims
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

        1. If :attr:`~BaseOptions.aggregate` is :any:`True`, aggregates the data (
           :func:`.genno.operator.aggregate`) on the |n| dimension using the key
           "n::groups".
        2. If :attr:`~BaseOptions.interpolate` is :any:`True`, interpolates the data (
           :func:`.genno.operator.interpolate`) on the |y| dimension using "y::coords".
        """
        k = base_key

        # Aggregate
        if self.options.aggregate:
            k = single_key(c.add(k + "1", "aggregate", k, "n::groups", keep=False))

        # Interpolate to the desired set of periods
        if self.options.interpolate:
            kw = dict(fill_value="extrapolate")
            k = single_key(c.add(k + "2", "interpolate", k, "y::coords", kwargs=kw))

        return k

    def raise_on_extra_kw(self, kwargs) -> None:
        """Helper for subclasses to handle the `source_kw` argument.

        1. Store :attr:`~BaseOptions.aggregate` and :attr:`~BaseOptions.interpolate`, if
           they remain in `kwargs`.
        2. Raise :class:`TypeError` if there are any other, unhandled keyword arguments
           in `kwargs`.

        .. deprecated::
           :class:`.ExoDataSource` subclasses should instead, in their :py:`__init__()`
           method, use :attr:`Options` that inherit from :class:`BaseOptions`
        """
        if not hasattr(self, "options"):
            kwargs.setdefault("aggregate", getattr(self, "aggregate", True))
            kwargs.setdefault("interpolate", getattr(self, "interpolate", True))
            kwargs.setdefault("name", getattr(self, "name", None))

            # Raises exception on extra/unrecognized `kwargs`
            self.options = self.Options(**kwargs)


def add_structure(c: "Computer", *, context: "Context", strict: bool = True) -> None:
    """Add structural information to `c`.

    Helper for :meth:`ExoDataSource.add_tasks` and :func:`prepare_computer`.
    """
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
    # TODO Move to somewhere usable without calling the current function
    c.add("y::coords", lambda years: dict(y=years), "y")
    c.add("yv::coords", lambda years: dict(yv=years), "y")
    c.add("y0::coord", lambda year: dict(y=year), "y0")


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

    .. deprecated::
       Use :meth:`ExoDataSource.add_tasks` instead, like so:

       .. code-block:: python

          from example import DataSource

          c.apply(DataSource.add_tasks, context=context, **source_kw)

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
        See :meth:`ExoDataSource.add_tasks`.

    Raises
    ------
    ValueError
        if no source is registered which can handle `source` and `source_kw`.
    """
    # Handle arguments
    source_kw = source_kw or dict()

    # Look up input data flow
    for cls in SOURCES.values():
        try:
            keys = c.apply(
                cls.add_tasks,
                source=source,
                source_kw=deepcopy(source_kw),
                context=context,
            )
        except Exception:
            pass  # Class does not recognize the arguments
        # except Exception as e:  # For debugging
        #     log.debug(f"{cls} → {e!r}")
        else:
            warn(
                f"prepare_computer(…, c, {source!r}, source_kw); instead use "
                f"c.apply({cls.__name__}.add_tasks, context=…, **source_kw)",
                DeprecationWarning,
                stacklevel=2,
            )
            return tuple(iter_keys(keys))

    raise ValueError(
        f"No source found that can handle {source=!r}, {source_kw=!r} among:\n  "
        + "\n  ".join(sorted(SOURCES))
    )


def register_source(
    cls: type[ExoDataSource], *, id: Optional[str] = None
) -> type[ExoDataSource]:
    """Register :class:`.ExoDataSource` `cls` as a source of exogenous data."""
    id_ = id or cls.id
    if id_ in SOURCES:
        raise ValueError(f"{SOURCES[id_]} already registered for ID {id_!r}")
    SOURCES[id_] = cls
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

    def __call__(self) -> "AnyQuantity":
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
