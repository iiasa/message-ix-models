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

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.structure import get_codes

if TYPE_CHECKING:
    from pathlib import Path

    from genno import Computer
    from genno.types import AnyQuantity


__all__ = [
    "MEASURES",
    "SOURCES",
    "BaseOptions",
    "DemoSource",
    "ExoDataSource",
    "add_structure",
    "prepare_computer",
    "register_source",
]

log = logging.getLogger(__name__)

#: Measures recognized by some data sources. Concrete :class:`.ExoDataSource` subclasses
#: may provide support for other measures.
#:
#: .. todo:: Store this in a separate code list or concept scheme.
MEASURES = ("GDP", "POP")

#: Registered sources for data. Use :func:`register_source` to add to this collection.
SOURCES: dict[str, type["ExoDataSource"]] = {}


@dataclass
class BaseOptions:
    """Options for a concrete ExoDataSource subclass.

    See :attr:`ExoDataSource.Options`.
    """

    #: :any:`True` if :meth:`ExoDataSource.transform` should aggregate data on the |n|
    #: dimension.
    aggregate: bool = True

    #: :any:`True` if :meth:`ExoDataSource.transform` should interpolate data on the |y|
    #: dimension.
    interpolate: bool = True

    #: Identifier for the primary measure of retrieved/returned data.
    measure: str = ""

    #: Name for the returned :class:`.Key`/:class:`.Quantity`.
    name: str = ""

    #: Dimensions for the returned :class:`.Key`/:class:`.Quantity`.
    dims: tuple[str, ...] = ("n", "y")

    @classmethod
    def from_args(cls, source_id: Union[str, "ExoDataSource"], *args, **kwargs):
        """Construct an instance from keyword arguments.

        Parameters
        ----------
        source_id
            For backwards-compatibility with :func:`prepare_computer`.
        """
        if not isinstance(source_id, str):
            source_id = type(source_id).__name__

        if 2 == len(args) and not kwargs:
            # Old-style source, source_kw as positional args
            kwargs, args = dict(source_id=args[0], source_kw=args[1]), ()

        if set(kwargs) == {"source", "source_kw"} and not args:
            # Old-style source, source_kw as keyword args
            if source_id != kwargs["source"]:
                raise ValueError(f"source_id == {source_id!r} != {kwargs['source']!r}")
            return cls(**kwargs["source_kw"])

        assert 0 == len(args)
        return cls(**kwargs)


class ExoDataSource(ABC):
    """Abstract class for sources of exogenous data."""

    #: Class defining per-instance options understood by this data source.
    #:
    #: An concrete class **may** override this with a subclass of :class:`.BaseOptions`.
    #: That subclass **may** change the default values of any attributes of BaseOptions,
    #: or add others.
    Options: type[BaseOptions] = BaseOptions

    #: Instance of the :attr:`Options` class.
    #:
    #: A concrete class that overrides :attr:`.Options` **should** redefine this
    #: attribute, to facilitate type checking.
    options: BaseOptions

    #: Key for the returned :class:`.Quantity`. This **may** either be set statically
    #: on a concrete subclass, or created via :meth:`__init__`.
    key: Key

    #: :any:`True` to allow the class to look up and use test data. If no test data
    #: exists, this setting has no effect. See :meth:`_where`.
    use_test_data: bool = False

    #: :py:`where` keyword argument to :func:`.path_fallback`. See :meth:`_where`.
    where: list[Union[str, "Path"]] = []

    # Class methods

    @classmethod
    def _where(self) -> list[Union[str, "Path"]]:
        """Helper for :meth:`__init__` methods in concrete classes.

        Return :attr:`where`

        If :attr:`use_test_data` is :any:`True`, also append :py:`"test"`.
        """
        return self.where + (["test"] if self.use_test_data else [])

    @classmethod
    def add_tasks(
        cls,
        c: "Computer",
        *args,
        context: Optional["Context"] = None,
        strict: bool = True,
        **kwargs,
    ) -> tuple:
        """Add tasks to `c` to provide and transform the data.

        The first returned key is :attr:`.key`, and will trigger the following tasks:

        1. Load or retrieve data by invoking :meth:`.ExoDataSource.get`.
        2. If :attr:`.BaseOptions.aggregate` is :any:`True`, aggregate on the |n| (node)
           dimension according to :attr:`.Config.regions`.
        3. If :attr:`.BaseOptions.interpolate` is :any:`True`, interpolate on the |y|
           (year) dimension according to :attr:`.Config.years`.

        Steps (2) and (3) are added by :meth:`.transform` and **may** differ in
        concrete classes.

        Other returned keys include further transformations:

        - :py:`key + "y0_indexed"`: same as :attr:`.key`, but indexed to the values as
          of the first model period.

        Other keys that are created but not returned can be accessed on `c`:

        - :py:`key + "message_ix_models.foo.bar.CLASS"`: the raw data, with a tag from
          the fully-qualified name of the ExoDataSource class.

        To support the loading and transformation of data, :func:`add_structure` is
        first called with `c`.

        .. todo:: Add option/tasks to index to a particular label on the |n| dimension.

        Parameters
        ----------
        context
            Passed to :func:`add_structure`.
        strict
            Passed to :func:`add_structure`.

        Returns
        -------
        tuple of .Key
        """
        # Create an instance of `cls`
        source = cls(*args, **kwargs)

        # Identify a context
        context = context or c.graph.get("context")
        if not context:
            log.warning(
                f'No ExoDataSource.add_tasks(…, context=…) and no "context" key in {c};'
                " using newest instance"
            )
            context = Context.get_instance(-1)

        # Add structure
        add_structure(c, context=context, strict=strict)

        # Prepare keys that will refer to the final/transformed and raw data
        k = source.key
        k_raw = k + f"{cls.__module__}.{cls.__name__}"
        result = [k]

        # Retrieve the raw data and convert to Quantity
        c.add(k_raw, source.get)

        # Transform the raw data according to .transform() (aggregate then interpolate)
        # or a subclass override
        k_transformed = source.transform(c, k_raw)

        # Alias `key` → `k`, the target key
        if k != k_transformed:
            c.add(k, k_transformed)

        # Index to y0
        result.append(k["y0_indexed"])
        c.add(result[-1], "index_to", k, "y0::coord")

        return tuple(result)

    # Instance methods

    def __init__(self, *args, **kwargs) -> None:
        """Create an instance and prepare info for :meth:`transform`/:meth:`get`.

        The base implementation:

        - Sets :attr:`options`—if not already set—by passing `kwargs` to
          :attr:`Options`.
        - Raises an exception if there are other/unhandled `args` or `kwargs`.
        - If :attr:`key` is not set, constructs it with:

          - Name :attr:`~BaseOptions.name` or :attr:`~BaseOptions.measure` in lower
            case.
          - Dimensions :attr:`~BaseOptions.dims`.

          Subclasses **may** pre-empt this behaviour by setting :attr:`key` statically
          or dynamically.

        A concrete class implementation **must**:

        - Set :attr:`options`, either directly or by calling :py:`super().__init__()`
          with or without keyword arguments.
        - Set :attr:`key`, either directly or by calling :py:`super().__init__()`. In
          the latter case, it **may** set :attr:`~.BaseOptions.name`,
          :attr:`~.BaseOptions.measure`, and/or :attr:`~.BaseOptions.dims` to control
          the behaviour.
        - Raise an exception if unrecognized or invalid `kwargs` are passed.

        and **may**:

        - Transform `kwargs` or :attr:`options` arguments into other values, for
          instance by mapping certain values to others, applying regular expressions, or
          other operations.
        - Store those values as instance attributes for use in :meth:`get`.
        - Log messages that give information that helps to debug exceptions.

        It **must not** perform any time- or memory-intensive operations, such as
        actually loading or fetching data. Those operations should be in :meth:`get`.
        """
        if len(args):
            raise ValueError(f"Unexpected args to ExoDataSource(): {args}")
        elif not hasattr(self, "options"):
            self.options = self.Options(**kwargs)
        elif len(kwargs):
            msg = (
                f"Unhandled extra keyword arguments to {type(self).__name__}: "
                + repr(kwargs)
            )
            log.error(msg)
            raise ValueError(msg)
        if not hasattr(self, "key"):
            # Key name
            name = self.options.name or self.options.measure.lower()
            self.key = Key(name, self.options.dims)

    @abstractmethod
    def get(self) -> "AnyQuantity":
        r"""Return the data.

        Implementations in concrete classes **may** load data from file, retrieve from
        remote sources or local caches, generate data, or anything else.

        The Quantity returned by this method **must** have dimensions corresponding to
        :attr:`key`. If the original/upstream/raw data has different dimensionality
        (fewer or more dimensions; different dimension IDs), a concrete class **must**
        transform these, make appropriate selections, etc.
        """
        raise NotImplementedError

    def transform(self, c: "Computer", base_key: Key) -> Key:
        """Add tasks to `c` to transform raw data from `base_key`.

        `base_key` refers to the :class:`.Quantity` returned by :meth:`get`. Via
        :meth:`add_tasks`, :meth:`transform` adds additional tasks to `c` that further
        transform the data. (Such operations **may** be done in :meth:`get` directly,
        but :meth:`transform` allows use of :mod:`genno` operators and conveniences.)

        In the default implementation:

        1. If :attr:`~BaseOptions.aggregate` is :any:`True`, aggregate the data (
           :func:`.genno.operator.aggregate`) on the |n| dimension using the key
           "n::groups".
        2. If :attr:`~BaseOptions.interpolate` is :any:`True`, interpolate the data (
           :func:`.genno.operator.interpolate`) on the |y| dimension using "y::coords".

        Concrete classes **may** override this method to, for instance, change how
        `aggregate` and `interpolate` are handled, or add further steps. Such overrides
        **may** call the base implementation, or not.

        Returns
        -------
        .Key
            referring to the data from `base_key` after any transformation. This **may**
            be the same as `base_key`.
        """
        k = base_key

        # Aggregate
        if self.options.aggregate:
            k = single_key(c.add(k[1], "aggregate", k, "n::groups", keep=False))

        # Interpolate to the desired set of periods
        if self.options.interpolate:
            kw = dict(fill_value="extrapolate")
            k = single_key(c.add(k[2], "interpolate", k, "y::coords", kwargs=kw))

        return k


def add_structure(c: "Computer", *, context: "Context", strict: bool = True) -> None:
    """Add structural information to `c`.

    Helper for :meth:`ExoDataSource.add_tasks` and :func:`prepare_computer`.

    The added tasks include:

    1. "context": `context`, if not already set.
    2. "n::codes": :func:`get_codes` for the node code list according to
       :attr:`.Config.regions`.
    3. "n::groups": :func:`codelist_to_groups` called on "n::codes".
    4. "y": list of periods according to :attr:`.Config.years`, if not already set.
    5. "y0": first element of "y".
    6. "y::coords":  :class:`dict` mapping :py:`str("y")` to the elements of "y".
    7. "yv::coords": :class:`dict` mapping :py:`str("yv")` to the elements of "y".
    8. "y0::coord": :class:`dict` mapping :py:`str("y")` to "y0".

    Parameters
    ----------
    strict
        if :any:`True`, raise exceptions if the keys to be added are already in `c`.
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

    .. deprecated:: 2025-06-06
       Use :meth:`ExoDataSource.add_tasks` instead. See :mod:`.exo_data`.

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
    id_ = id or cls.__name__
    if id_ in SOURCES:
        raise ValueError(f"{SOURCES[id_]} already registered for ID {id_!r}")

    SOURCES[id_] = cls

    return cls


@register_source
class DemoSource(ExoDataSource):
    """Example source of exogenous population and GDP data."""

    @dataclass
    class Options(BaseOptions):
        scenario: str = ""

    def __init__(self, *args, **kwargs) -> None:
        # Handle old-style positional or keyword arg like source="test s1", where "s1"
        # is the value for Options.scenario
        if args:
            source = args[0]
        elif source := kwargs.get("source"):
            pass

        if source:
            prefix = "test "
            _, source_id, scenario = source.rpartition(prefix)
            if not source_id == prefix:
                # Don't recognize this `source` string → can't provide data
                raise ValueError(source)
        else:
            scenario = None

        opt = self.options = self.Options.from_args(source, *args, **kwargs)

        # Use an explicit scenario ID or part of "source_id"
        # Map from the measure ID to a variable name
        self.indexers = dict(
            s=opt.scenario or scenario,
            v={"POP": "Population", "GDP": "GDP"}[opt.measure],
        )

        super().__init__()

    def get(self) -> "AnyQuantity":
        from genno.operator import select

        # - Retrieve the data.
        # - Apply the prepared indexers.
        return self.random_data().pipe(select, self.indexers, drop=True)  # type: ignore [arg-type]

    @staticmethod
    def random_data() -> "AnyQuantity":
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
