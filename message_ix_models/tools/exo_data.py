"""Generic tools for working with exogenous data sources."""
from abc import ABC, abstractmethod
from operator import itemgetter
from typing import Dict, Mapping, Optional, Type

from genno import Computer, Key, Quantity, quote
from genno.core.key import single_key

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

#: Supported measures.
#:
#: .. todo:: Store this in a separate code list or concept scheme.
MEASURES = ("GDP", "POP")

#: Known sources for data. Use :func:`register_source` to add to this collection.
SOURCES: Dict[str, Type["ExoDataSource"]] = {}


class ExoDataSource(ABC):
    """Base class for sources of exogenous data."""

    #: Identifier for this particular source.
    id: str = ""

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
        - Store those values as instance attributes for use in :meth:`__call__`, below.

        It **should not** actually load data or perform any time- or memory-intensive
        operations.
        """
        raise ValueError

    @abstractmethod
    def __call__(self) -> Quantity:
        """Return the data.

        The Quantity returned by this method **must** have dimensions "n" and "y". If
        the original/upstream/raw data has additional dimensions or different dimension
        IDs, the code **must** transform these, make appropriate selections, etc.
        """
        raise NotImplementedError


def prepare_computer(
    context, c: "Computer", source="test", source_kw: Optional[Mapping] = None
):
    """Prepare `c` to compute GDP, population, or other exogenous data.

    Returns a tuple of keys. The first, like ``{m}:n-y``, triggers the following
    computations:

    1. Load data by invoking a :class:`ExoDataSource`.
    2. Aggregate on the ``n`` (``node``) dimension according to :attr:`Config.regions`.
    3. Interpolate on the ``y`` (``year``) dimension according to :attr:`Config.years`.

    Additional key(s) include:

    - ``{m}:n-y:y0 indexed``: same as ``{m}:n-y``, indexed to values as of ``y0``, that
      is, the first model year.

    .. todo:: Extend to also prepare to compute values indexed to a particular ``n``.

    Parameters
    ----------
    source : str
        Identifier of the source, possibly with other information to be handled by a
        :class:`ExoDataSource`.
    source_kw : dict, *optional*
        Keyword arguments for a Source class. These can include indexers, selectors, or
        other information needed by the source class to identify the data to be
        returned.

        If the key "measure" is present, it **must** be one of :data:`MEASURES`.
    """
    # Handle arguments
    source_kw = source_kw or dict()
    if measure := source_kw.get("measure"):
        if measure not in MEASURES:
            raise ValueError(
                f"source_kw 'measure' must be one of {MEASURES}; got {measure!r}"
            )
    else:
        measure = "UNKNOWN"

    # Look up input data flow
    source_obj = None
    for cls in SOURCES.values():
        try:
            # Instantiate a Source object to provide this data
            source_obj = cls(source, source_kw or dict())
        except Exception:
            pass  # Class does not recognize the arguments

    if source_obj is None:
        raise ValueError(f"No source found that can handle {source!r}")

    # Add structural information to the Computer
    c.require_compat("message_ix_models.report.computations")

    # Retrieve the node codelist
    c.add("n::codes", quote(get_codes(f"node/{context.model.regions}")), strict=True)

    # Convert the codelist into a nested dict for aggregate()
    c.add("n::groups", "codelist_to_groups", "n::codes", strict=True)

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

    # Retrieve the raw data
    k = Key(measure.lower(), "ny")
    k_raw = k + source_obj.id  # Tagged with the source ID
    keys = [k]  # Keys to return

    c.add(k_raw, source_obj)

    # Aggregate
    c.add(k_raw + "agg", "aggregate", k_raw, "n::groups", keep=False)

    # Interpolate to the desired set of periods
    kwargs = dict(fill_value="extrapolate")
    c.add(k, "interpolate", k_raw + "agg", "y::coords", kwargs=kwargs)

    # Index to y0
    keys.append(single_key(c.add(k + "y0 indexed", "index_to", k, "y0::coord")))

    # TODO also insert (1) index to a particular label on the "n" dimension (2) both

    return tuple(keys)


def register_source(cls: Type[ExoDataSource]) -> Type[ExoDataSource]:
    """Register `cls` as a source of exogenous data."""
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
        self.indexers.update(
            v={"POP": "Population", "GDP": "GDP"}[source_kw["measure"]]
        )

    def __call__(self) -> Quantity:
        from genno.computations import select

        # - Retrieve the data.
        # - Apply the prepared indexers.
        return self.random_data().pipe(select, self.indexers, drop=True)

    @staticmethod
    def random_data():
        """Generate some random data with n, y, s, and v dimensions."""
        from genno.computations import relabel
        from genno.testing import random_qty
        from pycountry import countries

        return random_qty(dict(n=len(countries), y=2, s=5, v=2), units="kg").pipe(
            relabel,
            n={f"n{i}": c.alpha_3 for i, c in enumerate(countries)},
            v={"v0": "Population", "v1": "GDP"},
            y={"y0": 2010, "y1": 2050},
        )
