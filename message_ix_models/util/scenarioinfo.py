""":class:`ScenarioInfo` class."""
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import product
from typing import Dict, List

import pandas as pd
import sdmx.model

log = logging.getLogger(__name__)


class ScenarioInfo:
    """Information about a :class:`~message_ix.Scenario` object.

    Code that prepares data for a target Scenario can accept a ScenarioInfo instance.
    This avoids the need to load a Scenario, which can be slow under some conditions.

    ScenarioInfo objects can also be used (e.g. by :func:`.apply_spec`) to describe the
    contents of a Scenario *before* it is created.

    ScenarioInfo objects have the following convenience attributes:

    .. autosummary::
       set
       is_message_macro
       N
       Y
       y0
       yv_ya

    Parameters
    ----------
    scenario : message_ix.Scenario
        If given, :attr:`.set` is initialized from this existing scenario.

    See also
    --------
    .Spec
    """

    #: Elements of :mod:`ixmp`/:mod:`message_ix` sets.
    set: Dict[str, List] = {}

    #: Elements of :mod:`ixmp`/:mod:`message_ix` parameters.
    par: Dict[str, pd.DataFrame] = {}

    #: First model year, if set, else ``Y[0]``.
    y0: int = -1

    #: :obj:`True` if a MESSAGE-MACRO scenario.
    is_message_macro: bool = False

    _yv_ya: pd.DataFrame = None

    def __init__(self, scenario=None):
        self.set = defaultdict(list)
        self.par = dict()

        if not scenario:
            return

        for name in scenario.set_list():
            try:
                self.set[name] = scenario.set(name).tolist()
            except AttributeError:
                continue  # pd.DataFrame for ≥2-D set; don't convert

        for name in ("duration_period",):
            self.par[name] = scenario.par(name)

        self.is_message_macro = "PRICE_COMMODITY" in scenario.par_list()

        # Computed once
        fmy = scenario.cat("year", "firstmodelyear")
        self.y0 = int(fmy[0]) if len(fmy) else self.set["year"][0]

        self._yv_ya = scenario.vintage_and_active_years()

    @property
    def yv_ya(self):
        """:class:`pandas.DataFrame` with valid ``year_vtg``, ``year_act`` pairs."""
        if self._yv_ya is None:
            first = self.y0

            # Product of all years
            yv = ya = self.set["year"]

            # Predicate for filtering years
            def _valid(elem):
                yv, ya = elem
                return first <= yv <= ya

            # - Cartesian product of all yv and ya.
            # - Filter only valid years.
            # - Convert to data frame.
            self._yv_ya = pd.DataFrame(
                filter(_valid, product(yv, ya)), columns=["year_vtg", "year_act"]
            )

        return self._yv_ya

    @property
    def N(self):
        """Elements of the set 'node'."""
        return list(map(str, self.set["node"]))

    @property
    def Y(self):
        """Elements of the set 'year' that are >= the first model year."""
        return list(filter(lambda y: y >= self.y0, self.set["year"]))

    def update(self, other: "ScenarioInfo"):
        """Update with the set elements of `other`."""
        for name, data in other.set.items():
            self.set[name].extend(filter(lambda id: id not in self.set[name], data))

        for name, data in other.par.items():
            raise NotImplementedError("Merging parameter data")

    def __repr__(self):
        return (
            f"<ScenarioInfo: {sum(len(v) for v in self.set.values())} code(s) in "
            f"{len(self.set)} set(s)>"
        )

    def year_from_codes(self, codes: List[sdmx.model.Code]):
        """Update using a list of `codes`.

        The following are updated:

        - :attr:`.set` ``year``
        - :attr:`.set` ``cat_year``, with the first model year.
        - :attr:`.par` ``duration_period``

        Any existing values are discarded.

        After this, the attributes :attr:`.y0` and :attr:`.Y` give the first model year
        and model years, respectively.

        Examples
        --------
        Get a particular code list, create a ScenarioInfo instance, and update using
        the codes:

        >>> years = get_codes("year/A")
        >>> info = ScenarioInfo()
        >>> info.year_from_codes(years)

        Use populated values:

        >>> info.y0
        2020
        >>> info.Y[:3]
        [2020, 2030, 2040]
        >>> info.Y[-3:]
        [2090, 2100, 2110]

        """
        from message_ix_models.util import eval_anno

        # Clear existing values
        if len(self.set["year"]):
            log.debug(f"Discard existing 'year' elements: {repr(self.set['year'])}")
            self.set["year"] = []
        if len(self.set["cat_year"]):
            log.debug(
                f"Discard existing 'cat_year' elements: {repr(self.set['cat_year'])}"
            )
            self.set["cat_year"] = []
        if "duration_period" in self.par:
            log.debug("Discard existing 'duration_period' elements")

        fmy_set = False
        duration_period: List[Dict] = []

        # TODO use sorted() here once supported by sdmx
        for code in codes:
            year = int(code.id)
            # Store the year
            self.set["year"].append(year)

            # Check for an annotation 'firstmodelyear: true'
            if eval_anno(code, "firstmodelyear"):
                if fmy_set:
                    # No coverage: data that triggers this should not be committed
                    raise ValueError(  # pragma: no cover
                        "≥2 periods are annotated firstmodelyear: true"
                    )

                self.y0 = year
                self.set["cat_year"].append(("firstmodelyear", year))
                fmy_set = True

            # Store the duration_period: either from an annotation, or computed vs. the
            # prior period
            duration_period.append(
                dict(
                    year=year,
                    value=eval_anno(code, "duration_period")
                    or (year - duration_period[-1]["year"]),
                    unit="y",
                )
            )

        # Store
        self.par["duration_period"] = pd.DataFrame(duration_period)


@dataclass
class Spec:
    """A specification for the structure of a model or variant.

    A Spec collects 3 :class:`.ScenarioInfo` instances at the attributes :attr:`.add`,
    :attr:`.remove`, and :attr:`.require`. This is the type that is accepted by
    :func:`.apply_spec`; :doc:`model-build` describes how a Spec is used to modify a
    :class:`Scenario`. A Spec may also be used to express information about the target
    structure of data to be prepared; like :class:`.ScenarioInfo`, this can happen
    before the target :class:`.Scenario` exists.

    Spec also provides:

    - Dictionary-style access, e.g. ``s["add"]`` is equivalent to ``s.add.``.
    - Error checking; setting keys other than add/remove/require results in an error.
    - :meth:`.merge`, a helper method.
    """

    #: Structure to be added to a base scenario.
    add: ScenarioInfo = field(default_factory=ScenarioInfo)
    #: Structure to be removed from a base scenario.
    remove: ScenarioInfo = field(default_factory=ScenarioInfo)
    #: Structure that must be present in a base scenario.
    require: ScenarioInfo = field(default_factory=ScenarioInfo)

    # Dict-like features

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value: ScenarioInfo):
        if not hasattr(self, key):
            raise KeyError(key)
        setattr(self, key, value)

    def values(self):
        yield self.add
        yield self.remove
        yield self.require

    # Static methods

    @staticmethod
    def merge(a: "Spec", b: "Spec") -> "Spec":
        """Merge Specs `a` and `b` together.

        Returns a new Spec where each member is a union of the respective members of
        `a` and `b`.
        """
        result = Spec()

        for key in {"add", "remove", "require"}:
            result[key].update(a[key])
            result[key].update(b[key])

        return result
