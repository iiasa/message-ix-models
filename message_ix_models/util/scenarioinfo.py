""":class:`ScenarioInfo` class."""
import logging
from collections import defaultdict
from itertools import product
from typing import Dict, List

import pandas as pd
import sdmx.model

from message_ix_models.util import eval_anno

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
