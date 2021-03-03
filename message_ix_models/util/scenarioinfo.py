from collections import defaultdict
from itertools import product
from typing import Dict, List

import pandas as pd


class ScenarioInfo:
    """Information about a :class:`~message_ix.Scenario` object.

    Code that prepares data for a target Scenario can accept a ScenarioInfo instance.
    This avoids any need to load a Scenario, which can be slow under some conditions.
    ScenarioInfo objects are also used by :func:`.apply_spec` to describe the contents
    of a scenario before it is created.

    ScenarioInfo objects have the following attributes:

    .. autosummary::
       set
       is_message_macro
       N
       Y
       y0
       yv_ya
    """

    #: Elements of :mod:`ixmp`/:mod:`message_ix` sets in the Scenario.
    set: Dict[str, List] = {}

    #: First model year, if set, else ``Y[0]``.
    y0: int = -1

    #: :obj:`True` if a MESSAGE-MACRO scenario.
    is_message_macro: bool = False

    _yv_ya: pd.DataFrame = None

    def __init__(self, scenario=None):
        self.set = defaultdict(list)

        if not scenario:
            return

        for name in scenario.set_list():
            try:
                self.set[name] = scenario.set(name).tolist()
            except AttributeError:
                continue  # pd.DataFrame for ≥2-D set; don't convert

        self.is_message_macro = "PRICE_COMMODITY" in scenario.par_list()

        # Computed once
        fmy = scenario.cat("year", "firstmodelyear")
        self.y0 = int(fmy[0]) if len(fmy) else self.set["year"][0]

        self._yv_ya = scenario.vintage_and_active_years()

    @property
    def yv_ya(self):
        """(year_vtg, year_act) for the entire model horizon."""
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
