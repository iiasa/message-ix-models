""":class:`ScenarioInfo` class."""

import logging
import re
from collections import defaultdict
from dataclasses import InitVar, dataclass, field
from itertools import product
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd
import pint
import sdmx.model.v21 as sdmx_model

from .ixmp import parse_url

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)


# TODO: use kw_only=True once python 3.10 is oldest supported version
@dataclass()
class ScenarioInfo:
    """Information about a |Scenario| object.

    Code that prepares data for a target Scenario can accept a ScenarioInfo instance.
    This avoids the need to create or load an actual Scenario, which can be slow under
    some conditions.

    ScenarioInfo objects can also be used (for instance, by :func:`.apply_spec`) to
    describe the contents of a Scenario *before* it is created.

    ScenarioInfo objects have the following convenience attributes:

    .. autosummary::
       ~ScenarioInfo.set
       io_units
       is_message_macro
       N
       units_for
       Y
       y0
       yv_ya

    Parameters
    ----------
    scenario_obj : message_ix.Scenario
        If given, :attr:`.set` is initialized from this existing scenario.

    Examples
    --------
    Iterating over an instance gives "model", "scenario", "version" and the values of
    the respective attributes:
    >>> si = ScenarioInfo.from_url("model name/scenario name#123")
    >>> dict(si)
    {'model': 'model name', 'scenario': 'scenario name', 'version': 123}

    See also
    --------
    .Spec
    """

    # TODO: give this field kw_only=False once python 3.10 is the minimum version
    # Parameters for initialization only
    scenario_obj: InitVar[Optional["Scenario"]] = field(default=None)
    empty: InitVar[bool] = False

    platform_name: Optional[str] = None

    #: Model name; equivalent to :attr:`.TimeSeries.model`.
    model: Optional[str] = None

    #: Scenario name; equivalent to :attr:`.TimeSeries.scenario`.
    scenario: Optional[str] = None

    #: Scenario version; equivalent to :attr:`.TimeSeries.version`.
    version: Optional[int] = None

    #: Elements of :mod:`ixmp`/:mod:`message_ix` sets.
    set: Dict[str, List] = field(default_factory=lambda: defaultdict(list))

    #: Elements of :mod:`ixmp`/:mod:`message_ix` parameters.
    par: Dict[str, pd.DataFrame] = field(default_factory=dict)

    #: First model year, if set, else ``Y[0]``.
    y0: int = -1

    #: :obj:`True` if a MESSAGE-MACRO scenario.
    is_message_macro: bool = False

    _yv_ya: Optional[pd.DataFrame] = None

    def __post_init__(self, scenario_obj: Optional["Scenario"], empty: bool):
        if not scenario_obj:
            return

        self.model = scenario_obj.model
        self.scenario = scenario_obj.scenario
        self.version = (
            None if scenario_obj.version is None else int(scenario_obj.version)
        )

        if empty:
            return

        # Copy structure (set contents)
        for name in scenario_obj.set_list():
            value = scenario_obj.set(name)
            try:
                self.set[name] = value.tolist()
            except AttributeError:
                self.set[name] = value  # pd.DataFrame for ≥2-D set; don't convert

        # Copy data for a limited set of parameters
        for name in ("duration_period",):
            self.par[name] = scenario_obj.par(name)

        self.is_message_macro = "PRICE_COMMODITY" in scenario_obj.par_list()

        # Computed once
        fmy = scenario_obj.cat("year", "firstmodelyear")
        self.y0 = int(fmy[0]) if len(fmy) else self.set["year"][0]

        self._yv_ya = scenario_obj.vintage_and_active_years()

    @classmethod
    def from_url(cls, url: str) -> "ScenarioInfo":
        """Create an instance using only an :attr:`url`."""
        result = cls()
        result.url = url
        return result

    @property
    def yv_ya(self):
        """:class:`pandas.DataFrame` with valid ``year_vtg``, ``year_act`` pairs."""
        if self._yv_ya is None:
            # - Cartesian product of all yv and ya.
            # - Convert to data frame.
            # - Filter only valid years.
            self._yv_ya = (
                pd.DataFrame(
                    product(self.set["year"], self.set["year"]),
                    columns=["year_vtg", "year_act"],
                )
                .query("@self.y0 <= year_vtg <= year_act")
                .reset_index(drop=True)
            )

        return self._yv_ya

    @property
    def N(self):
        """Elements of the set 'node'.

        See also
        --------
        .nodes_ex_world
        """
        return list(map(str, self.set["node"]))

    @property
    def Y(self) -> List[int]:
        """Elements of the set 'year' that are >= the first model year."""
        return list(filter(lambda y: y >= self.y0, self.set["year"]))

    @property
    def url(self) -> str:
        """Identical to :attr:`.TimeSeries.url`."""
        return f"{self.model}/{self.scenario}#{self.version}"

    @url.setter
    def url(self, value):
        p, s = parse_url(value)
        self.platform_name = p.get("name")
        for k in "model", "scenario", "version":
            setattr(self, k, s.get(k))

    _path_re = [
        (re.compile(r"[/<>:\"\\\|\?\*]+"), "_"),
        (re.compile("#"), "_v"),
        (re.compile("__+"), "_"),
    ]

    @property
    def path(self) -> str:
        """A valid file system path name similar to :attr:`url`.

        Characters invalid in Windows paths are replaced with "_".
        """
        from functools import reduce

        return reduce(lambda s, e: e[0].sub(e[1], s), self._path_re, self.url)

    def update(self, other: "ScenarioInfo"):
        """Update with the set elements of `other`."""
        for name, data_list in other.set.items():
            self.set[name].extend(
                filter(lambda id: id not in self.set[name], data_list)
            )

        for name, data_frame in other.par.items():
            raise NotImplementedError("Merging parameter data")

    def __iter__(self):
        for k in "model", "scenario", "version":
            yield (k, getattr(self, k))

    def __repr__(self):
        return (
            f"<ScenarioInfo: {sum(len(v) for v in self.set.values())} code(s) in "
            f"{len(self.set)} set(s)>"
        )

    def units_for(self, set_name: str, id: str) -> pint.Unit:
        """Return the units associated with code `id` in MESSAGE set `set_name`.

        :mod:`ixmp` (or the sole :class:`~ixmp.backend.base.JDBCBackend`, as of v3.5.0)
        does not handle unit information for variables and equations (unlike parameter
        values), such as MESSAGE decision variables ``ACT``, ``CAP``, etc. In
        :mod:`message_ix_models` and :mod:`message_data`, the following conventions are
        (generally) followed:

        - The units of ``ACT`` and others are consistent for each ``technology``.
        - The units of ``COMMODITY_BALANCE``, ``STOCK``, ``commodity_stock``, etc. are
          consistent for each ``commodity``.

        Thus, codes for elements of these sets (e.g. :ref:`commodity-yaml`) can be used
        to carry the standard units for the corresponding quantities. :func:`units_for`
        retrieves these units, for use in model-building and reporting.

        .. todo:: Expand this discussion and transfer to the :mod:`message_ix` docs.

        See also
        --------
        io_units
        """

        try:
            idx = self.set[set_name].index(id)
        except ValueError:
            print(self.set[set_name])
            raise

        return self.set[set_name][idx].eval_annotation(
            id="units", globals=dict(registry=pint.get_application_registry())
        )

    def io_units(
        self, technology: str, commodity: str, level: Optional[str] = None
    ) -> pint.Unit:
        """Return units for the MESSAGE ``input`` or ``output`` parameter.

        These are implicitly determined as the ratio of:

        - The units for the origin (for ``input``) or destination `commodity`, per
          :meth:`.units_for`.
        - The units of activity for the `technology`.

        Parameters
        ----------
        level : str
            Placeholder for future functionality, i.e. to use different units per
            (commodity, level). Currently ignored. If given, a debug message is logged.

        Raises
        ------
        ValueError
            if either `technology` or `commodity` lack defined units.
        """
        if level is not None:
            log.debug(f"{level = } ignored")
        c = self.units_for("commodity", commodity)
        t = self.units_for("technology", technology)
        if None in (c, t):
            raise ValueError(
                "Cannot compute input/output units for: "
                f"commodity={commodity!r} [{c}] / technology={technology!r} [{t}]"
            )
        return c / t

    def year_from_codes(self, codes: List[sdmx_model.Code]):
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
            if code.eval_annotation(id="firstmodelyear"):
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
                    value=code.eval_annotation(id="duration_period")
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
    |Scenario|. A Spec may also be used to express information about the target
    structure of data to be prepared; like ScenarioInfo, this can happen before the
    target Scenario exists.

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
