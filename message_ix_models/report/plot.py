"""Plots for MESSAGEix-GLOBIOM reporting.

The current set functions on time series data stored on the scenario by
:mod:`message_ix_models.report` or :mod:`message_data` legacy reporting.
"""

import logging
import re
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import genno.compat.plotnine
import pandas as pd
import plotnine as p9
from genno import Computer, Key

if TYPE_CHECKING:
    from genno.core.key import KeyLike
    from message_ix import Scenario

    # NB The following is itself within an if TYPE_CHECKING block; mypy can't find it
    from plotnine.typing import PlotAddable  # type: ignore [attr-defined]

    from message_ix_models import Context

__all__ = [
    "PLOTS",
    "EmissionsCO2",
    "FinalEnergy0",
    "FinalEnergy1",
    "Plot",
    "PrimaryEnergy0",
    "PrimaryEnergy1",
    "callback",
]

log = logging.getLogger(__name__)


class Plot(genno.compat.plotnine.Plot):
    """Base class for plots based on reported time-series data.

    Subclasses should be used like:

    .. code-block:: python

       class MyPlot(Plot):
           ...

       c.add("plot myplot", MyPlot, "scenario")

    …that is, giving "scenario" or another key that points to a :class:`.Scenario`
    object with stored time series data. See the examples in this file.
    """

    #: 'Static' geoms: list of plotnine objects that are not dynamic.
    static: list["PlotAddable"] = [
        p9.theme(figure_size=(23.4, 16.5)),  # A3 paper in landscape [inches]
        # p9.theme(figure_size=(11.7, 8.3)),  # A4 paper in landscape
    ]

    #: Fixed plot title string. If not given, the first line of the class docstring is
    #: used.
    title = None

    #: Units expression for plot title.
    unit = None

    #: Scenario URL for plot title.
    url: Optional[str] = None

    # NB only here to narrow typing
    inputs: Sequence[str] = []

    #: List of regular expressions corresponding to :attr:`inputs`. These are passed as
    #: the `expr` argument to :func:`.filter_ts` to filter the entire set of time series
    #: data.
    inputs_regex: list[re.Pattern] = []

    @classmethod
    def add_tasks(
        cls, c: "Computer", key: "KeyLike", *inputs, strict: bool = False
    ) -> "KeyLike":
        from copy import copy
        from itertools import zip_longest

        scenario_key = inputs[0]

        # Retrieve all time series data, for advanced filtering
        all_data = Key(scenario_key) + "iamc"
        c.add(all_data, "get_ts", scenario_key)

        if len(cls.inputs_regex):
            # Iterate over matched items from `inputs` and `inputs_regex`
            for k, expr in zip_longest(cls.inputs, cls.inputs_regex):
                if expr is None:
                    break
                # Filter the data given by `expr` from all::iamc
                c.add(k, "filter_ts", all_data, copy(expr))
        else:
            for k in map(Key, cls.inputs):
                # Add a computation to get the time series data for a specific variable
                c.add(k, "get_ts", scenario_key, dict(variable=k.name))

        # Add the plot itself
        return super().add_tasks(c, key, *inputs[1:], strict=strict)

    def ggtitle(self, value=None) -> p9.ggtitle:
        """Return :class:`plotnine.ggtitle` including the current date & time."""
        title_pieces = [
            (self.title or self.__doc__ or "").splitlines()[0].rstrip("."),
            f"[{self.unit}]" if self.unit else None,
            value,
            "\n",
            self.url,
            f"({datetime.now().isoformat(timespec='minutes')})",
        ]
        return p9.ggtitle(" ".join(filter(None, title_pieces)))

    def groupby_plot(self, data: pd.DataFrame, *args):
        """Combination of groupby and ggplot().

        Groups by `args` and yields a series of :class:`plotnine.ggplot` objects, one
        per group, with :attr:`static` geoms and :func:`ggtitle` appended to each.
        """
        for group_key, group_df in data.groupby(*args):
            yield (
                group_key,
                (
                    p9.ggplot(group_df)
                    + self.static
                    + self.ggtitle(
                        group_key if isinstance(group_key, str) else repr(group_key)
                    )
                ),
            )


class EmissionsCO2(Plot):
    """CO₂ Emissions."""

    basename = "emission-CO2"
    inputs = ["Emissions|CO2::iamc", "scenario"]

    static = Plot.static + [
        p9.aes(x="year", y="value", color="region"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="Region"),
    ]

    def generate(self, data: pd.DataFrame, scenario: "Scenario"):
        self.url = scenario.url
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, data.region.str.contains("GLB")):
            y_max = max(ggplot.data["value"])
            yield ggplot + p9.expand_limits(y=[0, y_max]) + self.ggtitle("")


class FinalEnergy0(EmissionsCO2):
    """Final Energy."""

    basename = "fe0"
    inputs = ["Final Energy::iamc", "scenario"]


class FinalEnergy1(Plot):
    """Final Energy."""

    basename = "fe1"
    inputs = ["fe1-0::iamc", "scenario"]

    _c = [
        "Electricity",
        "Gases",
        "Geothermal",
        "Heat",
        "Hydrogen",
        "Liquids",
        "Solar",
        "Solids",
    ]
    inputs_regex = [re.compile(rf"Final Energy\|({'|'.join(_c)})")]

    static = Plot.static + [
        p9.aes(x="year", y="value", fill="variable"),
        p9.geom_bar(stat="identity", size=5.0),  # 5.0 is the minimum spacing of "year"
        p9.labs(x="Period", y="", fill="Commodity"),
    ]

    def generate(self, data: pd.DataFrame, scenario: "Scenario"):
        self.url = scenario.url
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "region"):
            yield ggplot


class PrimaryEnergy0(EmissionsCO2):
    """Primary Energy."""

    basename = "pe0"
    inputs = ["Primary Energy::iamc", "scenario"]


class PrimaryEnergy1(FinalEnergy1):
    """Primary Energy."""

    basename = "pe1"
    inputs = ["pe1-0::iamc", "scenario"]

    _omit = ["Fossil", "Non-Biomass Renewables", "Secondary Energy Trade"]
    inputs_regex = [re.compile(rf"Primary Energy\|((?!{'|'.join(_omit)})[^\|]*)")]


#: All plot classes.
PLOTS = (
    EmissionsCO2,
    FinalEnergy0,
    FinalEnergy1,
    PrimaryEnergy0,
    PrimaryEnergy1,
)


def callback(c: Computer, context: "Context") -> None:
    """Add all :data:`PLOTS` to `c`.

    Also add a key "plot all" to triggers the generation of all plots.
    """
    all_keys = [c.add(f"plot {p.basename}", p, "scenario") for p in PLOTS]
    c.add("plot all", all_keys)
    log.info(f"Add 'plot all' collecting {len(all_keys)} plots")
