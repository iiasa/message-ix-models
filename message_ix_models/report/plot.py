"""Plots for MESSAGEix-GLOBIOM reporting.

The current set functions on time series data stored on the scenario by :mod:`.report`
or legacy reporting.
"""
import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Sequence

import genno.compat.plotnine
import pandas as pd
import plotnine as p9
from genno import Computer, Key

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context

log = logging.getLogger(__name__)


class Plot(genno.compat.plotnine.Plot):
    """Base class for plots."""

    #: 'Static' geoms: list of plotnine objects that are not dynamic.
    static = [p9.theme(figure_size=(11.7, 8.3))]

    #: Fixed plot title string. If not given, the first line of the class docstring is
    #: used.
    title = None

    #: Units expression for plot title.
    unit = None

    #: Scenario URL for plot title.
    url: Optional[str] = None

    # NB only here to narrow typing
    inputs: Sequence[str] = []
    #: List of regular expressions corresponding to :attr:`inputs`.
    inputs_regex: List[re.Pattern] = []

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
            yield group_key, (
                p9.ggplot(group_df)
                + self.static
                + self.ggtitle(
                    group_key if isinstance(group_key, str) else repr(group_key)
                )
            )


class EmissionsCO2(Plot):
    """CO₂ Emissions."""

    basename = "emission-CO2"
    inputs = ["Emissions|CO2::iamc", "scenario"]

    static = Plot.static + [
        p9.aes(x="year", y="value", color="region"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y=None, color="Region"),
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
        p9.labs(x="Period", y=None, fill="Commodity"),
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


PLOTS = (
    EmissionsCO2,
    FinalEnergy0,
    FinalEnergy1,
    PrimaryEnergy0,
    PrimaryEnergy1,
)


def callback(c: Computer, context: "Context") -> None:
    from copy import copy
    from itertools import zip_longest

    all_keys = []

    # Retrieve all time series data, for advanced filtering
    c.add("all::iamc", "get_ts", "scenario")

    for p in PLOTS:
        # TODO move these into an override of Plot.add_tasks()
        if len(p.inputs_regex):
            # Iterate over matched items from `inputs` and `inputs_regex`
            for key, expr in zip_longest(p.inputs, p.inputs_regex):
                if expr is None:
                    break
                # Filter the data given by `expr` from all::iamc
                c.add(key, "filter_ts", "all::iamc", copy(expr))
        else:
            for key in map(Key, p.inputs):
                # Add a computation to get the time series data for a specific variable
                c.add(key, "get_ts", "scenario", dict(variable=key.name))

        # Add the plot itself
        all_keys.append(c.add(f"plot {p.basename}", p))

    c.add("plot all", all_keys)
