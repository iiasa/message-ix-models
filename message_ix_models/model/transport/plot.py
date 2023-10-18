"""Plots for MESSAGEix-Transport reporting."""
import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import plotnine as p9
from genno import Computer
from genno.compat.plotnine import Plot as BasePlot
from iam_units import registry

log = logging.getLogger(__name__)


class LabelFirst:
    """Labeller that labels the first item using a format string.

    Subsequent items are named with the bare value only.
    """

    __name__: Optional[str] = None

    def __init__(self, fmt_string):
        self.fmt_string = fmt_string
        self.first = True

    def __call__(self, value):
        first = self.first
        self.first = False
        return self.fmt_string.format(value) if first else value


class Plot(BasePlot):
    # Output goes in the "transport" subdirectory
    path = ["transport"]

    #: 'Static' geoms: list of plotnine objects that are not dynamic
    static = [p9.theme(figure_size=(11.7, 8.3))]

    #: Fixed plot title string. If not given, the first line of the class docstring is
    #: used.
    title: Optional[str] = None

    #: Units expression for plot title.
    unit: Optional[str] = None

    def ggtitle(self, value=None):
        """Return :class:`plotnine.ggtitle` including the current date & time."""
        title_pieces = [
            (self.title or self.__doc__).splitlines()[0].rstrip("."),
            f"[{self.unit}]" if self.unit else None,
            value,
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
                p9.ggplot(group_df) + self.static + self.ggtitle(repr(group_key))
            )


class InvCost0(Plot):
    """All transport investment cost."""

    basename = "inv-cost-transport"
    inputs = ["inv_cost:nl-t-yv:transport"]
    static = Plot.static + [
        p9.aes(x="yv", y="inv_cost", color="t"),
        p9.geom_line(),
        p9.geom_point(),
    ]

    def generate(self, data):
        y_max = max(data["inv_cost"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class InvCost1(InvCost0):
    """LDV transport investment cost.

    Same as InvCost0, but for LDV techs only.
    """

    basename = "inv-cost-ldv"
    inputs = ["inv_cost:nl-t-yv:ldv"]


class InvCost2(InvCost0):
    """Non-LDV transport investment cost.

    Same as InvCost0, but for non-LDV techs only.
    """

    basename = "inv-cost-nonldv"
    inputs = ["inv_cost:nl-t-yv:nonldv"]


class FixCost(Plot):
    """Fixed cost."""

    basename = "fix-cost"
    inputs = ["fix_cost:nl-t-yv-ya:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="fix_cost", color="t", group="t"),
        p9.geom_line(),
        p9.geom_point(),
    ]

    def generate(self, data):
        y_max = max(data["fix_cost"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class VarCost(Plot):
    """Variable cost."""

    basename = "var-cost"
    inputs = ["var_cost:nl-t-yv-ya:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="var_cost", color="t", group="yv"),
        p9.geom_line(),
        p9.geom_point(),
    ]

    def generate(self, data):
        y_max = max(data["var_cost"])
        self.unit = data["unit"].unique()[0]

        for nl, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class LDV_IO(Plot):
    """Input efficiency [GWa / km]."""

    basename = "ldv-efficiency"
    inputs = ["input:nl-t-yv-ya:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="input", color="t"),
        p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}")),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="LDV technology"),
    ]

    def generate(self, data):
        return p9.ggplot(data) + self.static + self.ggtitle()


class LDVTechShare0(Plot):
    """Activity [10⁹ km / y]."""

    basename = "ldv-tech-share"
    inputs = ["out:nl-t-ya:transport"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="t"),
        p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}")),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y=None, fill="LDV technology"),
    ]

    def generate(self, data):
        return p9.ggplot(data) + self.static + self.ggtitle()


class LDVTechShare1(Plot):
    """Usage of LDV technologies by CG."""

    basename = "ldv-tech-share-by-cg"
    inputs = ["out:nl-t-ya-c", "consumer groups"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="t"),
        p9.facet_wrap(["c"], ncol=5),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="Activity [10⁹ km / y]", fill="LDV technology"),
    ]

    def generate(self, data, cg):
        # TODO do these operations in reporting for broader reuse
        # - Select a subset of commodities
        # - Remove the consumer group name from the technology name.
        # - Remove the prefix from the commodity name.
        # - Discard others, e.g. non-LDV activity

        data = (
            data[data.c.str.contains("transport pax")]
            .assign(
                t=lambda df: df.t.str.split(" usage by ", expand=True)[0],
                c=lambda df: df.c.str.replace("transport pax ", ""),
            )
            .query(f"c in {list(map(str, cg))}")
        )

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot


def c_group(df: pd.DataFrame, cg):
    return df.assign(
        c_group=df.c.apply(
            lambda v: "transport pax LDV" if any(cg_.id in v for cg_ in cg) else v
        )
    )


class DemandCalibrated(Plot):
    """Transport demand [pass · km / a]."""

    basename = "demand"
    inputs = ["demand:n-c-y", "c::transport", "cg"]
    static = Plot.static + [
        p9.aes(x="y", y="demand", fill="c_group"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y=None, fill="Transport mode group"),
    ]

    def generate(self, data, commodities, cg):
        # Convert and select data
        data = data.query(f"c in {repr(list(map(str, commodities)))}").pipe(c_group, cg)
        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot


class DemandCalibratedCap(Plot):
    """Transport demand per capita [km / a]."""

    basename = "demand-capita"
    inputs = ["demand:n-c-y:capita", "c::transport", "cg"]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="c"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y=None, fill="Transport mode group"),
    ]

    def generate(self, data, commodities, cg):
        # Convert and select data
        data = data.query(f"c in {repr(list(map(str, commodities)))}").pipe(c_group, cg)
        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot


def _reduce_units(df: pd.DataFrame, target_units) -> Tuple[pd.DataFrame, str]:
    df_units = df["unit"].unique()
    assert 1 == len(df_units)
    tmp = registry.Quantity(1.0, df_units[0]).to(target_units)
    return (
        df.eval("value = value * @tmp.magnitude").assign(unit=f"{tmp.units:~}"),
        f"{tmp.units:~}",
    )


class DemandExo(Plot):
    """Passenger transport activity."""

    basename = "demand-exo"
    inputs = ["pdt:n-y-t"]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="t"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y=None, fill="Mode (tech group)"),
    ]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.astype(dict(value=float))
        data, self.unit = _reduce_units(data, "Gp km / a")
        y_max = max(data["value"])

        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class DemandExoCap(Plot):
    """Passenger transport activity per person."""

    basename = "demand-exo-capita"
    inputs = ["transport pdt:n-y-t:capita"]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="t"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y=None, fill="Mode (tech group)"),
    ]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.astype(dict(value=float))
        data, self.unit = _reduce_units(data, "Mm / a")
        y_max = max(data["value"])

        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class EnergyCmdty(Plot):
    """Energy input to transport [GWa]."""

    basename = "energy-by-cmdty"
    inputs = ["in:nl-ya-c:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="c"),
        p9.geom_bar(stat="identity", width=5, color="black"),
        p9.labs(x="Period", y="Energy", fill="Commodity"),
    ]

    def generate(self, data):
        # Discard data for certain commodities
        data = data[~(data.c.str.startswith("transport") | (data.c == "disutility"))]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot


class Stock0(Plot):
    """LDV transport vehicle stock."""

    basename = "stock-ldv"
    # Partial sum over driver_type dimension
    inputs = ["stock:nl-t-ya:ldv"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", color="t"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", color="Powertrain technology"),
    ]

    def generate(self, data):
        y_max = max(data["value"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class Stock1(Plot):
    """Non-LDV transport vehicle stock.

    Same as Stock0, but for non-LDV techs only.
    """

    basename = "stock-non-ldv"
    inputs = ["stock:nl-t-ya:non-ldv"]
    static = Plot.static + [
        p9.aes(x="yv", y="value", color="t"),
        p9.geom_line(),
        p9.geom_point(),
    ]

    def generate(self, data):
        if not len(data):
            return

        y_max = max(data["value"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


#: Plots of data from the built (and maybe solved) MESSAGEix-Transport scenario.
PLOTS = {}

# Inspect the defined plots to populate the dict
_ = obj = None
for _, obj in globals().items():
    if isinstance(obj, type) and issubclass(obj, Plot) and obj is not Plot:
        PLOTS[obj.basename] = obj


def prepare_computer(c: Computer):
    keys = []
    queue = []

    # Plots
    for name, cls in PLOTS.items():
        # Skip all but post-solve demand plots
        keys.append(f"plot {name}")
        queue.append((keys[-1], cls))

    c.add_queue(queue)

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(key)} plots")
    c.add(key, keys)
