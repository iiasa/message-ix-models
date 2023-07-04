"""Plots for MESSAGEix-Transport reporting."""
import logging
from datetime import datetime
from typing import Tuple

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

    __name__ = None

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

    static = [p9.theme(figure_size=(11.7, 8.3))]

    def title(self, value):
        """Return :class:`plotnine.ggtitle` including the current date & time."""
        return p9.ggtitle(f"{value} ({datetime.now().isoformat(timespec='minutes')})")


class InvCost0(Plot):
    basename = "inv-cost-transport"
    inputs = ["inv_cost:nl-t-yv:transport"]

    _title_detail = "All transport"

    def generate(self, data):
        data = data.rename(columns={0: "inv_cost"})
        y_max = max(data["inv_cost"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="yv", y="inv_cost", color="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"{self._title_detail} investment cost [{unit}] {nl}")
                + self.static
            )


class InvCost1(InvCost0):
    """Same as InvCost0, but for LDV techs only."""

    basename = "inv-cost-ldv"
    inputs = ["inv_cost:nl-t-yv:ldv"]
    _title_detail = "LDV transport"


class InvCost2(InvCost0):
    """Same as InvCost0, but for non-LDV techs only."""

    basename = "inv-cost-nonldv"
    inputs = ["inv_cost:nl-t-yv:nonldv"]
    _title_detail = "Non-LDV transport"


class FixCost(Plot):
    basename = "fix-cost"
    inputs = ["fix_cost:nl-t-yv-ya"]

    def generate(self, data):
        data = data.rename(columns={0: "fix_cost"})
        y_max = max(data["fix_cost"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="ya", y="fix_cost", color="t", group="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"Fixed cost [{unit}] {nl}")
                + self.static
            )


class VarCost(Plot):
    basename = "var-cost"
    inputs = ["var_cost:nl-t-yv-ya"]

    def generate(self, data):
        data = data.rename(columns={0: "var_cost"})
        y_max = max(data["var_cost"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="ya", y="var_cost", color="t", group="yv"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"Variable cost [{unit}] {nl}")
                + self.static
            )


class LDV_IO(Plot):
    basename = "ldv-efficiency"
    inputs = ["input:nl-t-yv-ya"]

    def generate(self, data):
        data = data.rename(columns={0: "input"})

        return (
            p9.ggplot(data, p9.aes(x="ya", y="input", color="t"))
            + p9.theme(figure_size=(11.7, 8.3))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}"))
            + p9.geom_line()
            + p9.geom_point()
            + p9.labs(
                x="Period",
                y="Input efficiency [GWa / km]",
                color="LDV technology",
            )
            + self.static
        )


class LDVTechShare0(Plot):
    """Only works with filter = True."""

    basename = "ldv-tech-share"
    inputs = ["out:nl-t-ya:transport"]

    def generate(self, data):
        data = data.rename(columns={0: "out"})

        # # DEBUG dump data
        # data.to_csv(f"{self.basename}.csv")

        return (
            p9.ggplot(data, p9.aes(x="ya", y="out", fill="t"))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}"))
            + p9.geom_bar(stat="identity", width=4)
            + p9.labs(
                x="Period",
                y="Activity [10⁹ km / y]",
                fill="LDV technology",
            )
            + self.static
        )


class LDVTechShare1(Plot):
    basename = "ldv-tech-share-by-cg"
    inputs = ["out:nl-t-ya-c", "consumer groups"]

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

        # DEBUG dump data
        data.to_csv(f"{self.basename}-1.csv")
        log.info(f"Dumped data to {self.basename}.csv")

        # Select a subset of technologies
        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(group_df, p9.aes(x="ya", y="value", fill="t"))
                + p9.facet_wrap(["c"], ncol=5)
                + p9.geom_bar(stat="identity", width=4)
                + p9.labs(
                    x="Period",
                    y="Activity [10⁹ km / y]",
                    fill="LDV technology",
                )
                + self.title(f"Usage of LDV technologies per CG, {nl}")
                + self.static
            )


class DemandCalibrated(Plot):
    basename = "demand"
    inputs = ["demand:n-c-y", "c:transport", "cg"]

    def generate(self, data, commodities, cg):
        # Convert and select data
        data = data.rename(columns={0: "value"}).query(
            f"c in {repr(list(map(str, commodities)))}"
        )
        data["c group"] = data["c"].apply(
            lambda v: "transport pax LDV" if any(cg_.id in v for cg_ in cg) else v
        )

        for node, node_data in data.groupby("n"):
            yield (
                p9.ggplot(node_data, p9.aes(x="y", y="value", fill="c group"))
                + p9.geom_bar(stat="identity", width=4)
                + p9.labs(x="Period", y=None, fill="Transport mode group")
                + self.title(f"Transport demand [pass · km / a] {node}")
                + self.static
            )


class DemandCalibratedCap(Plot):
    basename = "demand-capita"
    inputs = ["demand:n-c-y:capita", "c:transport", "cg"]

    def generate(self, data, commodities, cg):
        # Convert and select data
        print(data)
        data = data.rename(columns={0: "value"}).query(
            f"c in {repr(list(map(str, commodities)))}"
        )
        data["c group"] = data["c"].apply(
            lambda v: "transport pax LDV" if any(cg_.id in v for cg_ in cg) else v
        )

        for node, group_df in data.groupby("n"):
            yield (
                p9.ggplot(p9.aes(x="y", y="value", fill="c"), group_df)
                + p9.geom_bar(stat="identity", width=4)
                + p9.labs(x="Period", y=None, fill="Transport mode group")
                + self.title(f"Transport demand per capita [km / a] {node}")
                + self.static
            )


def _reduce_units(df: pd.DataFrame, target_units) -> Tuple[pd.DataFrame, str]:
    df_units = df["unit"].unique()
    assert 1 == len(df_units)
    tmp = registry.Quantity(1.0, df_units[0]).to(target_units)
    return (
        df.eval("value = value * @tmp.magnitude").assign(unit=f"{tmp.units:~}"),
        f"{tmp.units:~}",
    )


class DemandExo(Plot):
    basename = "demand-exo"
    inputs = ["pdt:n-y-t"]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.rename(columns={0: "value"}).astype(dict(value=float))
        data, unit = _reduce_units(data, "Gp km / a")
        y_max = max(data["value"])

        for n, group_df in data.groupby("n"):
            yield (
                p9.ggplot(p9.aes(x="y", y="value", fill="t"), group_df)
                + p9.geom_bar(stat="identity", width=4)
                + p9.expand_limits(y=[0, y_max])
                + p9.labs(x="Period", y=None, fill="Mode (tech group)")
                + self.title(f"Passenger transport activity [{unit}] {n}")
                + self.static
            )


class DemandExoCap(Plot):
    basename = "demand-exo-capita"
    inputs = ["transport pdt:n-y-t:capita"]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.rename(columns={0: "value"}).astype(dict(value=float))
        data, unit = _reduce_units(data, "Mm / a")
        y_max = max(data["value"])

        for n, group_df in data.groupby("n"):
            yield (
                p9.ggplot(p9.aes(x="y", y="value", fill="t"), group_df)
                + p9.geom_bar(stat="identity", width=4)
                + p9.expand_limits(y=[0, y_max])
                + p9.labs(x="Period", y=None, fill="Mode (tech group)")
                + self.title(f"Passenger transport activity per person [{unit}] {n}")
                + self.static
            )


class EnergyCmdty(Plot):
    basename = "energy-by-cmdty"
    inputs = ["in:nl-ya-c"]

    def generate(self, data):
        # Discard data for certain commodities
        data = data[~(data.c.str.startswith("transport") | (data.c == "disutility"))]
        unit = "GWa"

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="ya", y="value", fill="c"), group_df)
                + p9.geom_bar(stat="identity", width=5, color="black")
                + p9.labs(x="Period", y="Energy", fill="Commodity")
                + self.title(f"Energy input to transport [{unit}] {nl}")
                + self.static
            )


# class EmissionsTech(Plot):
#     basename = "emissions-by-tech"
#     inputs = ["emi:"]


class Stock0(Plot):
    basename = "stock-ldv"
    # Partial sum over driver_type dimension
    inputs = ["stock:nl-t-ya:ldv"]
    _title_detail = "LDV transport vehicle stock"
    static = Plot.static + [
        p9.aes(x="ya", y="stock", color="t"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", color="Powertrain technology"),
    ]

    def generate(self, data):
        data = data.rename(columns={"value": "stock"})
        y_max = max(data["stock"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(group_df)
                + self.static
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"{self._title_detail} [{unit}] {nl}")
            )


class Stock1(Plot):
    """Same as Stock0, but for non-LDV techs only."""

    basename = "stock-non-ldv"
    inputs = ["stock:nl-t-ya:non-ldv"]
    _title_detail = "Non-LDV transport"

    def generate(self, data):
        data = data.rename(columns={0: "stock"})
        y_max = max(data["stock"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="yv", y="stock", color="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"{self._title_detail} Vehicle stock [{unit}] {nl}")
                + self.static
            )


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
        if "-exo" in name:
            continue
        keys.append(f"plot {name}")
        queue.append((keys[-1], cls.make_task()))

    c.add_queue((item, dict()) for item in queue)

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(key)} plots")
    c.add(key, keys)
