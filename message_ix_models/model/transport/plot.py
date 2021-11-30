"""Plots for MESSAGEix-Transport reporting."""
import logging
from datetime import datetime

import pint
import plotnine as p9
from genno.compat.plotnine import Plot as BasePlot

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
                + self.title(f"{self._title_detail} Investment cost [{unit}] {nl}")
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
    basename = "ldv-tech-share"
    inputs = ["out:nl-t-ya:transport"]

    def generate(self, data):
        data = data.rename(columns={0: "out"})

        # # DEBUG dump data
        # data.to_csv(f"{self.basename}.csv")

        return (
            p9.ggplot(data, p9.aes(x="ya", y="out", fill="t"))
            + p9.theme(figure_size=(11.7, 8.3))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}"))
            + p9.geom_bar(stat="identity", width=4)
            + p9.labs(
                x="Period",
                y="Activity [10⁹ km / y]",
                fill="LDV technology",
            )
        )


class LDVTechShare1(Plot):
    basename = "ldv-tech-share-by-cg"
    inputs = ["out:nl-t-ya-c", "consumer groups"]

    def generate(self, data, cg):
        data = data.rename(columns={0: "out"})

        # TODO do these operations in reporting for broader reuse
        # Select a subset of commodities
        data = data[data.c.str.contains("transport pax")]

        # - Remove the consumer group name from the technology name.
        # - Remove the prefix from the commodity name.
        data = data.assign(
            t=data.t.str.split(" usage by ", expand=True)[0],
            c=data.c.str.replace("transport pax ", ""),
        )

        # Discard others, e.g. non-LDV activity
        data = data.query(f"c in {list(map(str, cg))}")

        # DEBUG dump data
        data.to_csv(f"{self.basename}-1.csv")
        log.info(f"Dumped data to {self.basename}.csv")

        # Select a subset of technologies
        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(group_df, p9.aes(x="ya", y="out", fill="t"))
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
    inputs = ["demand:n-c-y", "c:transport"]

    def generate(self, data, commodities):
        # Convert and select data
        data = data.rename(columns={0: "value"}).query(
            f"c in {repr(list(map(str, commodities)))}"
        )

        for node, node_data in data.groupby("n"):
            yield (
                p9.ggplot(node_data, p9.aes(x="y", y="value", fill="c"))
                + p9.geom_bar(stat="identity", width=4)
                + p9.labs(
                    x="Period",
                    y=r"‘demand’ parameter [km / pass / a]",
                    fill="Transport mode group",
                )
                + self.title(f"Investment cost {node}")
                + self.static
            )


class DemandCalibratedCap(Plot):
    basename = "demand-capita"
    inputs = ["demand:n-c-y:capita", "c:transport"]

    def generate(self, data, commodities):
        # TODO handle this by adding additional computations in report.callback() or
        #      functions it calls.
        # try:
        #     # Convert and select data
        #     data = computations.ratio(demand, population)
        # except TypeError:
        #     log.error(f"Missing data to plot {self.basename}")
        #     return []

        # TODO remove; this is now handled by genno…Plot
        # df = (
        #     data.to_series()
        #     .rename("value")
        #     .sort_index()
        #     .reset_index()
        #     .astype(dict(value=float))
        #     .query(f"c in {repr(list(map(str, commodities)))}")
        # )

        for node, node_data in data.groupby("n"):
            yield (
                p9.ggplot(node_data, p9.aes(x="y", y="value", fill="c"))
                + p9.theme(figure_size=(11.7, 8.3))
                + p9.geom_bar(stat="identity", width=4)
                + self.title(f"Node: {node}")
                + p9.labs(
                    x="Period",
                    y=r"‘demand’ parameter [km / pass / a]",
                    fill="Transport mode group",
                )
            )


class DemandExo(Plot):
    basename = "demand-exo"
    inputs = ["transport pdt:n-y-t"]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.rename(columns={0: "value"}).astype(dict(value=float))
        y_max = max(data["value"])
        unit = pint.Quantity(1, data["unit"].unique()[0]).to_reduced_units().units

        scale = 1e6

        for n, group_df in data.groupby("n"):
            yield (
                p9.ggplot(p9.aes(x="y", y=f"value / {scale}", fill="t"), group_df)
                + p9.geom_bar(stat="identity", width=4)
                + p9.expand_limits(y=[0, y_max / scale])
                + p9.labs(x="Period", fill="Mode (tech group)")
                + self.title(f"Passenger transport activity [10⁶ {unit:~}] {n}")
                + self.static
            )


class DemandExoCap(Plot):
    basename = "demand-exo-capita"
    inputs = ["transport pdt:n-y-t:capita"]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.rename(columns={0: "value"}).astype(dict(value=float))
        y_max = max(data["value"])
        unit = pint.Quantity(1, data["unit"].unique()[0]).to_reduced_units().units

        scale = 1e3

        for n, group_df in data.groupby("n"):
            yield (
                p9.ggplot(p9.aes(x="y", y=f"value / {scale}", fill="t"), group_df)
                + p9.geom_bar(stat="identity", width=4)
                + p9.expand_limits(y=[0, y_max / scale])
                + p9.labs(x="Period", fill="Mode (tech group)")
                + self.title(
                    f"Passenger transport activity per person [10³ {unit:~}] {n}"
                )
                + self.static
            )


class EnergyCmdty(Plot):
    basename = "energy-by-cmdty"
    inputs = ["in:nl-t-ya-c"]

    def generate(self, data):
        # Discard data for certain technologies
        data = data[
            ~(
                data.t.str.startswith("transport vehicle.*")
                | data.t.str.startswith("disutility")
            )
        ].rename(columns={0: "in"})

        return (
            p9.ggplot(data, p9.aes(x="ya", y="in", fill="c"))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("Node: {}"))
            + p9.geom_bar(stat="identity", width=5, color="black")
            + p9.labs(x="Period", y="Energy", fill="Commodity")
            + self.title("Energy input to transport")
            + self.static
        )


# class EmissionsTech(Plot):
#     basename = "emissions-by-tech"
#     inputs = ["emi:"]


class Stock0(Plot):
    basename = "stock-ldv"
    inputs = ["ldv stock:nl-t-ya-driver_type"]
    _title_detail = "LDV transport"

    def generate(self, data):
        data = data.rename(columns={0: "ldv stock"})
        y_max = max(data["ldv stock"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="yv", y="ldv stock", color="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"{self._title_detail} Vehicle stock [{unit}] {nl}")
                + self.static
            )


class Stock1(Plot):
    """Same as Stock0, but for non-LDV techs only."""

    basename = "stock-non-ldv"
    inputs = ["non ldv stock:nl-t-ya"]
    _title_detail = "Non-LDV transport"

    def generate(self, data):
        data = data.rename(columns={0: "ldv stock"})
        y_max = max(data["ldv stock"])
        unit = data["unit"].unique()[0]

        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="yv", y="ldv stock", color="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"{self._title_detail} Vehicle stock [{unit}] {nl}")
                + self.static
            )


#: Plots of data from the built (and maybe solved) MESSAGEix-Transport scenario.
PLOTS = [
    FixCost,
    InvCost0,
    InvCost1,
    InvCost2,
    VarCost,
    EnergyCmdty,
    LDV_IO,
    LDVTechShare0,
    LDVTechShare1,
    Stock0,
    Stock1,
    DemandCalibrated,
    DemandCalibratedCap,
]

#: Plots of data from the exogenous demand calculation.
DEMAND_PLOTS = [
    DemandExo,
    DemandExoCap,
]
