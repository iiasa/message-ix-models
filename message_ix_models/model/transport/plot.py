import logging
from datetime import datetime

import plotnine as p9
from genno.compat.plotnine import Plot as BasePlot
from ixmp.reporting import computations

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


class Costs0(Plot):
    basename = "inv-cost"
    inputs = ["inv_cost:nl-t-yv"]

    def generate(self, data):
        y_max = max(data["inv_cost"])
        unit = data["unit"].unique()[0]
        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="yv", y="inv_cost", color="t"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"Investment cost [{unit}] {nl}")
                + self.static
            )


class Costs1(Plot):
    basename = "fix-cost"
    inputs = ["fix_cost:nl-t-yv-ya"]

    def generate(self, data):
        y_max = max(data["fix_cost"])
        unit = data["unit"].unique()[0]
        for nl, group_df in data.groupby("nl"):
            yield (
                p9.ggplot(p9.aes(x="ya", y="fix_cost", color="t", group="yv"), group_df)
                + p9.geom_line()
                + p9.geom_point()
                + p9.expand_limits(y=[0, y_max])
                + self.title(f"Fixed cost [{unit}] {nl}")
                + self.static
            )


class Costs2(Plot):
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
    name = "ldv-efficiency"
    inputs = ["input:nl-t-yv-ya"]

    def generate(self, data):
        df = data.to_series().rename("value").reset_index()

        return (
            p9.ggplot(df, p9.aes(x="ya", y="value", color="t"))
            + p9.theme(figure_size=(11.7, 8.3))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}"))
            + p9.geom_line()
            + p9.geom_point()
            + p9.labs(
                x="Period",
                y="Input efficiency [GWa / km]",
                color="LDV technology",
            )
        )


class LDVTechShare0(Plot):
    name = "ldv-tech-share"
    inputs = ["out:nl-t-ya:transport"]

    def generate(self, data):
        # Select a subset of technologies
        techs = list(filter(lambda n: "usage" in n, data.coords["t"].values))
        df = data.sel(t=techs).to_series().rename("value").reset_index()

        return (
            p9.ggplot(df, p9.aes(x="ya", y="value", fill="t"))
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
    name = "ldv-tech-share-by-cg"
    inputs = ["out:nl-t-ya-c:transport"]

    def generate(self, data):
        techs = list(filter(lambda n: "usage" in n, data.coords["t"].values))

        # Select a subset of technologies
        for node in data.coords["nl"].values:
            df = data.sel(t=techs, nl=node).to_series().rename("value").reset_index()

            yield (
                p9.ggplot(df, p9.aes(x="ya", y="value", fill="t"))
                + p9.theme(figure_size=(11.7, 8.3))
                + p9.facet_wrap(["c"], ncol=5)
                + p9.geom_bar(stat="identity", width=4)
                + p9.labs(
                    x="Period",
                    y="Activity [10⁹ km / y]",
                    fill="LDV technology",
                )
                + p9.ggtitle(f"Mode share by CG — {node}")
            )


class DemandCalibrated(Plot):
    name = "par-demand"
    inputs = ["demand:n-c-y", "c:transport"]

    def generate(self, data, commodities):
        # Convert and select data
        df = (
            data.to_series()
            .rename("value")
            .sort_index()
            .reset_index()
            .astype(dict(value=float))
            .query(f"c in {repr(list(map(str, commodities)))}")
        )

        for node, node_data in df.groupby("n"):
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


class DemandCalibratedCap(Plot):
    name = "par-demand-cap"
    inputs = ["demand:n-c-y", "population:n-y", "c:transport"]

    def generate(self, demand, population, commodities):
        # Convert and select data
        data = computations.ratio(demand, population)
        print(data)
        df = (
            data.to_series()
            .rename("value")
            .sort_index()
            .reset_index()
            .astype(dict(value=float))
            .query(f"c in {repr(list(map(str, commodities)))}")
        )

        for node, node_data in df.groupby("n"):
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
    name = "demand-exo"
    inputs = ["transport pdt:n-y-t", "config"]

    def generate(self, data, config):
        # TODO select a subset of technologies
        df = (
            data.to_series()
            .rename("value")
            .sort_index()
            .reset_index()
            .astype(dict(value=float))
        )

        return (
            p9.ggplot(df, p9.aes(x="y", y="value", fill="t"))
            + p9.theme(figure_size=(11.7, 8.3))
            + p9.facet_wrap(["n"], ncol=2, labeller=LabelFirst("Node: {}"))
            + p9.geom_bar(stat="identity", width=4)
            + p9.labs(
                x="Period",
                y="Activity [km / pass / a]",
                fill="Transport mode group",
            )
        )


class EnergyCmdty(Plot):
    name = "energy-by-cmdty"
    inputs = ["in:nl-t-ya-c"]

    def generate(self, data):
        df = (
            data.to_series()
            .rename("value")
            .sort_index()
            .reset_index()
            .astype(dict(value=float))
        )

        df = df[~df["t"].str.startswith("transport vehicle.*")]
        df = df[~df["t"].str.startswith("disutility")]
        print(df)

        return (
            p9.ggplot(df, p9.aes(x="ya", y="value", fill="c"))
            + p9.theme(figure_size=(11.7, 8.3))
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("Node: {}"))
            + p9.geom_bar(stat="identity", width=5, color="black")
            + p9.labs(x="Period", y="Energy", fill="Commodity")
        )


# class EmissionsTech(Plot):
#     name = "emissions-by-tech"
#     inputs = ["emi:"]


PLOTS = [
    Costs0,
    Costs1,
    Costs2,
    EnergyCmdty,
    LDVTechShare0,
    LDVTechShare1,
    DemandCalibrated,
    DemandCalibratedCap,
    DemandExo,
]
