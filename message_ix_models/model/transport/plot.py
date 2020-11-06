import logging

import plotnine as p9

from message_data.reporting.plot import Plot

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
        techs = list(
            filter(lambda n: "usage" in n, data.coords["t"].values)
        )

        # Select a subset of technologies
        for node in data.coords["nl"].values:
            df = (
                data.sel(t=techs, nl=node)
                .to_series().rename("value").reset_index()
            )

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


class ModeShare2(Plot):
    name = "demand-by-mode"
    inputs = ["transport pdt:n-y-t", "config"]

    def generate(self, data, config):
        # TODO select a subset of technologies
        df = (
            data.to_series().rename("value").sort_index().reset_index()
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
            data.to_series().rename("value").sort_index().reset_index()
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
    EnergyCmdty,
    LDVTechShare0,
    LDVTechShare1,
    ModeShare2,
]
