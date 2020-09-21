import logging

import matplotlib
import plotnine as p9


log = logging.getLogger(__name__)

try:
    matplotlib.use("cairo")
except ImportError:
    log.info(
        f"'cairo' not available; using {matplotlib.get_backend()} matplotlib "
        "backend"
    )


class Plot:
    save_args = dict(verbose=False)
    inputs = []
    name = ""

    # TODO add static geoms
    __static = []

    def __call__(self, config, *args):
        path = config["output dir"] / f"{self.name}.pdf"
        plot_or_plots = self.generate(*args)

        try:
            # Single plot
            plot_or_plots.save(path, **self.save_args)
        except AttributeError:
            # Iterator containing multiple plots
            p9.save_as_pdf_pages(plot_or_plots, path, **self.save_args)

        return path

    def generate(*args):
        return p9.ggplot(*args)


class LabelFirst:
    __name__ = None

    def __init__(self, fmt_string):
        self.fmt_string = fmt_string
        self.first = True

    def __call__(self, value):
        first = self.first
        self.first = False
        return self.fmt_string.format(value) if first else value


class ModeShare0(Plot):
    name = "mode-share"
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


class ModeShare1(Plot):
    name = "mode-share-by-cg"
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
    inputs = ["transport pdt:n-y-t:mode", "config"]

    def generate(self, data, config):
        # Select a subset of technologies
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


PLOTS = [ModeShare0, ModeShare1, ModeShare2]
