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
    save_args = dict(height=210, width=297, units="mm", verbose=False)
    inputs = []
    name = ""

    def __call__(self, *args):
        filename = f"{self.name}.pdf"
        self.generate(*args).save(filename, **self.save_args)
        return filename

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
            + p9.facet_wrap(["nl"], ncol=2, labeller=LabelFirst("node: {}"))
            + p9.geom_bar(stat="identity", width=4)
            + p9.labs(
                x="Period",
                y="Activity [10⁹ km / y]",
                fill="LDV technology",
            )
        )


class ModeShare1(Plot):
    name = "mode-share-cg-WEU"
    inputs = ["out:nl-t-ya-c:transport"]

    def generate(self, data):
        # Select a subset of technologies
        techs = list(filter(lambda n: "usage" in n, data.coords["t"].values))
        df = (
            data.sel(t=techs, nl="R11_WEU")
            .to_series().rename("value").reset_index()
        )

        return (
            p9.ggplot(df, p9.aes(x="ya", y="value", fill="t"))
            + p9.facet_wrap(
                ["c"], ncol=5, labeller=LabelFirst("commodity: {}")
            )
            + p9.geom_bar(stat="identity", width=4)
            + p9.labs(
                x="Period",
                y="Activity [10⁹ km / y]",
                fill="LDV technology",
            )
        )


PLOTS = [ModeShare0, ModeShare1]
