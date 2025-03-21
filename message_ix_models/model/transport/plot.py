"""Plots for MESSAGEix-Transport reporting."""

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import genno.compat.plotnine
import pandas as pd
import plotnine as p9
from genno import Computer
from iam_units import registry

from .key import gdp_cap, pdt_nyt
from .key import report as k_report

if TYPE_CHECKING:
    from genno.core.key import KeyLike

    # NB The following is itself within an if TYPE_CHECKING block; mypy can't find it
    from plotnine.typing import PlotAddable  # type: ignore [attr-defined]

    from .config import Config

log = logging.getLogger(__name__)

# Quiet messages like:
#   "Fontsize 0.00 < 1.0 pt not allowed by FreeType. Setting fontsize= 1 pt"
# TODO Investigate or move upstream
logging.getLogger("matplotlib.font_manager").setLevel(logging.INFO + 1)


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


class Plot(genno.compat.plotnine.Plot):
    """Base class for plots.

    This class extends :class:`genno.compat.plotnine.Plot` with extra features.
    """

    #: 'Static' geoms: list of plotnine objects that are not dynamic
    static: list["PlotAddable"] = [
        p9.theme(figure_size=(11.7, 8.3)),
    ]

    #: Fixed plot title string. If not given, the first line of the class docstring is
    #: used.
    title: Optional[str] = None

    #: Units expression for plot title.
    unit: Optional[str] = None

    #: :obj:`False` for plots not intended to be run on a solved scenario.
    runs_on_solved_scenario: bool = True

    def ggtitle(self, extra: Optional[str] = None):
        """Return :class:`plotnine.ggtitle` including the current date & time."""
        title_parts = [
            (self.title or self.__doc__ or "").splitlines()[0].rstrip("."),
            f"[{self.unit}]" if self.unit else None,
            f"— {extra}" if extra else None,
        ]
        subtitle_parts = [
            getattr(self.scenario, "url", "no Scenario"),
            "—",
            datetime.now().isoformat(timespec="minutes"),
        ]
        return p9.labs(
            title=" ".join(filter(None, title_parts)), subtitle=" ".join(subtitle_parts)
        )

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
                    + self.ggtitle(f"{'-'.join(args)}={group_key!r}")
                ),
            )

    def save(self, config, *args, **kwargs) -> Optional[Path]:
        # Strip off the last of `args`, a pre-computed path, and store
        *_args, self.path, self.scenario = args
        # Call the parent method with the remaining arguments
        return super().save(config, *_args, **kwargs)

    @classmethod
    def add_tasks(
        cls, c: Computer, key: "KeyLike", *inputs, strict: bool = False
    ) -> "KeyLike":
        """Use a custom output path."""
        from operator import itemgetter

        from genno import quote

        # Output path for this parameter
        k_path = f"plot {cls.basename} path"
        filename = f"{cls.basename}{cls.suffix}"
        if cls.runs_on_solved_scenario:
            # Make a path including the Scenario URL
            c.add(k_path, "make_output_path", "config", name=filename)
        else:
            # Build phase: no Scenario/URL exists; use a path set by add_debug()
            c.add(f"{k_path} 0", itemgetter("transport build debug dir"), "config")
            c.add(k_path, Path.joinpath, f"{k_path} 0", quote(filename))

        # Same as the parent method
        _inputs = list(inputs if inputs else cls.inputs)

        # Append the key for `path` to the inputs
        return super(Plot, cls).add_tasks(
            c, key, *_inputs, k_path, "scenario", strict=strict
        )


class BaseEnergy0(Plot):
    """Transport final energy intensity of GDP."""

    basename = "base-fe-intensity-gdp"
    inputs = ["fe intensity:nl-ya:units"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", color="nl"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="Node"),
        p9.expand_limits(y=[0, 0.1]),
    ]

    def generate(self, data):
        self.unit = data["unit"].unique()[0]
        return p9.ggplot(data) + self.static + self.ggtitle()


class CapNewLDV(Plot):
    # FIXME remove hard-coded units
    """New LDV capacity [10⁶ vehicle]."""

    basename = "cap-new-t-ldv"
    inputs = ["historical_new_capacity:nl-t-yv:ldv", "CAP_NEW:nl-t-yv:ldv"]
    static = Plot.static + [
        p9.aes(x="yv", y="value", color="t"),
        p9.geom_vline(xintercept=2020, size=4, color="white"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="LDV technology"),
    ]

    def generate(self, data0, data1):
        # - Concatenate data0 (values in "historical_new_capacity" column) and
        #   data1 (values in "CAP_NEW" column).
        # - Fill with zeros.
        # - Compute a "value" column: one or the other.
        # - Remove some errant values for R12_GLB.
        #   FIXME Investigate and remove the source
        data = (
            pd.concat([data0, data1])
            .fillna(0)
            .eval("value = CAP_NEW + historical_new_capacity")
            .query("nl != 'R12_GLB'")
        )

        yield from [ggplot for _, ggplot in self.groupby_plot(data, "nl")]


def read_csvs(stem: str, *paths: Path, **kwargs) -> pd.DataFrame:
    """Read and concatenate data for debugging plots.

    - Read data from files named :file:`{stem}.csv` in each of `paths`.
    - Store with shortened scenario labels extracted from the `paths`.
    - Concatenate to a single data frame with a "scenario" column.
    """

    def label_from(dirname: Path) -> str:
        """Extract e.g. "SSP(2024).2-R12-B" from "debug-ICONICS_SSP(2024).2-R12-B"."""
        return dirname.parts[-1].split("ICONICS_", maxsplit=1)[-1]

    kwargs.setdefault("comment", "#")
    return pd.concat(
        {
            label_from(p): pd.read_csv(p.joinpath(f"{stem}.csv"), **kwargs)
            for p in paths
        },
        names=["scenario"],
    ).reset_index("scenario")


class ComparePDT(Plot):
    """Passenger activity.

    This plot is used in :func:`.transport.build.debug_multi`, not in ordinary
    reporting. Rather than receiving data from computed quantities already in the graph,
    it reads them from files named :file:`pdt.csv` (per :attr:`.kind`) in the
    directories generated by the workflow steps like "SSP1 debug build" (
    :func:`.transport.build.debug`).

    - One page per |n|.
    - 5 horizontal panels for |t| (=transport modes).
    - One line with points per scenario, coloured by scenario.
    """

    runs_on_solved_scenario = False
    basename = "compare-pdt"

    static = Plot.static + [
        p9.aes(x="y", y="value", color="scenario"),
        p9.facet_wrap("t", ncol=5),
        p9.geom_line(),
        p9.geom_point(size=0.5),
        p9.scale_y_log10(),
        p9.labs(y="Activity"),
    ]

    #: Base name for source data files, for instance :file:`pdt.csv`.
    kind = "pdt"

    #: Units of input files
    unit = "km/a"
    #: Unit adjustment factor.
    factor = 1e6

    def generate(self, *paths: Path):
        data = read_csvs(self.kind, *paths).eval("value = value / @self.factor")

        # Add factor to the unit expression
        if self.factor != 1.0:
            self.unit = f"{self.factor:.0e} {self.unit}"

        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot + p9.expand_limits(y=[5e-2, max(data["value"])])


class ComparePDTCap0(ComparePDT):
    """Passenger activity per capita.

    Identical to :class:`.ComparePDT`, but reads from :file:`pdt-cap.csv` instead.
    """

    basename = "compare-pdt-cap"
    kind = "pdt-cap"
    factor = 1e3


#: Common layers for :class:`ComparePDTCap1` and :class:`DemandExoCap1`.
PDT_CAP_GDP_STATIC = Plot.static + [
    p9.aes(x="gdp", y="value", color="t"),
    p9.geom_line(),
    p9.geom_point(),
    p9.scale_x_log10(),
    p9.scale_y_log10(),
    p9.labs(x="GDP [10³ USD_2017 / capita]", y="", color="Transport mode"),
]


class ComparePDTCap1(Plot):
    """Passenger activity.

    Similar to :class:`DemandExoCap1`, except comparing multiple scenarios.

    - One page per |n|.
    - 5 horizontal panels for scenarios.
    - One line with points per |t| (=transport mode), coloured by mode.
    """

    runs_on_solved_scenario = False
    basename = "compare-pdt-capita-gdp"
    static = PDT_CAP_GDP_STATIC + [
        p9.facet_wrap("scenario", ncol=5),
    ]
    unit = "km/a"

    def generate(self, *paths: Path):
        # Read data
        df_pdt = read_csvs("pdt-cap", *paths)
        df_gdp = read_csvs("gdp-ppp-cap", *paths)

        # Merge data from two quantities; keep separate column names
        # NB Same as DemandExoCap1, except on=["scenario", …]
        data = df_pdt.merge(
            df_gdp.rename(columns={"value": "gdp", "unit": "gdp_unit"}),
            on=["scenario", "n", "y"],
        )

        # Set limits for log-log plot
        stats = data.describe()
        # NB Do not set common x limits across pages/nodes; only within.
        limits = p9.expand_limits(y=[3e1, stats.loc["max", "value"]])

        yield from [ggplot + limits for _, ggplot in self.groupby_plot(data, "n")]


class InvCost0(Plot):
    """All transport investment cost."""

    basename = "inv-cost-transport"
    inputs = ["inv_cost:nl-t-yv:transport all"]
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
    inputs = ["inv_cost:nl-t-yv:non-ldv"]


class FixCost(Plot):
    """Fixed cost."""

    basename = "fix-cost"
    inputs = ["fix_cost:nl-t-yv-ya:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="fix_cost", color="t", group="t * yv"),
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
        p9.aes(x="ya", y="var_cost", color="t", group="t * yv"),
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
        # TODO remove typing exclusion once plotnine >0.12.4 is released
        p9.facet_wrap(
            ["nl"],
            ncol=2,
            labeller=LabelFirst("node: {}"),  # type: ignore [arg-type]
        ),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="LDV technology"),
    ]

    def generate(self, data):
        return p9.ggplot(data) + self.static + self.ggtitle()


class OutShareLDV0(Plot):
    """Share of total LDV output [Ø]."""

    basename = "out-share-t-ldv"
    inputs = ["out:nl-t-ya:ldv+units"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="t"),
        p9.geom_bar(stat="identity", width=4),
        # # Select a palette with up to 12 colors
        # p9.scale_fill_brewer(type="qual", palette="Set3"),
        p9.labs(x="Period", y="", fill="LDV technology"),
    ]

    def generate(self, data):
        # Normalize data
        # TODO Do this in genno
        data["value"] = data["value"] / data.groupby(["nl", "ya"])["value"].transform(
            "sum"
        )

        yield from [ggplot for _, ggplot in self.groupby_plot(data, "nl")]


class OutShareLDV1(Plot):
    """Share of LDV usage [Ø]."""

    basename = "out-share-t-cg-ldv"
    inputs = ["out:nl-t-ya-c", "cg"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="t"),
        p9.facet_wrap(["c"], ncol=5),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="", fill="LDV technology"),
    ]

    def generate(self, data, cg):
        # TODO do these operations in reporting for broader reuse
        # - Recover the consumer group code from the commodity code.
        # - Select only the consumer groups.
        # - Recover the LDV technology code from the usage technology code.
        data = (
            data.assign(c=lambda df: df.c.str.replace("transport pax ", ""))
            .query("c in @cg")
            .assign(t=lambda df: df.t.str.split(" usage by ", expand=True)[0])
        )
        # Normalize data
        data["value"] = data["value"] / data.groupby(["c", "nl", "ya"])[
            "value"
        ].transform("sum")

        yield from [ggplot for _, ggplot in self.groupby_plot(data, "nl")]


def c_group(df: pd.DataFrame, cg):
    return df.assign(
        c_group=df.c.apply(
            lambda v: "transport pax LDV" if any(cg_.id in v for cg_ in cg) else v
        )
    )


class Demand0(Plot):
    """Passenger transport demand [pass · km / a]."""

    basename = "demand"
    inputs = ["demand:n-c-y", "c::transport", "cg"]
    static = Plot.static + [
        p9.aes(x="y", y="demand", fill="c_group"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="", fill="Transport mode"),
    ]

    @staticmethod
    def _prep_data(data, commodities, cg):
        # Convert and select data
        _commodity = list(map(str, commodities))
        return (
            data.query("c in @_commodity")
            .pipe(c_group, cg)
            .groupby(["c_group", "n", "y"])
            .aggregate({"demand": "sum"})
            .reset_index()
        )

    def generate(self, data, commodities, cg):
        data = self._prep_data(data, commodities, cg)
        yield from [ggplot for _, ggplot in self.groupby_plot(data, "n")]


class Demand1(Demand0):
    """Share of transport demand [Ø]."""

    basename = "demand-share"

    def generate(self, data, commodities, cg):
        data = self._prep_data(data, commodities, cg)
        # Normalize
        data["demand"] = data["demand"] / data.groupby(["n", "y"])["demand"].transform(
            "sum"
        )
        yield from [ggplot for _, ggplot in self.groupby_plot(data, "n")]


class DemandCap(Plot):
    """Transport demand per capita [km / a]."""

    basename = "demand-capita"
    inputs = ["demand:n-c-y:capita", "c::transport", "cg"]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="c"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="", fill="Transport mode group"),
    ]

    def generate(self, data, commodities, cg):
        # Convert and select data
        data = data.query(f"c in {repr(list(map(str, commodities)))}").pipe(c_group, cg)
        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot


def _reduce_units(df: pd.DataFrame, target_units) -> tuple[pd.DataFrame, str]:
    df_units = df["unit"].unique()
    assert 1 == len(df_units)
    tmp = registry.Quantity(1.0, df_units[0]).to(target_units)
    return (
        df.eval("value = value * @tmp.magnitude").assign(unit=f"{tmp.units:~}"),
        f"{tmp.units:~}",
    )


class DemandExo(Plot):
    """Passenger transport activity."""

    runs_on_solved_scenario = False
    basename = "demand-exo"
    inputs = [pdt_nyt]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="t"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="", fill="Mode (tech group)"),
    ]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.astype(dict(value=float))
        data, self.unit = _reduce_units(data, "Gp km / a")
        y_max = max(data["value"])

        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class DemandExoCap0(Plot):
    """Passenger transport activity per person."""

    runs_on_solved_scenario = False
    basename = "demand-exo-capita"
    inputs = [pdt_nyt + "capita+post"]
    static = Plot.static + [
        p9.aes(x="y", y="value", fill="t"),
        p9.geom_bar(stat="identity", width=4),
        p9.labs(x="Period", y="", fill="Transport mode"),
    ]

    def generate(self, data):
        # FIXME shouldn't need to change dtype here
        data = data.astype(dict(value=float))
        data, self.unit = _reduce_units(data, "Mm / a")
        y_max = max(data["value"])

        for _, ggplot in self.groupby_plot(data, "n"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class DemandExoCap1(DemandExoCap0):
    """Transport demand per capita.

    Unlike :class:`DemandExoCap0`, this uses GDP per capita as the abscissa/x-aesthetic.
    """

    runs_on_solved_scenario = False
    basename = "demand-exo-capita-gdp"
    inputs = [pdt_nyt + "capita+post", gdp_cap]
    static = PDT_CAP_GDP_STATIC

    def generate(self, df_pdt, df_gdp):
        # Merge data from two quantities; keep separate column names
        data = df_pdt.merge(
            df_gdp.rename(columns={"value": "gdp", "unit": "gdp_unit"}), on=["n", "y"]
        )

        data, self.unit = _reduce_units(data, "km / a")

        # Set limits for log-log plot
        stats = data.describe()
        limits = p9.expand_limits(
            x=stats.loc[["min", "max"], "gdp"], y=[3e1, stats.loc["max", "value"]]
        )

        yield from [ggplot + limits for _, ggplot in self.groupby_plot(data, "n")]


class EnergyCmdty0(Plot):
    """Energy input to transport [GWa]."""

    basename = "energy-c"
    inputs = ["y0", "in:nl-ya-c:transport all"]
    static = Plot.static + [
        p9.aes(x="ya", y="value", fill="c"),
        p9.geom_bar(stat="identity", width=5, color="black"),
        p9.labs(x="Period", y="Energy", fill="Commodity"),
    ]

    def generate(self, y0: int, data):
        # Discard data for certain commodities
        data = data[
            ~(
                data.c.str.startswith("transport")
                | (data.c == "disutility")
                | (data.ya < y0)
            )
        ]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot


class EnergyCmdty1(EnergyCmdty0):
    """Share of energy input to transport [0]."""

    basename = "energy-c-share"

    def generate(self, y0: int, data):
        # Discard data for certain commodities
        data = data[
            ~(
                data.c.str.startswith("transport")
                | (data.c == "disutility")
                | (data.ya < y0)
            )
        ]
        # Normalize data
        # TODO Do this in genno
        data["value"] = data["value"] / data.groupby(["nl", "ya"])["value"].transform(
            "sum"
        )

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot


class Scale1Diff(Plot):
    """scale-1 factor in y=2020; changes between 2 scenarios."""

    basename = "scale-1-diff"

    runs_on_solved_scenario = False
    inputs = ["scale-1:nl-t-c-l-h:a", "scale-1:nl-t-c-l-h:b"]

    _s, _v, _y = "scenario", "value", "t + ' ' + c"
    static = Plot.static + [
        p9.aes(x=_v, y=_y, yend=_y, group=_s, color=_s, shape=_s),
        p9.facet_wrap("nl", scales="free_x"),
        p9.geom_vline(p9.aes(xintercept=_v), pd.DataFrame([[1.0]], columns=[_v])),
        p9.geom_point(),
        p9.scale_shape(unfilled=True),
        p9.scale_x_log10(),
        p9.labs(x="", y="Mode × commodity"),
    ]

    def generate(self, data_a, data_b):
        # Data for plotting points
        df0 = (
            pd.concat([data_a.assign(scenario="a"), data_b.assign(scenario="b")])
            .drop(["l", "h", "unit"], axis=1)
            .query("nl != 'R12_GLB'")
        )
        # Data for plotting segments/arrows
        df1 = (
            df0.pivot(columns="scenario", index=["nl", "t", "c"])
            .set_axis(["value", "xend"], axis=1)
            .reset_index()
            .assign(scenario="diff")
        )

        return (
            p9.ggplot(df0)
            + p9.geom_segment(p9.aes(xend="xend"), df1, arrow=p9.arrow(length=0.03))
            + self.static
            + self.ggtitle()
        )


class Stock0(Plot):
    """LDV transport vehicle stock."""

    basename = "stock-ldv"
    # Partial sum over driver_type dimension
    inputs = ["CAP:nl-t-ya:ldv+units"]
    static = Plot.static + [
        p9.aes(x="ya", y="CAP", color="t"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="Powertrain technology"),
    ]

    def generate(self, data):
        y_max = max(data["CAP"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


class Stock1(Plot):
    """Non-LDV transport vehicle stock.

    Same as Stock0, but for non-LDV techs only.
    """

    basename = "stock-non-ldv"
    inputs = ["CAP:nl-t-ya:non-ldv+units"]
    static = Plot.static + [
        p9.aes(x="ya", y="CAP", color="t"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="Powertrain technology"),
    ]

    def generate(self, data):
        if not len(data):
            return

        y_max = max(data["CAP"])
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, "nl"):
            yield ggplot + p9.expand_limits(y=[0, y_max])


def prepare_computer(c: Computer):
    """Add :data:`.PLOTS` to `c`.

    Adds:

    - 1 key like **"plot inv-cost"** corresponding to the :attr:`~.Plot.basename` of
      each :class:`.Plot` subclass defined in this module.
    - The key **"transport plots"** that triggers writing all the plots to file.
    """
    import matplotlib

    # Force matplotlib to use a non-interactive backend for plotting
    matplotlib.use("pdf")

    keys = []

    config: "Config" = c.graph["config"]["transport"]

    # Iterate over the Plot subclasses defined in the current module
    for plot in filter(
        lambda cls: isinstance(cls, type) and issubclass(cls, Plot) and cls is not Plot,
        globals().values(),
    ):
        if (not plot.runs_on_solved_scenario and config.with_solution) or (
            False  # Use True here or uncomment below to skip some or all plots
            # "stock" not in plot.basename
        ):
            log.info(f"Skip {plot}")
            continue
        keys.append(f"plot {plot.basename}")
        c.add(keys[-1], plot)

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(keys)} plots")
    c.add(key, keys)

    c.graph[k_report.all].append(key)
