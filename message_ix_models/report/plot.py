"""Plots for MESSAGEix-GLOBIOM reporting.

The current set functions on time series data stored on the scenario by
:mod:`message_ix_models.report` or :mod:`message_data` legacy reporting.
"""

import logging
import re
from collections.abc import Iterator, Sequence
from datetime import datetime
from importlib import import_module
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Literal

import genno.compat.plotnine
import pandas as pd
import plotnine as p9
from genno import Computer, Key, Keys

from message_ix_models.model.workflow import STAGE

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Protocol

    from genno.core.key import KeyLike

    from message_ix_models import Context
    from message_ix_models.types import PlotAddable

    class HasURL(Protocol):
        url: str


__all__ = [
    "EmissionsCO2",
    "FinalEnergy0",
    "FinalEnergy1",
    "LabelFirst",
    "Plot",
    "PlotTimeSeries",
    "PlotFromIAMC",
    "PrimaryEnergy0",
    "PrimaryEnergy1",
    "callback",
    "collect",
    "prepare_computer",
]

log = logging.getLogger(__name__)

# Quiet messages like:
#   "Fontsize 0.00 < 1.0 pt not allowed by FreeType. Setting fontsize= 1 pt"
# TODO Investigate or move upstream
logging.getLogger("matplotlib.font_manager").setLevel(logging.INFO + 1)

#: Reusable components.
COMMON: dict[str, "PlotAddable"] = {
    "A2 landscape": p9.theme(figure_size=(23.4, 16.6)),
    "A3 portrait": p9.theme(figure_size=(11.7, 16.6)),
    "A4 landscape": p9.theme(figure_size=(11.7, 8.3)),
}


class LabelFirst:
    """Labeller that labels the first item using a format string.

    Subsequent items are named with the bare value only.
    """

    __name__: str | None = None

    def __init__(self, fmt_string):
        self.fmt_string = fmt_string
        self.first = True

    def __call__(self, value):
        first = self.first
        self.first = False
        return self.fmt_string.format(value) if first else value


class Plot(genno.compat.plotnine.Plot):
    """Base class for plots based on reported time-series data.

    Subclasses should be used like:

    .. code-block:: python

       class MyPlot(Plot):
           ...

       c.add("plot myplot", MyPlot, "scenario")

    …that is, giving "scenario" or another key that points to a :class:`.Scenario`
    object with stored time series data. See the examples in this file.

    The :attr:`single` and :attr:`stage` class attributes can be used to specify the
    context in which the plot is meant to be used:

    - :data:`.STAGE.BUILD`, :py:`single=True`: Plots to be used during the process of
      building a single scenario. These are used within a :class:`~.genno.Computer`,
      without a built or solved Scenario. See for example
      :func:`.transport.build.get_computer`.

    - :data:`.STAGE.BUILD`, :py:`single=False`: Plots for pre-solve or build-process
      analysis/debugging of multiple scenarios. These are added by, for instance,
      :func:`.transport.build.debug_multi`.

    - :data:`.STAGE.REPORT`, :py:`single=True`: Plots for post-solve reporting of
       single MESSAGEix-GLOBIOM scenarios. These are added to a :class:`.Reporter` via
       :func:`.report.prepare_reporter` or a module-specific callback, for instance
       :func:`.transport.report.callback`.

    - :data:`.STAGE.REPORT`, :py:`single=False`: Plots of post-solve reporting of
      multiple MESSAGEix-GLOBIOM scenarios, or of data reported from these. These are
      added by, for instance, :func:`.transport.report.multi`.
    """

    # Narrow upstream type
    # TODO Move upstream to genno
    inputs: Sequence["KeyLike"]

    #: 'Static' geoms: list of plotnine objects that are not dynamic.
    static: list["PlotAddable"] = [COMMON["A2 landscape"]]

    #: Fixed plot title string. If not given, the first line of the class docstring is
    #: used.
    title: str | None = None

    #: Units expression for plot title.
    unit: str | None = None

    #: Workflow stage at which the Plot is to be used.
    stage: Literal[STAGE.BUILD, STAGE.REPORT] = STAGE.REPORT

    #: :any:`True` if the plot is to be used in a :class:`.Reporter` with keys/tasks for
    #: reporting a single scenario. Use :any:`False` for plots of data from multiple
    #: scenarios.
    single: bool = True

    # Object (Scenario, ScenarioInfo, etc.) with a `url` attribute, for ggtitle()
    _scenario: "HasURL"

    @classmethod
    def add_tasks(
        cls, c: Computer, key: "KeyLike", *inputs, strict: bool = False
    ) -> "KeyLike":
        """Add tasks to `c` to generate and save the Plot.

        Beyond the base class method, this method:

        - Constructs an output path from :attr:`basename`, :attr:`suffix`, and the
          :func:`make_output_path` operator. This in turn uses the :py:`c.config` values
          "output_dir" (if :attr:`stage` is :any:`.STAGE.REPORT`) or "build debug dir".
        - If :py:`single is False`, adds a key "scenario" with a dummy URL that can be
          used by :meth:`.ggtitle`.
        - Appends 2 keys for the above to the `inputs` for use by :meth:`save`.
        """

        # Output path for this parameter
        k_path = Key(key) + "path"
        # Construct the output path for this plot
        # If single=True, config["output_dir"] **may** include a subdirectory from the
        # scenario URL
        c.add(
            k_path,
            "make_output_path",
            "config",
            name=f"{cls.basename}{cls.suffix}",
            config_key="output_dir" if cls.stage is STAGE.REPORT else "build debug dir",
        )

        if not cls.single and c.graph.get("scenario", None) is None:
            # Add a placeholder for ggtitle() formatting of scenario.url
            c.add("scenario", SimpleNamespace(url="Multiple scenarios"))

        # Prepare inputs
        # - Same as parent class: explicit args to add_tasks() or from class attribute
        # - 2 items expected by save(), below
        _inputs = list(inputs if inputs else cls.inputs) + [k_path, "scenario"]

        return super(Plot, cls).add_tasks(c, key, *_inputs, strict=strict)

    def ggtitle(self, extra: str | None = None) -> "PlotAddable":
        """Return :class:`plotnine.ggtitle` including the current date & time."""
        title_parts = [
            (self.title or self.__doc__ or "").splitlines()[0].rstrip("."),
            f"[{self.unit}]" if self.unit else None,
            f"— {extra}" if extra else None,
        ]
        subtitle_parts = [
            self._scenario.url,
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

    def save(self, config, *args, **kwargs) -> "Path | None":
        """Store the last 2 `args` appended by :meth:`add_tasks`."""
        *_args, self.path, self._scenario = args

        # Call the parent method with the remaining arguments
        return super().save(config, *_args, **kwargs)


class PlotTimeSeries(Plot):
    """Plot of time series data from a scenario."""

    single = True

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

        match len(inputs):
            case 0:
                k_scenario = "scenario"
            case 1:
                k_scenario = inputs[0]
            case _:
                raise ValueError(f"Expected at most 1 inputs; got {inputs}")

        if len(cls.inputs_regex):
            # Retrieve all time series data, for advanced filtering
            all_data = Key(k_scenario) + "iamc"
            c.add(all_data, "get_ts", k_scenario)

            # Iterate over matched items from `inputs` and `inputs_regex`
            for k, expr in zip_longest(cls.inputs, cls.inputs_regex):
                if expr is None:
                    break
                # Filter the data given by `expr` from all::iamc
                c.add(k, "filter_ts", all_data, copy(expr))
        else:
            for k in map(Key, cls.inputs):
                # Add a computation to get the time series data for a specific variable
                c.add(k, "get_ts", k_scenario, dict(variable=k.name))

        # Add the plot itself
        return super().add_tasks(c, key, strict=strict)


class PlotFromIAMC(Plot):
    """:class:`Plot` that uses a subset of data from an IAMC-structured input.

    :attr:`.Plot.inputs` must be of length 1 and include the dimensions :math:`(n, s, u,
    v, y)`.
    """

    #: :py:`variable` argument to :func:`genno.compat.pyam.operator.quantity_from_iamc`.
    iamc_variable_pattern: str

    @classmethod
    def add_tasks(
        cls, c: Computer, key: "KeyLike", *inputs, strict: bool = False
    ) -> "KeyLike":
        """Select a subset of data and reduce its dimensionality."""

        assert 1 == len(cls.inputs) and not inputs
        k = Keys(input=cls.inputs[0], subset=Key(cls.basename, "nsy", "in"))
        assert set("nsy") < set(k.input.dims)

        c.add(
            k.subset, "quantity_from_iamc", k.input, variable=cls.iamc_variable_pattern
        )

        # Call the upstream method
        return super(PlotFromIAMC, cls).add_tasks(c, key, k.subset, strict=strict)


class EmissionsCO2(PlotTimeSeries):
    """CO₂ Emissions."""

    basename = "emission-CO2"
    inputs = ["Emissions|CO2::iamc"]

    static = Plot.static + [
        p9.aes(x="year", y="value", color="region"),
        p9.geom_line(),
        p9.geom_point(),
        p9.labs(x="Period", y="", color="Region"),
    ]

    def generate(self, data: pd.DataFrame):
        self.unit = data["unit"].unique()[0]

        for _, ggplot in self.groupby_plot(data, data.region.str.contains("GLB")):
            y_max = max(ggplot.data["value"])
            yield ggplot + p9.expand_limits(y=[0, y_max]) + self.ggtitle("")


class FinalEnergy0(EmissionsCO2):
    """Final Energy."""

    basename = "fe0"
    inputs = ["Final Energy::iamc"]


class FinalEnergy1(PlotTimeSeries):
    """Final Energy."""

    basename = "fe1"
    inputs = ["fe1-0::iamc"]

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

    def generate(self, data: pd.DataFrame):
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
    inputs = ["pe1-0::iamc"]

    _omit = ["Fossil", "Non-Biomass Renewables", "Secondary Energy Trade"]
    inputs_regex = [re.compile(rf"Primary Energy\|((?!{'|'.join(_omit)})[^\|]*)")]


def callback(c: Computer, context: "Context") -> None:
    """Add all :data:`PLOTS` to `c`.

    Also add a key "plot all" to triggers the generation of all plots.
    """
    prepare_computer(c, __name__, "plot all")


def collect(
    module: str | ModuleType, stage: STAGE | None = None, single: bool | None = None
) -> Iterator[type[Plot]]:
    """Iterate over plots from `module`.

    If `stage` or `single` are given, collect() iterates over only those plots where
    the attributes of the same name match.
    """

    mod = import_module(module) if isinstance(module, str) else module

    for obj in map(lambda name: getattr(mod, name), dir(mod)):
        # Check for a concrete subclass of Plot that matches the filters
        if (
            isinstance(obj, type)
            and issubclass(obj, Plot)
            and obj not in (Plot, PlotFromIAMC, PlotTimeSeries)
            and stage in {None, obj.stage}
            and single in {None, obj.single}
        ):
            yield obj


def prepare_computer(
    c: Computer,
    module: str | ModuleType | None = None,
    target: "KeyLike | None" = None,
    *,
    stage: STAGE | None = None,
    single: bool | None = None,
) -> None:
    """Add plots to `c` from `module`.

    Parameters
    ----------
    stage
    single
        Passed to :func:`collect`.
    target
        If given, add a task at this key that collects and summarizes all added plots.
    """
    # Force matplotlib to use a non-interactive backend for plotting
    import matplotlib

    matplotlib.use("pdf")

    # Iterate over the Plot subclasses defined in the current module
    keys = []
    for plot in collect(module or __name__, stage=stage, single=single):
        keys.append(f"plot {plot.basename}")
        c.add(keys[-1], plot)

    if target:
        log.info(f"Add {target!r} collecting {len(keys)} plots")
        c.add(target, "summarize", *keys)
    else:
        log.info(f"Added {len(keys)} plots")
