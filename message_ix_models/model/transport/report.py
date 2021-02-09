import logging

from dask.core import quote
from message_ix.reporting import Reporter
import pandas as pd

from message_data.tools import Context, ScenarioInfo
from .plot import PLOTS
from .utils import read_config


log = logging.getLogger(__name__)


#: Reporting configuration for :func:`check`. Adds a single key,
#: 'transport check', that depends on others and returns a
#: :class:`pandas.Series` of :class:`bool`.
CONFIG = dict(
    general=[
        dict(key="transport check", comp="transport_check", inputs=["scenario", "ACT"])
    ],
)


def check(scenario):
    """Check that the transport model solution is complete.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario with solution.

    Returns
    -------
    pd.Series
        Index entries are str descriptions of checks. Values are the
        :obj:`True` if the respective check passes.
    """
    # NB this is here to avoid circular imports
    from message_data.reporting.core import prepare_reporter

    rep, key = prepare_reporter(scenario, CONFIG, "transport check")
    return rep.get(key)


def check_computation(scenario, ACT):
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=["transport freight load factor", "transport pax load factor"])
    checks["'transport * load factor' technologies are active"] = len(
        ACT_lf
    ) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)


def callback(rep: Reporter):
    """:meth:`.prepare_reporter` callback for MESSAGE-Transport.

    Adds:

    - ``{in,out}::transport``: with outputs aggregated by technology group or
      "mode".
    - ``transport plots``: the plots from :mod:`.transport.plot`.
    - ``transport all``: all of the above.
    """
    from .build import get_spec
    from .demand import prepare_reporter as prepare_demand

    # Read transport reporting configuration onto the latest Context
    context = Context.get_instance(-1)
    read_config(context)

    # Node list / regional aggregation
    # Get the name of one node
    _n = rep.get("n")[-1]
    context.regions = _n.split("_")[0]
    log.info(f"Infer regional aggregation {repr(context.regions)} from {repr(_n)}")

    # Add configuration to the Reporter
    config = context["transport config"]["report"]
    config.update(context["transport config"])
    rep.graph["config"].setdefault("transport", {})
    rep.graph["config"]["transport"].update(config.copy())

    spec = get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"]
    rep.add("t:transport", quote(technologies))

    # Groups of transport technologies for aggregation
    t_groups = {
        tech.id: list(tech.child)
        # Only include those technologies with children
        for tech in filter(
            lambda t: len(t.child), context["transport set"]["technology"]["add"]
        )
    }

    # Set of all transport commodities
    rep.add("c:transport", quote(spec["add"].set["commodity"]))

    # Apply filters if configured
    if config["filter"]:
        # Include only technologies with "transport" in the name
        log.info("Filter out non-transport technologies")
        rep.set_filters(t=sorted(technologies))

    # List of all reporting keys added
    all_keys = []

    # Aggregate transport technologies
    for k in rep.infer_keys(["in", "out"]):
        keys = rep.aggregate(k, "transport", dict(t=t_groups), sums=True)
        all_keys.append(keys[0])
        log.info(f"Add {repr(keys[0])} + {len(keys)-1} partial sums")

    # Add ex-post mode and demand calculations
    try:
        prepare_demand(rep, context, configure=False)
    except Exception as e:
        log.error(e)
        assert False

    # Add all plots
    plot_keys = []

    for plot in PLOTS:
        key = f"plot {plot.name}"
        task = plot.computation()
        rep.add(key, task)
        plot_keys.append(key)

        log.info(f"Add {repr(key)}")
        log.debug(repr(task))

    rep.add("transport plots", plot_keys)

    # Add key collecting all others
    rep.add("transport all", all_keys + plot_keys)
