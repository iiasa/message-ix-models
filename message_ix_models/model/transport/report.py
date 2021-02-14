import logging

from dask.core import quote
from message_ix.reporting import Reporter

from message_data.tools import Context
from . import computations
from .utils import read_config
from .plot import PLOTS


log = logging.getLogger(__name__)


def check(scenario):
    """Check that the transport model solution is complete.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario with solution.

    Returns
    -------
    pd.Series
        Index entries are str descriptions of checks. Values are :obj:`True` if the
        respective check passes.
    """
    # NB this is here to avoid circular imports
    from message_data.reporting import prepare_reporter, register

    register(callback)
    rep, key = prepare_reporter(scenario, "global.yaml", "transport check")
    return rep.get(key)


def callback(rep: Reporter):
    """:meth:`.prepare_reporter` callback for MESSAGE-Transport.

    Adds:

    - ``{in,out}::transport``: with outputs aggregated by technology group or
      "mode".
    - ``transport plots``: the plots from :mod:`.transport.plot`.
    - ``transport all``: all of the above.
    """
    from . import build, demand

    rep.modules.append(computations)

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

    spec = build.get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"]
    rep.add("t:transport", quote(technologies))

    # Groups of transport technologies for aggregation
    t_groups = {
        tech.id: list(c.id for c in tech.child)
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
        demand.prepare_reporter(rep, context, configure=False)
    except Exception as e:
        log.error(e)
        assert False

    # Add all plots
    plot_keys = []

    for plot in PLOTS:
        task = plot.make_task()
        plot_keys.append(rep.add(f"plot {plot.basename}", plot.make_task()))

        log.info(f"Add {repr(plot_keys[-1])}")
        log.debug(repr(task))

    rep.add("transport plots", plot_keys)

    # Add key collecting all others
    rep.add("transport all", all_keys + plot_keys)

    # Configuration for :func:`check`. Adds a single key, 'transport check', that
    # depends on others and returns a :class:`pandas.Series` of :class:`bool`.
    ACT = rep.infer_keys("ACT")
    rep.add("transport check", computations.transport_check, "scenario", ACT)
