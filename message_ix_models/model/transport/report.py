import logging
from operator import attrgetter

from dask.core import quote
from message_ix.reporting import Reporter
from message_ix_models import Context

from . import computations
from .plot import PLOTS
from .utils import read_config

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

    if computations not in rep.modules:
        rep.modules.append(computations)

    try:
        solved = rep.graph["scenario"].has_solution()
    except KeyError:
        solved = False

    # Read transport reporting configuration onto the latest Context
    context = Context.get_instance(-1)
    read_config(context)

    # Node list / regional aggregation
    if "regions" not in context:
        # Get the name of one node
        _n = rep.get("n")[-1]
        context.regions = _n.split("_")[0]
        log.info(f"Infer regional aggregation {repr(context.regions)} from {repr(_n)}")

    config = context["transport config"]["report"]
    config.update(context["transport config"])

    # Add configuration to the Reporter
    rep.graph["config"].setdefault("transport", {})
    rep.graph["config"]["transport"].update(config.copy())

    # Get a specification that describes this setting
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
        rep.set_filters(t=sorted(technologies, key=attrgetter("id")))

    # Queue of computations to add
    queue = []

    # Aggregate transport technologies
    for k in ("in", "out"):
        try:
            queue.append(
                (
                    ("aggregate", rep.full_key(k), "transport", dict(t=t_groups)),
                    dict(sums=True),
                )
            )
        except KeyError:
            if solved:
                raise

    # Only viable keys added
    rep.add_queue(queue)

    # Add key collecting all others
    # FIXME `added` includes all partial sums of in::transport etc.
    rep.add("transport all", ["transport plots"])

    # Configuration for :func:`check`. Adds a single key, 'transport check', that
    # depends on others and returns a :class:`pandas.Series` of :class:`bool`.
    rep.add(
        "transport check",
        computations.transport_check,
        "scenario",
        "ACT:nl-t-yv-ya-m-h",
    )

    # Add ex-post mode and demand calculations
    demand.prepare_reporter(
        rep, context, configure=False, exogenous_data=not solved, info=spec["add"]
    )

    # Add plots
    add_plots(rep)


def add_plots(rep: Reporter):
    try:
        solved = rep.graph["scenario"].has_solution()
    except KeyError:
        solved = False

    queue = []

    for plot in PLOTS:
        key = f"plot {plot.basename}"
        queue.append(((key, plot.make_task()), dict()))

    added = rep.add_queue(queue, max_tries=2, fail="raise" if solved else logging.INFO)

    plots = list(k for k in added if str(k).startswith("plot"))

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(plots)} plots")
    rep.add(key, plots)
