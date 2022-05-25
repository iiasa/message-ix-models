"""Reporting/postprocessing for MESSAGEix-Transport."""
import logging
from copy import deepcopy
from operator import attrgetter
from typing import Any, Dict, List, Tuple

from dask.core import quote
from genno import Computer
from message_ix.reporting import Key, Reporter
from message_ix_models import Context, Spec

import message_data.tools.gdp_pop
from message_data.model.transport import computations, configure
from message_data.model.transport.plot import PLOTS

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


def transport_technologies(context) -> Tuple[Spec, List, Dict]:
    """Return info about transport technologies, given `context`."""
    from . import build

    # Get a specification that describes this setting
    spec = build.get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"].copy()

    # Subsets of transport technologies for aggregation and filtering
    t_groups: Dict[str, List[str]] = {"non-ldv": []}
    for tech in filter(  # Only include those technologies with children
        lambda t: len(t.child), context["transport set"]["technology"]["add"]
    ):
        t_groups[tech.id] = list(c.id for c in tech.child)
        # Store non-LDV technologies
        if tech.id != "LDV":
            t_groups["non-ldv"].extend(t_groups[tech.id])

    return spec, technologies, t_groups


def register_modules(rep: Computer):
    # NB this can be replaced by rep.require_compat(…) in genno >= 1.12
    for mod in (computations, message_data.tools.gdp_pop):
        if mod not in rep.modules:
            rep.modules.append(mod)


def callback(rep: Reporter):
    """:meth:`.prepare_reporter` callback for MESSAGE-Transport.

    Among others, adds:

    - ``{in,out}::transport``: with outputs aggregated by technology group or
      "mode".
    - ``transport plots``: the plots from :mod:`.transport.plot`.

      If the scenario to be reported is not solved, only a subset of plots are added.
    - ``transport all``: all of the above.
    """
    from . import demand

    register_modules(rep)

    try:
        solved = rep.graph["scenario"].has_solution()
    except KeyError:
        solved = False  # "scenario" is not present in the Reporter; may be added later

    N_keys = len(rep.graph)

    # Read transport configuration, including reporting config, onto the latest Context
    context = Context.get_instance(-1)
    configure(context, rep.graph.get("scenario"))

    config = context["transport config"]["report"]
    config.update(context["transport config"])

    # Add configuration to the Reporter
    rep.graph["config"].setdefault("transport", {})
    rep.graph["config"]["transport"].update(config.copy())

    # Retrieve information about the model structure
    spec, technologies, t_groups = transport_technologies(context)

    # 1. Add ex-post mode and demand calculations
    demand.prepare_reporter(
        rep, context, configure=False, exogenous_data=not solved, info=spec["add"]
    )

    # 2. Add model structure to the reporter
    # NB demand.add_structure() has already added some

    # Bare lists
    rep.add("t::transport", quote(technologies))
    rep.add("c::transport", quote(spec["add"].set["commodity"]))

    # Mappings for use with select()
    for id, techs in t_groups.items():
        rep.add(f"t::transport {id}", quote(dict(t=techs)))

    # Apply filters if configured
    if config["filter"]:
        # Include only technologies with "transport" in the name
        log.info("Filter out non-transport technologies")
        rep.set_filters(t=list(map(str, sorted(technologies, key=attrgetter("id")))))

    # 3. Assemble a queue of computations to add
    queue: List[Tuple[Tuple, Dict]] = []
    _ = dict()  # Shorthand for no keyword arguments
    _s = dict(sums=True)

    # Aggregate transport technologies
    key: Any
    for key in ("in", "out"):
        try:
            k = rep.full_key(key)
        except KeyError:
            if solved:
                raise
            else:
                continue
        queue.append((("aggregate", k, "transport", dict(t=t_groups)), _))

    # Selected subsets of certain quantities
    for key in (
        Key("CAP", "nl t ya".split()),
        Key("in", "nl t ya c".split()),
        Key("inv_cost", "nl t yv".split()),
    ):
        queue.append((("select", key.add_tag("ldv"), key, "t::transport LDV"), _s))
        queue.append(
            (("select", key.add_tag("non-ldv"), key, "t::transport non-ldv"), _s)
        )

    # Add all the computations in `queue`. Some may be discarded if inputs are missing.
    rep.add_queue(queue)

    # 4. Add further computations (incl. IAMC tables) defined in
    #    data/transport/report.yaml
    rep.configure(**deepcopy(context["transport report"]))

    # 5. Add plots
    queue = [((f"plot {name}", cls.make_task()), dict()) for name, cls in PLOTS.items()]

    added = rep.add_queue(queue, max_tries=2, fail="raise" if solved else logging.INFO)

    plots = list(k for k in added if str(k).startswith("plot"))

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(plots)} plots")
    rep.add(key, plots)

    # Add key collecting all others
    rep.add(
        "transport all",
        ["transport plots", "transport iamc file", "transport iamc store"],
    )

    log.info(f"Added {len(rep.graph)-N_keys} keys")
