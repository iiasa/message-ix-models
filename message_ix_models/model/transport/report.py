"""Reporting/postprocessing for MESSAGEix-Transport."""
import logging
from copy import deepcopy
from typing import Dict, List, Tuple

from dask.core import quote
from genno import Computer, MissingKeyError
from genno.computations import aggregate
from message_ix.reporting import Reporter
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


def _gen0(c: Computer, *keys):
    """Aggregate using groups of transport technologies."""
    for k1 in keys:
        k2 = k1.add_tag("transport agg 1")
        k3 = k1.add_tag("transport agg 2")
        k4 = k1.add_tag("transport")
        # Reference the function to avoid the genno magic which would treat as sum()
        c.add(k2, aggregate, k1, "nl::world agg", True)
        c.add(k3, aggregate, k2, "t::transport agg", False)
        c.add("select", k4, k3, "t::transport modes 1", sums=True)


def _gen1(c: Computer, *keys):
    """Selected subsets of of transport technologies."""
    for key in keys:
        c.add("select", key.add_tag("ldv"), key, "t::transport LDV", sums=True)
        c.add("select", key.add_tag("non-ldv"), key, "t::transport non-ldv", sums=True)


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

    # Add configuration to the Reporter
    config = context["transport config"]["report"]
    config.update(context["transport config"])
    rep.graph["config"].setdefault("transport", {})
    rep.graph["config"]["transport"].update(config.copy())

    # Retrieve information about the model structure
    spec, technologies, t_groups = transport_technologies(context)

    # 1. Add ex-post mode and demand calculations
    # TODO this calls add_structure(), which could be merged with (2) below
    demand.prepare_reporter(
        rep, context, configure=False, exogenous_data=not solved, info=spec["add"]
    )

    # 2. Add model structure to the reporter
    # NB demand.add_structure() has already added some

    # Bare lists
    rep.add("t::transport", quote(technologies))
    rep.add("c::transport", quote(spec["add"].set["commodity"]))

    # Mappings for use with aggregate, select, etc.
    rep.add("t::transport agg", quote(dict(t=t_groups)))
    # Sum across modes, including "non-ldv"
    rep.add("t::transport modes 0", quote(dict(t=list(t_groups.keys()))))
    # Sum across modes, excluding "non-ldv"
    rep.add(
        "t::transport modes 1",
        quote(dict(t=list(filter(lambda k: k != "non-ldv", t_groups.keys())))),
    )
    for id, techs in t_groups.items():
        rep.add(f"t::transport {id}", quote(dict(t=techs)))

    if config["filter"]:
        log.info("Filter out non-transport technologies")

        # Plain "transport" from the base model, for e.g. prices
        t_filter = {"transport"}
        # MESSAGEix-Transport -specific technologies
        t_filter.update(map(str, technologies.copy()))
        # # Required commodities (e.g. fuel) from the base model
        # t_filter.update(spec.require.set["commodity"])

        rep.set_filters(t=sorted(t_filter))

    # 3. Apply some functions that generate sub-graphs
    try:
        rep.apply(_gen1, "in", "out")
    except MissingKeyError:
        if solved:
            raise

    rep.apply(_gen1, "CAP:nl-t-ya", "in:nl-t-ya-c", "inv_cost:nl-t-yv")

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
        [
            "transport plots",
            # FIXME trailing colons should not be needed
            "transport iamc CSV:",
            "transport iamc XLSX:",
            # "transport iamc store:",
        ],
    )

    log.info(f"Added {len(rep.graph)-N_keys} keys")
