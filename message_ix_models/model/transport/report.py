"""Reporting/postprocessing for MESSAGEix-Transport."""
import logging
from collections import ChainMap, defaultdict
from copy import deepcopy
from operator import attrgetter
from typing import Dict, List, Tuple

import pandas as pd
from dask.core import quote
from ixmp.reporting import RENAME_DIMS
from message_ix.models import MESSAGE_ITEMS
from message_ix.reporting import Key, Quantity, Reporter
from message_ix_models import Context, ScenarioInfo, Spec
from pandas.api.types import is_scalar

from . import computations, configure
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

    if computations not in rep.modules:
        rep.modules.append(computations)

    try:
        solved = rep.graph["scenario"].has_solution()
    except KeyError:
        solved = False

    # Read transport reporting configuration onto the latest Context
    context = Context.get_instance(-1)
    configure(context)

    config = context["transport config"]["report"]
    config.update(context["transport config"])

    # Add configuration to the Reporter
    rep.graph["config"].setdefault("transport", {})
    rep.graph["config"]["transport"].update(config.copy())

    # Get a specification that describes this setting
    spec = build.get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"]

    # Combine technologies from disutility formulation
    # TODO do this somewhere earlier, e.g. in build.get_spec()
    disutil_spec = build.get_disutility_spec(context)
    technologies.extend(disutil_spec["add"].set["technology"])

    rep.add("t:transport", quote(technologies))

    # Subsets of transport technologies for aggregation and filtering
    t_groups: Dict[str, List[str]] = dict(nonldv=[])
    for tech in filter(  # Only include those technologies with children
        lambda t: len(t.child), context["transport set"]["technology"]["add"]
    ):
        t_groups[tech.id] = list(c.id for c in tech.child)
        rep.add(f"t:transport {tech.id}", quote(dict(t=t_groups[tech.id])))
        # Store non-LDV technologies
        if tech.id != "LDV":
            t_groups["nonldv"].extend(t_groups[tech.id])

    rep.add("t:transport non-LDV", quote(dict(t=t_groups["nonldv"])))

    # Set of all transport commodities
    rep.add("c:transport", quote(spec["add"].set["commodity"]))

    # Apply filters if configured
    if config["filter"]:
        # Include only technologies with "transport" in the name
        log.info("Filter out non-transport technologies")
        rep.set_filters(t=list(map(str, sorted(technologies, key=attrgetter("id")))))

    # Queue of computations to add
    queue: List[Tuple[Tuple, Dict]] = []

    # Shorthands for queue of computations to add
    _s = dict(sums=True)
    _si = dict(sums=True, index=True)

    # Aggregate transport technologies
    for k in ("in", "out"):
        try:
            queue.append(
                (("aggregate", rep.full_key(k), "transport", dict(t=t_groups)), _s)
            )
        except KeyError:
            if solved:
                raise

    # Keys
    dist_ldv = Key("distance", "nl driver_type".split(), "ldv")
    dist_nonldv = Key("distance", "nl", "non-ldv")
    inv_cost = Key("inv_cost", "nl t yv".split())
    CAP = Key("CAP", "nl t ya".split())
    CAP_ldv = CAP.add_tag("ldv")
    CAP_nonldv = CAP.add_tag("non-ldv")

    # Vehicle stocks
    queue.extend(
        [
            # Per capita
            (
                ("ratio", "demand:n-c-y:capita", "demand:n-c-y", "population:n-y"),
                dict(sums=False),
            ),
            # Investment costs
            (("select", inv_cost.add_tag("ldv"), inv_cost, "t:transport LDV"), _si),
            (
                (
                    "select",
                    inv_cost.add_tag("non-ldv"),
                    inv_cost,
                    "t:transport non-LDV",
                ),
                _si,
            ),
            (("select", CAP_ldv, CAP, "t:transport LDV"), _si),
            (("select", CAP_nonldv, CAP, "t:transport non-LDV"), _si),
            # Vehicle stocks for LDV
            ((dist_ldv, computations.distance_ldv, "config"), _si),
            (("ratio", "stock:nl-t-ya-driver_type:ldv", CAP_ldv, dist_ldv), _si),
            # Vehicle stocks for non-LDV technologies
            # commented: distance_nonldv() is incomplete
            # ((dist_nonldv, computations.distance_nonldv, "config"), _si),
            (("ratio", "stock:nl-t-ya:non-ldv", CAP_nonldv, dist_nonldv), _si),
        ]
    )

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
        Key("ACT", "nl t yv ya m h".split()),
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

    queue: List[Tuple[Tuple, Dict]] = []

    for plot in PLOTS:
        key = f"plot {plot.basename}"
        queue.append(((key, plot.make_task()), dict()))

    added = rep.add_queue(queue, max_tries=2, fail="raise" if solved else logging.INFO)

    plots = list(k for k in added if str(k).startswith("plot"))

    key = "transport plots"
    log.info(f"Add {repr(key)} collecting {len(plots)} plots")
    rep.add(key, plots)


# Shorthand for MESSAGE_VARS, below
def item(ix_type, idx_names):
    return dict(ix_type=ix_type, idx_names=tuple(idx_names.split()))


# Copied from message_ix.models.MESSAGE_ITEMS, where these entries are commented because
# of JDBCBackend limitations.
# TODO read from that location once possible
MESSAGE_VARS = {
    # Activity
    "ACT": item("var", "nl t yv ya m h"),
    # Maintained capacity
    "CAP": item("var", "nl t yv ya"),
    # New capacity
    "CAP_NEW": item("var", "nl t yv"),
    # Emissions
    "EMISS": item("var", "n e type_tec y"),
    # Extraction
    "EXT": item("var", "n c g y"),
    # Land scenario share
    "LAND": item("var", "n land_scenario y"),
    # Objective (scalar)
    "OBJ": dict(ix_type="var", idx_names=[]),
    # Price of emissions
    "PRICE_COMMODITY": item("var", "n c l y h"),
    # Price of emissions
    "PRICE_EMISSION": item("var", "n e t y"),
    # Relation (lhs)
    "REL": item("var", "relation nr yr"),
    # Stock
    "STOCK": item("var", "n c l y"),
}


def simulate_qty(name: str, item_info: dict, **data) -> Tuple[Key, Quantity]:
    """Return simulated data for item `name`."""
    # NB this is code lightly modified from make_df

    # Dimensions of the resulting quantity
    dims = list(
        map(
            lambda d: RENAME_DIMS.get(d, d),
            item_info.get("idx_names") or item_info.get("idx_sets", []),
        )
    )

    # Default values for every column
    data = ChainMap(data, defaultdict(lambda: None))

    # Arguments for pd.DataFrame constructor
    args = dict(data={})

    # Flag if all values in `data` are scalars
    all_scalar = True

    for column in dims + ["value"]:
        # Update flag
        all_scalar &= is_scalar(data[column])
        # Store data
        args["data"][column] = data[column]

    if all_scalar:
        # All values are scalars, so the constructor requires an index to be passed
        # explicitly.
        args["index"] = [0]

    return Key(name, dims), Quantity(
        pd.DataFrame(**args).set_index(dims) if len(dims) else pd.DataFrame(**args)
    )


def simulated_solution(rep: Reporter, spec: Spec):
    """Add data for a simulated model solution to `rep`, given `spec`."""
    # Merge the "require" and "add" sets of the spec
    info = ScenarioInfo()
    info.update(spec.require)
    info.update(spec.add)

    # Populate the sets (from `info`, maybe empty) and pars (empty)
    to_add = deepcopy(MESSAGE_ITEMS)
    # Populate variables
    to_add.update(MESSAGE_VARS)
    # Populate MACRO items
    to_add.update(
        {
            "GDP": item("var", "n y"),
            "MERtoPPP": item("var", "n y"),
        }
    )

    for name, item_info in to_add.items():
        if item_info["ix_type"] == "set":
            # Add the set elements
            rep.add(RENAME_DIMS.get(name, name), quote(info.set[name]))
        elif item_info["ix_type"] in ("par", "var"):
            key, data = simulate_qty(name, item_info)
            # log.debug(f"{key}\n{data}")
            rep.add(key, data, sums=True, index=True)

    # Prepare the base MESSAGEix computations
    rep._prepare("raise")
