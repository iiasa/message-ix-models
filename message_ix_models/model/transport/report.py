"""Reporting/postprocessing for MESSAGEix-Transport."""
import logging
from typing import cast

import genno.config
from genno import Computer, MissingKeyError
from genno.computations import aggregate
from message_ix import Reporter
from message_ix_models import Context
from message_ix_models.util import eval_anno, private_data_path

from message_data.reporting.util import add_replacements

from . import Config
from .build import get_spec

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


def require_compat(c: Computer) -> None:
    c.require_compat("ixmp.reporting.computations")
    c.require_compat("message_ix.reporting.computations")
    c.require_compat("message_data.reporting.computations")
    c.require_compat("message_data.tools.gdp_pop")
    c.require_compat("message_data.model.transport.computations")


def _gen0(c: Computer, *keys) -> None:
    """Aggregate using groups of transport technologies."""
    for k0 in keys:
        # Reference the function to avoid the genno magic which would treat as sum()
        k = c.add(k0.add_tag("world agg"), aggregate, k0, "nl::world agg", False)
        k = c.add(k0.add_tag("transport agg"), aggregate, k, "t::transport agg", False)
        c.add("select", k0.add_tag("transport"), k, "t::transport modes 1", sums=True)


def _gen1(c: Computer, *keys) -> None:
    """Selected subsets of of transport technologies."""
    for key in keys:
        c.add("select", key.add_tag("ldv"), key, "t::transport LDV", sums=True)
        c.add("select", key.add_tag("non-ldv"), key, "t::transport non-ldv", sums=True)


@genno.config.handles("MESSAGEix-Transport", iterate=False)
def _handler(c: Computer, info):
    """Handle the ``MESSAGEix-Transport:`` config section."""
    # Require modules with computations
    require_compat(c)

    if info.get("filter", False):
        log.info("Filter out non-transport technologies")

        # Plain "transport" from the base model, for e.g. prices
        t_filter = {"transport"}
        # MESSAGEix-Transport -specific technologies
        t_filter.update(map(str, c.get("t::transport").copy()))
        # # Required commodities (e.g. fuel) from the base model
        # t_filter.update(spec.require.set["commodity"])

        cast(Reporter, c).set_filters(t=sorted(t_filter))

    context = c.graph["context"]
    config = c.graph["config"]
    config.setdefault("regions", context.model.regions)
    config["transport"] = context.transport
    config.setdefault("data source", dict())
    config["data source"].update(
        {k: getattr(context.transport.data_source, k) for k in ("gdp", "population")}
    )
    config["output_dir"] = context.get_local_path()


def callback(rep: Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGEix-Transport.

    Among others, adds:

    - ``{in,out}::transport``: with outputs aggregated by technology group or
      "mode".
    - ``transport plots``: the plots from :mod:`.transport.plot`.

      If the scenario to be reported is not solved, only a subset of plots are added.
    - ``transport all``: all of the above.
    """
    from . import build

    N_keys = len(rep.graph)

    scenario = rep.graph.get("scenario")
    try:
        solved = scenario.has_solution() if scenario else False
    except AttributeError:
        solved = False  # "scenario" is not present in the Reporter; may be added later

    # - Configure MESSAGEix-Transport.
    # - Add structure and other information.
    # - Call, inter alia:
    #   - demand.prepare_computer() for ex-post mode and demand calculations
    #   - plot.prepare_computer() for plots
    check = build.get_computer(
        context, obj=rep, scenario=scenario, options=dict(exogenous_data=not solved)
    )

    assert check is rep

    # Configure replacements for conversion to IAMC data structure
    spec = context["transport spec"]
    add_replacements("t", spec.add.set["technology"])

    # Apply some functions that generate sub-graphs
    try:
        rep.apply(_gen0, "in", "out", "emi")
    except MissingKeyError:
        if solved:
            raise

    rep.apply(_gen1, "CAP:nl-t-ya", "in:nl-t-ya-c", "inv_cost:nl-t-yv")

    # Add further computations (including conversions to IAMC tables) defined in a file
    rep.configure(path=private_data_path("transport", "report.yaml"))

    log.info(f"Added {len(rep.graph)-N_keys} keys")


def configure_legacy_reporting(config: dict) -> None:
    """Callback to configure the legacy reporting."""
    from message_data.tools.post_processing.default_tables import COMMODITY

    # NB the legacy reporting doesn't pass a context object to the hook that calls this
    #    function, so get an instance directly
    context = Context.get_instance()

    # If it does not already exist, read transport configuration onto the Context,
    # including reporting config
    context.setdefault("transport", Config.from_context(context))

    # Get a spec
    spec = get_spec(context)

    # Commented: pp_utils._retr_act_data() raises IndexError if lists are empty
    # # Clear existing entries
    # # NB it should not have any effect to leave these in
    # for key in config:
    #     if key.startswith("trp "):
    #         # log.debug(f"Discard '{key}': {config[key]}")
    #         config[key] = []

    # Iterate over technologies in the transport model spec
    for t in spec.add.set["technology"]:
        try:
            # Retrieve the input commodity for this technology
            commodity = eval_anno(t, "input")["commodity"]
        except (TypeError, KeyError):  # No annotation, or no "commodity" info
            commodity = None
        else:
            # Map to the shorthands used in legacy reporting
            commodity = COMMODITY.get(commodity)

        if commodity is None:
            # log.debug(f"{t}: No legacy reporting")  # Verbose
            continue

        group = f"trp {commodity}"
        # log.debug(f"{t} → '{group}'")
        config[group].append(t.id)
