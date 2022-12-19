"""Demand calculation for MESSAGEix-Transport."""
import logging
from functools import partial
from operator import itemgetter
from typing import Dict, List, Optional, cast

import genno.computations
import message_ix
import numpy as np
import pandas as pd
from dask.core import literal, quote
from genno import Computer, Key, KeyExistsError
from genno.computations import interpolate
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix.reporting import Reporter
from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.structure import get_codes, get_region_codes
from message_ix_models.util import adapt_R11_R14, broadcast, check_support

from message_data.model.transport import computations, plot
from message_data.model.transport.data import groups
from message_data.model.transport.report import require_compat, update_config
from message_data.model.transport.utils import get_techs, path_fallback
from message_data.tools import gdp_pop

log = logging.getLogger(__name__)


def dummy(
    commodities: List, nodes: List[str], y: List[int], config: dict
) -> Dict[str, pd.DataFrame]:
    """Dummy demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    if not config["transport"].data_source.dummy_demand:
        # No dummy data → return nothing
        return dict()

    common = dict(level="useful", time="year", value=10 + np.arange(len(y)), year=y)

    dfs = []

    for commodity in commodities:
        try:
            commodity.get_annotation(id="demand")
        except (AttributeError, KeyError):
            continue  # Not a demand commodity

        unit = "t km" if "freight" in commodity.id else "km"
        dfs.append(make_df("demand", commodity=commodity.id, unit=unit, **common))

    # # Dummy demand for light oil
    # common["level"] = "final"
    # dfs.append(make_df("demand", commodity="lightoil", **common))

    return dict(demand=pd.concat(dfs).pipe(broadcast, node=nodes))


def from_scenario(scenario: message_ix.Scenario) -> Reporter:
    """Return a Reporter for calculating demand based on `scenario`.

    Parameters
    ----------
    Scenario
        Solved Scenario

    Returns
    -------
    Reporter
    """
    rep = Reporter.from_scenario(scenario)

    prepare_reporter(rep, Context.get_instance())

    return rep


def add_exogenous_data(c: Computer, context: Context, info: ScenarioInfo) -> None:
    """Add exogenous data to `c` that mocks data coming from an actual Scenario.

    The specific quantities added are:

    - ``GDP:n-y``, from GEA, SSP, or SHAPE data; see :func:`.gdp_pop`.
    - ``PRICE_COMMODITY:n-c-y``, currently mocked based on the shape of ``GDP:n-y``
      using :func:`.dummy_prices`.

      .. todo:: Add an external data source.

    - ``MERtoPPP:n-y``, from :file:`mer-to-ppp.csv`. If ``context.model.regions`` is
      “R14”, data are adapted from R11 using :func:`.adapt_R11_R14`.

    See also
    --------
    :doc:`/reference/model/transport/data`
    """
    check_support(
        context,
        settings=dict(regions=frozenset(["R11", "R12", "R14"])),
        desc="Exogenous data for demand projection",
    )

    # Data from files. Add 3 computations per quantity.
    for key, basename, units in (
        # (gdp_k, "gdp", "GUSD/year"),  # Handled below
        (Key("MERtoPPP", "ny"), "mer-to-ppp", ""),
    ):
        # 1. Load the file
        k1 = Key(key.name, tag="raw")
        c.add(
            k1,
            partial(genno.computations.load_file, units=units),
            path_fallback(context, f"{basename}.csv"),
        )

        # 2. Rename dimensions
        k2 = key.add_tag("rename")
        c.add("rename_dims", k2, k1, quote(RENAME_DIMS))

        # 3. Maybe transform from R11 to another node list
        k3 = key.add_tag(context.model.regions)
        if context.model.regions in ("R11", "R12"):
            c.add(k3, k2)  # No-op/pass-through
        elif context.model.regions == "R14":
            c.add(k3, adapt_R11_R14, k2)

        c.add(key, partial(interpolate, coords=dict(y=info.Y)), k3, sums=True)

    gdp_keys = c.add("GDP:n-y", gdp_pop.gdp, "y", "config", sums=True)
    c.add("PRICE_COMMODITY:n-c-y", (computations.dummy_prices, gdp_keys[0]), sums=True)


def add_structure(c: Computer, context: Context, info: ScenarioInfo):
    """Add keys to `c` for model structure required by demand computations.

    This uses `info` to mock the contents that would be reported from an already-
    populated Scenario for sets "node", "year", and "cat_year".
    """
    # `info` contains only structure to be added, not existing/required structure. Add
    # information about the year dimension, to be used below.
    # TODO accomplish this by 'merging' the ScenarioInfo/spec.
    if not len(info.set["years"]):
        info.year_from_codes(get_codes(f"year/{context.years}"))
    if not len(info.set["node"]):
        info.set["node"] = get_region_codes(context.model.regions)

    for key, value in (
        ("c::transport", quote(info.set["commodity"])),
        ("cg", quote(info.set["consumer_group"])),
        ("n", quote(list(map(str, info.set["node"])))),
        ("nodes", quote(info.set["node"])),
        ("t::transport modes", quote(context.transport.demand_modes)),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
    ):
        try:
            c.add(key, value, strict=True)  # Raise an exception if `key` exists
        except KeyExistsError:
            continue  # Already present; don't overwrite

    # Retrieve information about the model structure
    spec, technologies, t_groups = get_techs(context)

    # Lists and subsets
    c.add("c::transport", quote(spec["add"].set["commodity"]))
    c.add("t::transport", quote(technologies))
    # TODO move upstream, e.g. to message_ix
    c.add("model_periods", "y::model", "y", "cat_year")

    # Mappings for use with aggregate, select, etc.
    c.add("t::transport agg", quote(dict(t=t_groups)))
    # Sum across modes, including "non-ldv"
    c.add("t::transport modes 0", quote(dict(t=list(t_groups.keys()))))
    # Sum across modes, excluding "non-ldv"
    c.add(
        "t::transport modes 1",
        quote(dict(t=list(filter(lambda k: k != "non-ldv", t_groups.keys())))),
    )
    for id, techs in t_groups.items():
        c.add(f"t::transport {id}", quote(dict(t=techs)))


def prepare_reporter(
    rep: Computer,
    context: Context,
    configure: bool = True,
    exogenous_data: bool = False,
    info: Optional[ScenarioInfo] = None,
) -> None:
    """Prepare `rep` for calculating transport demand.

    Parameters
    ----------
    rep : Reporter
        Must contain the keys ``<GDP:n-y>``, ``<MERtoPPP:n-y>``.
    """
    if configure:
        # Configure the reporter; keys are stored
        rep.configure(transport=context.transport)

    require_compat(rep)
    update_config(rep, context)

    # Always ensure structure is available
    add_structure(rep, context, info or ScenarioInfo())

    if exogenous_data:
        assert info, "`info` arg required for prepare_reporter(…, exogenous_data=True)"
        add_exogenous_data(rep, context, info)

    # Keys to refer to quantities
    # Existing keys, from Reporter.from_scenario() or add_structure() (above)
    gdp = rep.full_key("GDP")
    mer_to_ppp = rep.full_key("MERtoPPP")
    price_full = cast(Key, rep.full_key("PRICE_COMMODITY")).drop("h", "l")

    # Keys for new quantities
    pop_at = Key("population", "n y area_type".split())
    pop = pop_at.drop("area_type")
    cg = Key("cg share", "n y cg".split())
    gdp_ppp = Key("GDP", "ny", "PPP")
    gdp_ppp_cap = gdp_ppp.add_tag("capita")
    gdp_index = gdp_ppp_cap.add_tag("index")
    pdt_nyt = Key("pdt", "nyt")  # Total PDT shared out by mode
    pdt_cap = pdt_nyt.drop("t").add_tag("capita")
    pdt_ny = pdt_nyt.drop("t").add_tag("total")
    price_sel1 = price_full.add_tag("transport")
    price_sel0 = price_sel1.add_tag("raw units")
    price = price_sel1.add_tag("smooth")
    cost = Key("cost", "nyct")
    sw = Key("share weight", "nty")

    n = "n::ex world"
    y = "y::model"

    # Inputs for Computer.add_queue()
    # NB the genno method actually requires an iterable of (tuple, dict), where the dict
    #    is keyword arguments to Computer.add(). In all cases here, this would be empty,
    #    so for simplicity the dict is added below in the call to add_queue()
    # TODO enhance genno so the empty dict() is optional.
    queue = [
        # Values based on configuration
        ("quantity_from_config", "speed:t", "config", quote("speeds")),
        ("quantity_from_config", "whour:", "config", quote("work_hours")),
        ("quantity_from_config", "lambda:", "config", quote("lamda")),
        # List of nodes excluding "World"
        # TODO move upstream to message_ix
        ("nodes_ex_world", "n::ex world", "n"),
        ("nodes_ex_world", "n::ex world+code", "nodes"),
        ("nodes_world_agg", "nl::world agg", "config"),
        # Base share data
        ("base_shares", "base shares:n-t-y", n, "t::transport modes", y, "config"),
        # Population data; data source according to config
        (pop, partial(gdp_pop.population, extra_dims=False), "y", "config"),
        # Population shares by area_type
        (pop_at, groups.urban_rural_shares, y, "config"),
        # Consumer group sizes
        # TODO ixmp is picky here when there is no separate argument to the callable;
        # fix.
        (cg, groups.cg_shares, pop_at, quote(context)),
        # PPP GDP, total and per capita
        ("product", gdp_ppp, gdp, mer_to_ppp),
        ("ratio", gdp_ppp_cap, gdp_ppp, pop),
        # GDP index
        ("y0", itemgetter(0), y),
        ("index_to", gdp_index, gdp_ppp_cap, literal("y"), "y0"),
        # Total demand
        ("pdt_per_capita", pdt_cap, gdp_ppp_cap, "config"),
        ("product", pdt_ny, pdt_cap, pop),
        # Value-of-time multiplier
        ("votm", "votm:n-y", gdp_ppp_cap),
        # Select only the price of transport services
        # FIXME should be the full set of prices
        ("select", price_sel0, price_full, dict(c="transport")),
        ("price_units", price_sel1, price_sel0),
        # Smooth prices to avoid zig-zag in share projections
        ("smooth", price, price_sel1),
        # Transport costs by mode
        ("cost", cost, price, gdp_ppp_cap, "whour:", "speed:t", "votm:n-y", y),
        # Share weights
        (
            "share_weight",
            sw,
            "base shares:n-t-y",
            gdp_ppp_cap,
            cost,
            "lambda:",
            n,
            y,
            "t::transport",
            "cat_year",
            "config",
        ),
        # Shares
        ("shares:n-t-y", partial(computations.logit, dim="t"), cost, sw, "lambda:", y),
        # Total PDT shared out by mode
        ("product", pdt_nyt.add_tag("0"), pdt_ny, "shares:n-t-y"),
        # Adjustment factor
        ("factor_pdt", "pdt factor:n-y-t", n, y, "t::transport modes", "config"),
        # Only the LDV values
        (
            "select",
            "ldv pdt factor:n-y",
            "pdt factor:n-y-t",
            dict(t=["LDV"], drop=True),
        ),
        ("product", pdt_nyt, pdt_nyt.add_tag("0"), "pdt factor:n-y-t"),
        # Per capita (for validation)
        ("ratio", "transport pdt:n-y-t:capita", pdt_nyt, pop),
        # LDV PDT only
        ("select", "ldv pdt:n-y:ref", pdt_nyt, dict(t=["LDV"], drop=True)),
        # Indexed to base year
        ("index_to", "ldv pdt:n-y:index", "ldv pdt:n-y:ref", literal("y"), "y0"),
        ("advance_ldv_pdt", "ldv pdt:n:advance", "config"),
        # Compute LDV PDT as ADVANCE base-year values indexed to overall growth
        ("product", "ldv pdt::total+0", "ldv pdt:n-y:index", "ldv pdt:n:advance"),
        ("product", "ldv pdt::total", "ldv pdt:n-y:total+0", "ldv pdt factor:n-y"),
        # LDV PDT shared out by consumer group
        ("product", "ldv pdt", "ldv pdt:n-y:total", cg),
        # Freight from IEA EEI
        # (("iea_eei_fv", "fv:n-y:historical", quote("tonne-kilometres"), "config"),
        # Freight from ADVANCE
        ("advance_fv", "fv:n:historical", "config"),
        ("product", "fv:n-y:0", "fv:n:historical", gdp_index),
        # Adjustment factor
        ("factor_fv", "fv factor:n-y", n, y, "config"),
        ("product", "fv:n-y", "fv:n-y:0", "fv factor:n-y"),
        # Convert to ixmp format
        (
            "as_message_df",
            "transport demand freight::ixmp",
            "fv:n-y",
            "demand",
            dict(node="n", year="y"),
            dict(commodity="transport freight", level="useful", time="year"),
        ),
        (
            "transport demand passenger::ixmp",
            computations.demand_ixmp0,
            pdt_nyt,
            "ldv pdt:n-y-cg",
        ),
        # Dummy demands, in case these are configured
        (
            "dummy demand::ixmp",
            dummy,
            "c::transport",
            "nodes::ex world",
            "y::model",
            "config",
        ),
    ]

    # Plots
    for name, cls in plot.PLOTS.items():
        if "demand" not in name:
            continue
        queue.append((f"plot {name}", cls.make_task()))

    rep.add_queue((item, dict()) for item in queue)
