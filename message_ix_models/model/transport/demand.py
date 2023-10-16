"""Demand calculation for MESSAGEix-Transport."""
import logging
from functools import partial
from operator import itemgetter
from typing import Dict, List, Tuple, cast

import numpy as np
import pandas as pd
from dask.core import literal, quote
from genno import Computer, Key, Quantity
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast

from . import computations, groups

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


def add_exogenous_data(c: Computer, info: ScenarioInfo) -> None:
    """Add exogenous data to `c` that mocks data coming from an actual Scenario.

    The specific quantities added are:

    - ``GDP:n-y``, from GEA, SSP, or SHAPE data; see :func:`.gdp_pop`.
    - ``PRICE_COMMODITY:n-c-y``, currently mocked based on the shape of ``GDP:n-y``
      using :func:`.dummy_prices`.

      .. todo:: Add an external data source.

    - ``MERtoPPP:n-y``, from :file:`mer-to-ppp.csv`. If ``context.model.regions`` is
      “R14”, data are adapted from R11 using :obj:`.adapt_R11_R14`.

    See also
    --------
    :doc:`/reference/model/transport/data`
    """
    context = c.graph["context"]

    import message_ix_models.project.ssp.data  # noqa: F401
    from message_ix_models.project.ssp import SSP_2017, SSP_2024
    from message_ix_models.tools.exo_data import prepare_computer

    from . import data  # noqa: F401

    # Added keys
    keys = {}

    source = str(context.transport.ssp)

    # Identify appropriate source keyword arguments for loading GDP and population data
    if context.transport.ssp in SSP_2017:
        source_kw = (
            dict(measure="GDP", model="IIASA GDP"),
            dict(measure="POP", model="IIASA GDP"),
        )
    elif context.transport.ssp in SSP_2024:
        source_kw = (dict(measure="GDP", model="IIASA GDP 2023"), dict(measure="POP"))

    for kw in source_kw:
        keys[kw["measure"]] = prepare_computer(
            context, c, source, source_kw=kw, strict=False
        )

    # Add data for MERtoPPP
    prepare_computer(
        context,
        c,
        "message_data.model.transport",
        source_kw=dict(measure="MERtoPPP", context=context),
        strict=False,
    )

    # Alias for other computations which expect the upper-case name
    c.add("GDP:n-y", "gdp:n-y")
    c.add("MERtoPPP:n-y", "mertoppp:n-y")

    # Ensure correct units
    c.add("population:n-y", "mul", "pop:n-y", Quantity(1.0, units="passenger"))

    # Dummy prices
    c.add("PRICE_COMMODITY:n-c-y", "dummy_prices", keys["GDP"][0], sums=True)


def prepare_computer(c: Computer) -> None:
    """Prepare `rep` for calculating transport demand.

    Parameters
    ----------
    rep : Reporter
        Must contain the keys ``<GDP:n-y>``, ``<MERtoPPP:n-y>``.
    """
    # Keys to refer to quantities
    # Existing keys, either from Reporter.from_scenario() or .build.add_structure()
    # NB if not using `c` here, then the entire `queue` could be moved out of the
    #    function
    gdp = c.full_key("GDP")
    mer_to_ppp = c.full_key("MERtoPPP")
    price_full = cast(Key, c.full_key("PRICE_COMMODITY")).drop("h", "l")

    # Keys for new quantities
    pop_at = Key("population", "n y area_type".split())
    pop = pop_at.drop("area_type")
    cg = Key("cg share", "n y cg".split())
    gdp_ppp = Key("GDP", "ny", "PPP")
    gdp_ppp_cap = gdp_ppp + "capita"
    gdp_index = gdp_ppp_cap + "index"
    pdt_nyt = Key("pdt", "nyt")  # Total PDT shared out by mode
    pdt_cap = pdt_nyt.drop("t") + "capita"
    pdt_ny = pdt_nyt.drop("t") + "total"
    price_sel1 = price_full + "transport"
    price_sel0 = price_sel1 + "raw units"
    price = price_sel1 + "smooth"
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
        # Base share data
        ("base_shares", "base shares:n-t-y", n, "t::transport modes", y, "config"),
        # Population shares by area_type
        (pop_at, groups.urban_rural_shares, y, "config"),
        # Consumer group sizes
        # TODO ixmp is picky here when there is no separate argument to the callable;
        # fix.
        (cg, groups.cg_shares, pop_at, "context"),
        # PPP GDP, total and per capita
        ("product", gdp_ppp, gdp, mer_to_ppp),
        ("ratio", gdp_ppp_cap, gdp_ppp, pop),
        # GDP index
        ("y0", itemgetter(0), y),  # TODO move upstream to message_ix
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
            ("select", "ldv pdt factor:n-y", "pdt factor:n-y-t", dict(t=["LDV"])),
            dict(drop=True),
        ),
        ("product", pdt_nyt, pdt_nyt.add_tag("0"), "pdt factor:n-y-t"),
        # Per capita (for validation)
        ("ratio", "transport pdt:n-y-t:capita", pdt_nyt, pop),
        # LDV PDT only
        (("select", "ldv pdt:n-y:ref", pdt_nyt, dict(t=["LDV"])), dict(drop=True)),
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

    # Add the empty dict() of kwargs for items without
    # FIXME adjust add_queue to be more flexible
    _queue: List[Tuple[Tuple, Dict]] = []
    for elem in queue:
        if len(elem) == 2 and isinstance(elem[0], tuple) and isinstance(elem[1], dict):
            # NB could use a TypeGuard here, but Python 3.10 only
            _queue.append(elem)  # type: ignore[arg-type]
        else:
            _queue.append((elem, dict()))

    c.add_queue(_queue)
