"""Demand calculation for MESSAGEix-Transport."""
import logging
from typing import Dict, List

import numpy as np
import pandas as pd
from dask.core import literal, quote
from genno import Computer, Key
from message_ix import make_df
from message_ix_models.util import broadcast

log = logging.getLogger(__name__)

# Keys to refer to quantities
# Existing keys, either from Reporter.from_scenario() or .build.add_structure()
gdp = Key("GDP:n-y")
mer_to_ppp = Key("MERtoPPP:n-y")
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")
price_full = PRICE_COMMODITY.drop("h", "l")

# Keys for new quantities
pop_at = Key("population", "n y area_type".split())
pop = pop_at.drop("area_type")
cg = Key("cg share", "n y cg".split())
gdp_ppp = gdp + "PPP"
gdp_ppp_cap = gdp_ppp + "capita"
gdp_index = gdp_ppp_cap + "index"
pdt_nyt = Key("pdt", "nyt")  # Total PDT shared out by mode
pdt_cap = pdt_nyt.drop("t") + "capita"
pdt_ny = pdt_nyt.drop("t") + "total"
pdt_cny = Key("pdt", "cny")  # With 'c' instead of 't' dimension, for demand
ldv_ny = Key("pdt ldv", "ny")
ldv_nycg = Key("pdt ldv") * cg
ldv_cny = Key("pdt ldv", "cny")
fv = Key("freight activity", "nty")
fv_cny = Key("freight activity", "cny")
price_sel1 = price_full + "transport"
price_sel0 = price_sel1 + "raw units"
price = price_sel1 + "smooth"
cost = Key("cost", "nyct")
sw = Key("share weight", "nty")

n = "n::ex world"
t_modes = "t::transport modes"
y = "y::model"


def dummy(
    commodities: List, nodes: List[str], y: List[int], config: dict
) -> Dict[str, pd.DataFrame]:
    """Dummy demands.


    Parameters
    ----------
    info : .ScenarioInfo
    """
    if not config["transport"].dummy_demand:
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


# Common positional args to as_message_df
_demand_common = (
    "demand",
    dict(commodity="c", node="n", year="y"),
    dict(level="useful", time="year"),
)

#: Task for computing and adding demand data; inputs to :meth:`.Computer.add_queue`.
TASKS = [
    # Values based on configuration
    ("speed:t", "quantity_from_config", "config", quote("speeds")),
    ("whour:", "quantity_from_config", "config", quote("work_hours")),
    ("lambda:", "quantity_from_config", "config", quote("lamda")),
    # Base passenger mode share
    ("mode share:n-t-y:base", "base_shares", "mode share:n-t:ref", n, t_modes, y),
    # PPP GDP, total and per capita
    (gdp_ppp, "mul", gdp, mer_to_ppp),
    (gdp_ppp_cap, "div", gdp_ppp, pop),
    # GDP index
    (gdp_index, "index_to", gdp_ppp_cap, literal("y"), "y0"),
    # Projected PDT per capita
    (pdt_cap, "pdt_per_capita", gdp_ppp_cap, (pdt_cap / "y") + "ref", "y0", "config"),
    # Total PDT
    (pdt_ny, "mul", pdt_cap, pop),
    # Value-of-time multiplier
    ("votm:n-y", "votm", gdp_ppp_cap),
    # Select only the price of transport services
    # FIXME should be the full set of prices
    ((price_sel0, "select", price_full), dict(indexers=dict(c="transport"), drop=True)),
    (price_sel1, "price_units", price_sel0),
    # Smooth prices to avoid zig-zag in share projections
    (price, "smooth", price_sel1),
    # Transport costs by mode
    (cost, "cost", price, gdp_ppp_cap, "whour:", "speed:t", "votm:n-y", y),
    # Share weights
    (
        sw,
        "share_weight",
        "mode share:n-t-y:base",
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
    (("mode share:n-t-y", "logit", cost, sw, "lambda:", y), dict(dim="t")),
    # Total PDT shared out by mode
    (pdt_nyt + "0", "mul", pdt_ny, "mode share:n-t-y"),
    # Adjustment factor
    ("pdt factor:n-y-t", "factor_pdt", n, y, t_modes, "config"),
    # Only the LDV values
    (
        ("ldv pdt factor:n-y", "select", "pdt factor:n-y-t", dict(t=["LDV"])),
        dict(drop=True),
    ),
    (pdt_nyt, "mul", pdt_nyt + "0", "pdt factor:n-y-t"),
    # Per capita (for validation)
    ("transport pdt:n-y-t:capita", "div", pdt_nyt, pop),
    # LDV PDT only
    ((ldv_ny + "ref", "select", pdt_nyt), dict(indexers=dict(t="LDV"), drop=True)),
    # Indexed to base year
    (ldv_ny + "index", "index_to", ldv_ny + "ref", literal("y"), "y0"),
    ("ldv pdt:n:advance", "advance_ldv_pdt", "config"),
    # Compute LDV PDT as ADVANCE base-year values indexed to overall growth
    (ldv_ny + "total+0", "mul", ldv_ny + "index", "ldv pdt:n:advance"),
    (ldv_ny + "total", "mul", ldv_ny + "total+0", "ldv pdt factor:n-y"),
    # LDV PDT shared out by consumer group
    (ldv_nycg, "mul", ldv_ny + "total", cg),
    #
    # Base freight mode share
    # …from IEA EEI
    # ("iea_eei_fv", "fv:n-y:historical", quote("tonne-kilometres"), "config"),
    # …from ADVANCE
    ("fv:n:advance", "advance_fv", "config"),
    # …from file
    (
        "fv:n-t:historical",
        "mul",
        "freight mode share:n-t:ref",
        "freight activity:n:ref",
    ),
    (fv + "0", "mul", "fv:n-t:historical", gdp_index),
    # Adjustment factor: generate and apply
    ("fv factor:n-t-y", "factor_fv", n, y, "config"),
    (fv + "1", "mul", fv + "0", "fv factor:n-t-y"),
    # Select only the ROAD data
    ((fv + "2", "select", fv + "1"), dict(indexers={"t": "ROAD"})),
    # Relabel
    (
        (fv_cny, "relabel2", fv + "2"),
        dict(new_dims={"c": "transport freight {t.lower()}"}),
    ),
    # Convert to ixmp format
    ("t demand freight::ixmp", "as_message_df", fv_cny, *_demand_common),
    # Select only non-LDV PDT
    ((pdt_nyt + "1", "select", pdt_nyt), dict(indexers=dict(t=["LDV"]), inverse=True)),
    # Relabel PDT
    (
        (pdt_cny + "0", "relabel2", pdt_nyt + "1"),
        dict(new_dims={"c": "transport pax {t.lower()}"}),
    ),
    (pdt_cny, "convert_units", pdt_cny + "0", "Gp km / a"),
    # Convert to ixmp format
    ("t demand pax non-ldv::ixmp", "as_message_df", pdt_cny, *_demand_common),
    # Relabel ldv pdt:n-y-cg
    ((ldv_cny + "0", "relabel2", ldv_nycg), dict(new_dims={"c": "transport pax {cg}"})),
    (ldv_cny, "convert_units", ldv_cny + "0", "Gp km / a"),
    ("t demand pax ldv::ixmp", "as_message_df", ldv_cny, *_demand_common),
    # Dummy demands, if these are configured
    ("t demand dummy::ixmp", dummy, "c::transport", "nodes::ex world", y, "config"),
    # Merge all data together
    (
        "transport demand::ixmp",
        "merge_data",
        "t demand pax ldv::ixmp",
        "t demand pax non-ldv::ixmp",
        "t demand freight::ixmp",
        "t demand dummy::ixmp",
    ),
]


def prepare_computer(c: Computer) -> None:
    """Prepare `c` to calculate and add transport demand data.

    See also
    --------
    TASKS
    """
    c.add_queue(TASKS)
    c.add("transport_data", __name__, key="transport demand::ixmp")
