"""Demand calculation for MESSAGEix-Transport."""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd
from dask.core import literal
from genno import Computer
from message_ix import make_df
from message_ix_models.util import broadcast

from .key import (
    cg,
    cost,
    fv,
    fv_cny,
    gdp,
    gdp_cap,
    gdp_index,
    gdp_ppp,
    ldv_cny,
    ldv_ny,
    ldv_nycg,
    ms,
    n,
    pdt_cap,
    pdt_cny,
    pdt_ny,
    pdt_nyt,
    pop,
    price,
    price_full,
    price_sel0,
    price_sel1,
    sw,
    t_modes,
    y,
)

log = logging.getLogger(__name__)


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
    (("speed:t", "quantity_from_config", "config"), dict(name="speeds")),
    (("whour:", "quantity_from_config", "config"), dict(name="work_hours")),
    (("lambda:", "quantity_from_config", "config"), dict(name="lamda")),
    (("y::conv", "quantity_from_config", "config"), dict(name="year_convergence")),
    # Base passenger mode share (exogenous/reference data)
    (ms + "base", "base_shares", "mode share:n-t:ref", n, t_modes, y),
    # GDP expressed in PPP. The in the SSP(2024) input files, this conversion is already
    # applied, so no need to multiply by a mer_to_ppp factor here → simple alias.
    (gdp_ppp, gdp),
    # GDP PPP per capita
    (gdp_cap, "div", gdp_ppp, pop),
    # …indexed to base-year values
    (gdp_index, "index_to", gdp_cap, literal("y"), "y0"),
    # Projected passenger-distance travelled (PDT) per capita
    (pdt_cap, "pdt_per_capita", gdp_cap, (pdt_cap / "y") + "ref", "y0", "config"),
    # Total PDT (n, y) = product of PDT / capita and population
    (pdt_ny, "mul", pdt_cap + "adj", pop),
    # Value-of-time multiplier
    ("votm:n-y", "votm", gdp_cap),
    # Select only the price of transport services
    # FIXME should be the full set of prices
    ((price_sel0, "select", price_full), dict(indexers=dict(c="transport"), drop=True)),
    (price_sel1, "price_units", price_sel0),
    # Smooth prices to avoid zig-zag in share projections
    (price, "smooth", price_sel1),
    # Cost of transport (n, t, y)
    (cost, "cost", price, gdp_cap, "whour:", "speed:t", "votm:n-y", y),
    # Share weights (n, t, y)
    (sw, "share_weight", ms + "base", gdp_cap, cost, "lambda:", t_modes, y, "config"),
    # Mode shares
    ((ms, "logit", cost, sw, "lambda:", y), dict(dim="t")),
    # Total PDT (n, t, y), with modes for the 't' dimension
    (pdt_nyt + "0", "mul", pdt_ny, ms),
    # Scenario-specific adjustment factors
    ("pdt factor:n-y-t", "factor_pdt", n, y, t_modes, "config"),
    # Only the LDV values
    (
        ("ldv pdt factor:n-y", "select", "pdt factor:n-y-t"),
        dict(indexers=dict(t="LDV"), drop=True),
    ),
    (pdt_nyt, "mul", pdt_nyt + "0", "pdt factor:n-y-t"),
    # Per capita (for validation)
    (pdt_nyt + "capita+post", "div", pdt_nyt, pop),
    # LDV PDT only (n, y)
    ((ldv_ny + "ref", "select", pdt_nyt), dict(indexers=dict(t="LDV"), drop=True)),
    # commented: The following computes LDV PDT as base-year values from the ADVANCE
    # database × an index of the top-down (Schäfer) LDV PDT versus base-year values
    # # Indexed to base year
    # (ldv_ny + "index", "index_to", ldv_ny + "ref", literal("y"), "y0"),
    # # Compute LDV PDT as ADVANCE base-year values indexed to overall growth
    # (ldv_ny + "total+0", "mul", ldv_ny + "index", "pdt ldv:n:advance"),
    # # Apply the scenario-specific adjustment factor
    # (ldv_ny + "total", "mul", ldv_ny + "total+0", "ldv pdt factor:n-y"),
    #
    # Apply the scenario-specific adjustment factor
    (ldv_ny + "total", "mul", ldv_ny + "ref", "ldv pdt factor:n-y"),
    # LDV PDT shared out by consumer group (cg, n, y)
    (ldv_nycg, "mul", ldv_ny + "total", cg),
    #
    # # Base freight activity from IEA EEI
    # ("iea_eei_fv", "fv:n-y:historical", quote("tonne-kilometres"), "config"),
    # Base year freight activity from file (n, t), with modes for the 't' dimension
    (
        "fv:n-t:historical",
        "mul",
        "freight mode share:n-t:ref",
        "freight activity:n:ref",
    ),
    (fv + "0", "mul", "fv:n-t:historical", gdp_index),
    # Scenario-specific adjustment factor for freight activity
    ("fv factor:n-t-y", "factor_fv", n, y, "config"),
    # Apply the adjustment factor
    (fv + "1", "mul", fv + "0", "fv factor:n-t-y"),
    # Select only the ROAD data. NB Do not drop so 't' labels can be used for 'c', next.
    ((fv + "2", "select", fv + "1"), dict(indexers=dict(t=["ROAD"]))),
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
    from . import factor

    # NB It is necessary to pre-add this key because Computer.apply() errors otherwise
    c.add_single(pdt_cap, None)
    # Insert a scaling factor that varies according to SSP setting
    c.apply(factor.insert, pdt_cap, name="pdt non-active", target=pdt_cap + "adj")

    c.add_queue(TASKS)
    c.add("transport_data", __name__, key="transport demand::ixmp")
