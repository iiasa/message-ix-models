"""Demand calculation for MESSAGEix-Transport."""

import logging
from typing import TYPE_CHECKING

import genno
import numpy as np
import pandas as pd
from dask.core import literal
from genno import Computer, KeySeq
from message_ix import make_df

from message_ix_models.util import broadcast

from . import files as exo
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

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from .config import Config

log = logging.getLogger(__name__)


def dummy(
    commodities: list, nodes: list[str], y: list[int], config: dict
) -> dict[str, pd.DataFrame]:
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


# Common keyword args to as_message_df()
_DEMAND_KW = dict(
    name="demand",
    dims=dict(commodity="c", node="n", year="y"),
    common=dict(level="useful", time="year"),
)

#: Task for computing and adding demand data; inputs to :meth:`.Computer.add_queue`.
TASKS = [
    # Values based on configuration
    # Disabled for #551
    # (("speed:t", "quantity_from_config", "config"), dict(name="speeds")),
    (("whour:", "quantity_from_config", "config"), dict(name="work_hours")),
    (("lambda:", "quantity_from_config", "config"), dict(name="lamda")),
    (("y::conv", "quantity_from_config", "config"), dict(name="year_convergence")),
    # Base passenger mode share (exogenous/reference data)
    (ms + "base", "base_shares", "mode share:n-t:exo", n, t_modes, y),
    # GDP expressed in PPP. The in the SSP(2024) input files, this conversion is already
    # applied, so no need to multiply by a mer_to_ppp factor here → simple alias.
    (gdp_ppp, gdp),
    # GDP PPP per capita
    (gdp_cap, "div", gdp_ppp, pop),
    #
    # Total PDT (n, y) = product of PDT / capita and population. See pdt_per_capita()
    # that sets up the calculation of `pdt_cap + "adj"`
    (pdt_ny, "mul", pdt_cap + "adj", pop),
    # Value-of-time multiplier
    # ("votm:n-y", "votm", gdp_cap + "adj"),
    # use the original GDP path for votm calculations
    ("votm:n-y", "votm", gdp_cap),
    # Select only the price of transport services
    # FIXME should be the full set of prices
    ((price_sel0, "select", price_full), dict(indexers=dict(c="transport"), drop=True)),
    (price_sel1, "price_units", price_sel0),
    # Smooth prices to avoid zig-zag in share projections
    (price, "smooth", price_sel1),
    # Interpolate speed data
    (
        ("speed:scenario-n-t-y:0", "interpolate", exo.speed, "y::coords"),
        dict(kwargs=dict(fill_value="extrapolate")),
    ),
    # Select speed data
    ("speed:n-t-y", "select", "speed:scenario-n-t-y:0", "indexers:scenario"),
    # Cost of transport (n, t, y)
    (cost, "cost", price, gdp_cap, "whour:", "speed:n-t-y", "votm:n-y", y),
    # Share weights (n, t, y)
    (
        sw,
        "share_weight",
        ms + "base",
        gdp_cap,
        cost,
        "lambda:",
        t_modes,
        y,
        "config",
    ),
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
    ("fv:n-t:historical", "mul", exo.mode_share_freight, exo.activity_freight),
    # …indexed to base-year values
    (gdp_index, "index_to", gdp_ppp, literal("y"), "y0"),
    (fv + "0", "mul", "fv:n-t:historical", gdp_index),
    # Scenario-specific adjustment factor for freight activity
    ("fv factor:n-t-y", "factor_fv", n, y, "config"),
    # Apply the adjustment factor
    (fv + "1", "mul", fv + "0", "fv factor:n-t-y"),
    # Select only the ROAD data. NB Do not drop so 't' labels can be used for 'c', next.
    ((fv + "2", "select", fv + "1"), dict(indexers=dict(t=["ROAD"]))),
    # Relabel
    ((fv_cny, "relabel2", fv + "2"), dict(new_dims={"c": "transport F {t}"})),
    # Convert to ixmp format
    (("t demand freight::ixmp", "as_message_df", fv_cny), _DEMAND_KW),
    # Select only non-LDV PDT
    ((pdt_nyt + "1", "select", pdt_nyt), dict(indexers=dict(t=["LDV"]), inverse=True)),
    # Relabel PDT
    (
        (pdt_cny + "0", "relabel2", pdt_nyt + "1"),
        dict(new_dims={"c": "transport pax {t.lower()}"}),
    ),
    (pdt_cny, "convert_units", pdt_cny + "0", "Gp km / a"),
    # Convert to ixmp format
    (("t demand pax non-ldv::ixmp", "as_message_df", pdt_cny), _DEMAND_KW),
    # Relabel ldv pdt:n-y-cg
    ((ldv_cny + "0", "relabel2", ldv_nycg), dict(new_dims={"c": "transport pax {cg}"})),
    (ldv_cny, "convert_units", ldv_cny + "0", "Gp km / a"),
    (("t demand pax ldv::ixmp", "as_message_df", ldv_cny), _DEMAND_KW),
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


def pdt_per_capita(c: Computer) -> None:
    """Set up calculation of :data:`~.key.pdt_cap`.

    Per Schäfer et al. (2009) Figure 2.5: linear interpolation between log GDP PPP per
    capita and log PDT per capita, specifically between the observed (GDP, PDT) point in
    |y0| and (:attr:`.Config.fixed_GDP`, :attr:`.Config.fixed_pdt`), which give a future
    “fixed point” towards which all regions converge.

    Values from the file :file:`pdt-elasticity.csv` are selected according to
    :attr:`.Config.ssp <.transport.config.Config.ssp>` and used to scale the difference
    between projected, log GDP in each future period and the log GDP in the reference
    year.
    """
    from . import key

    gdp = KeySeq(key.gdp)
    pdt = KeySeq("_pdt:n-y")

    # GDP expressed in PPP. In the SSP(2024) input files, this conversion is already
    # applied, so no need to multiply by a mer_to_ppp factor here → simple alias.
    c.add(gdp["PPP"], gdp.base)

    # GDP PPP per capita
    c.add(gdp["capita"], "div", gdp["PPP"], pop)

    # Add `y` dimension. Here for the future fixed point we use y=2 * max(y), e.g.
    # 4220 for y=2110. The value doesn't matter, we just need to avoid overlap with y
    # in the model.
    def _future(qty: "AnyQuantity", years: list[int]) -> "AnyQuantity":
        return qty.expand_dims(y=[years[-1] * 2])

    # Same, but adding y0
    c.add(pdt["ref"], lambda q, y: q.expand_dims(y=[y]), "pdt:n:capita+ref", "y0")

    def _delta(qty: "AnyQuantity", y0: int) -> "AnyQuantity":
        """Compute slope of `qty` between the last |y|-index and `y0`."""
        ym1 = sorted(qty.coords["y"].values)[-1]
        return qty.sel(y=ym1) - qty.sel(y=y0)

    # Same transformation for both quantities
    for x, reference_values in ((gdp, gdp["capita"]), (pdt, pdt["ref"])):
        # Retrieve value from configuration
        k = KeySeq(f"{x.name}::fixed")
        c.add(k[0], "quantity_from_config", "config", name=f"fixed_{x.name.strip('_')}")
        # Broadcast on `n` dimension
        c.add(k[1] * "n", "mul", k[0], "n:n:ex world")
        # Add dimension y=4220 (see _future, above)
        c.add(x["fixed"], _future, k[1] * "n", "y")
        # Concatenate with reference values
        # TODO Ensure units are consistent
        c.add(x["ext"], "concat", reference_values, x["fixed"])
        # Log X
        c.add(x["log"], np.log, x["ext"])
        # Log X indexed to values at y=y0. By construction the values for y=y0 are 1.0.
        c.add(x[0], "index_to", x["log"], literal("y"), "y0")
        # Delta log X minus y=y0 value. By construction the values for y=y0 are 0.0.
        c.add(x[1], "sub", x[0], genno.Quantity(1.0))
        # Difference between the fixed point and y0 values
        # TODO Maybe simplify this. Isn't the slope equal to the fixed-point values
        #      by construction?
        c.add(x["delta"] / "y", _delta, x[1], "y0")

    # Compute slope of PDT w.r.t. GDP after transformation
    c.add("pdt slope:n", "div", pdt["delta"] / "y", gdp["delta"] / "y")

    # Select 'elasticity' from "pdt elasticity:scenario-n:exo"
    c.add("pdt elasticity:n", "select", exo.pdt_elasticity, "indexers:scenario")

    # Adjust GDP by multiplying by 'elasticity'
    c.add(gdp[2], "mul", gdp[1], "pdt elasticity:n")

    # Projected PDT = m × adjusted GDP
    c.add(pdt["proj"], "mul", gdp[2], "pdt slope:n")

    # Reverse the transform for the adjusted GDP and projected PDT
    # TODO Derive `units` the inputs, __ and pdt["ref"]
    for x, start, units in (
        (gdp, gdp[2], "kUSD_2017 / passenger / year"),
        (pdt, pdt["proj"], "km/year"),
    ):
        # Reverse transform
        c.add(x[3], "add", start, genno.Quantity(1.0))
        c.add("y::y0", lambda v: dict(y=v), "y0")
        c.add(x["log"] + "y0", "select", x["log"], "y::y0")
        c.add(x[4], "mul", x[3], x["log"] + "y0")
        c.add(x[5], np.exp, x[4])
        c.add(x[6], "assign_units", x[5], units=units)

    # Alias the last step to the target key
    c.add(pdt_cap, pdt[6])

    # Provide a key for the adjusted GDP
    c.add(gdp_cap + "adj", gdp[6])


def prepare_computer(c: Computer) -> None:
    """Prepare `c` to calculate and add transport demand data.

    See also
    --------
    TASKS
    """
    from . import factor

    config: "Config" = c.graph["context"].transport

    if config.project.get("LED", False):
        # Select from the file input
        c.add(pdt_cap, "select", exo.pdt_cap_proj, indexers=dict(scenario="LED"))
    else:
        c.apply(pdt_per_capita)

    # Insert a scaling factor that varies according to SSP setting
    c.apply(factor.insert, pdt_cap, name="pdt non-active", target=pdt_cap + "adj")

    c.add_queue(TASKS)
    c.add("transport_data", __name__, key="transport demand::ixmp")
