"""Demand calculation for MESSAGEix-Transport."""

import logging
from typing import TYPE_CHECKING

import genno
import numpy as np
import pandas as pd
from genno import Key, literal
from message_ix import make_df

from message_ix_models.report.key import GDP
from message_ix_models.util import broadcast

from . import factor
from .key import (
    cg,
    cost,
    exo,
    gdp_cap,
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
    sw,
    t_modes,
    y,
)
from .util import EXTRAPOLATE

if TYPE_CHECKING:
    from genno import Computer
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
    (gdp_ppp, GDP),
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
    ((price[1], "select", price[0]), dict(indexers=dict(c="transport"), drop=True)),
    (price[2], "price_units", price[1]),
    # Smooth prices to avoid zig-zag in share projections
    (price.base, "smooth", price[2]),
    # Interpolate speed data
    (("speed:scenario-n-t-y:0", "interpolate", exo.speed, "y::coords"), EXTRAPOLATE),
    # Select speed data
    ("speed:n-t-y", "select", "speed:scenario-n-t-y:0", "indexers:scenario"),
    # Cost of transport (n, t, y)
    (cost, "cost", price.base, gdp_cap, "whour:", "speed:n-t-y", "votm:n-y", y),
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
    (pdt_nyt[0], "mul", pdt_ny, ms),
    # Scenario-specific adjustment factors
    ("pdt factor:n-y-t", "factor_pdt", n, y, t_modes, "config"),
    # Only the LDV values
    (
        ("ldv pdt factor:n-y", "select", "pdt factor:n-y-t"),
        dict(indexers=dict(t="LDV"), drop=True),
    ),
    (pdt_nyt, "mul", pdt_nyt[0], "pdt factor:n-y-t"),
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
    # Select only non-LDV PDT
    ((pdt_nyt[1], "select", pdt_nyt), dict(indexers=dict(t=["LDV"]), inverse=True)),
    # Relabel PDT
    (
        (pdt_cny[0], "relabel2", pdt_nyt[1]),
        dict(new_dims={"c": "transport pax {t.lower()}"}),
    ),
    (pdt_cny, "convert_units", pdt_cny[0], "Gp km / a"),
    # Convert to ixmp format
    (("demand::P+ixmp", "as_message_df", pdt_cny), _DEMAND_KW),
    # Relabel ldv pdt:n-y-cg
    ((ldv_cny[0], "relabel2", ldv_nycg), dict(new_dims={"c": "transport pax {cg}"})),
    (ldv_cny, "convert_units", ldv_cny[0], "Gp km / a"),
    (("demand::LDV+ixmp", "as_message_df", ldv_cny), _DEMAND_KW),
    # Dummy demands, if these are configured
    ("demand::dummy+ixmp", dummy, "c::transport", "nodes::ex world", y, "config"),
    # Merge all data together
    (
        "transport demand::ixmp",
        "merge_data",
        "demand::LDV+ixmp",
        "demand::P+ixmp",
        "demand::dummy+ixmp",
    ),
]


def pdt_per_capita(c: "Computer") -> None:
    """Set up calculation of :data:`~.key.pdt_cap`.

    Per Schäfer et al. (2009) Figure 2.5: linear interpolation between log GDP PPP per
    capita and log PDT per capita, specifically between the observed (GDP, PDT) point in
    |y0| and (:attr:`.Config.fixed_GDP`, :attr:`.Config.fixed_pdt`), which give a future
    “fixed point” towards which all regions converge.

    Values from the file :data:`.elasticity_p` are selected according to
    :attr:`.Config.ssp <.transport.config.Config.ssp>` and used to scale the difference
    between projected, log GDP in each future period and the log GDP in the reference
    year.
    """
    gdp = Key(GDP)
    pdt = Key("_pdt:n-y")

    # GDP expressed in PPP. In the SSP(2024) input files, this conversion is already
    # applied, so no need to multiply by a mer_to_ppp factor here → simple alias.
    c.add(gdp["PPP"], gdp)

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
        k = Key(x.name, (), "fixed")
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

    # Select 'elasticity' from "elasticity:scenario-n-y:P+exo"
    k_e = genno.Key(exo.elasticity_p.name, "ny")
    c.add(k_e[0], "select", exo.elasticity_p, "indexers:scenario")

    # Interpolate on "y" dimension
    c.add(k_e[1], "interpolate", k_e[0], "y::coords", **EXTRAPOLATE)

    # Adjust GDP by multiplying by 'elasticity'
    c.add(gdp[2], "mul", gdp[1], k_e[1])

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


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` to calculate and add transport demand data.

    See also
    --------
    TASKS
    """
    config: "Config" = c.graph["context"].transport

    # Compute total PDT per capita
    c.apply(pdt_per_capita)

    # Insert a scaling factor that varies according to SSP setting
    c.apply(factor.insert, pdt_cap, name="pdt non-active", target=pdt_cap + "adj")

    # Add other tasks for demand calculation
    c.add_queue(TASKS)

    if config.project.get("LED", False):
        # Replace certain calculations for LED projected activity

        # Select data from input file: projected PDT per capita
        c.add(pdt_cap * "t", "select", exo.pdt_cap_proj, indexers=dict(scenario="LED"))

        # Multiply by population for the total
        c.add(pdt_nyt[0], "mul", pdt_cap * "t", pop)

    c.add("transport_data", __name__, key="transport demand::ixmp")
