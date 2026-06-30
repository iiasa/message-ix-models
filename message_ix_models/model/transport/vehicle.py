"""Operational parameters (capacity factor, technical lifetime) and stock of vehicles.

Some calculations for LDVs are more complex, and are handled in :mod:`.transport.ldv`.
"""

import logging
from typing import TYPE_CHECKING, Any

from genno import Key, Keys

from message_ix_models.util.genno import Collector

from . import key as K
from .util import COMMON, DIMS

if TYPE_CHECKING:
    from genno import Computer

    from message_ix_models.model.transport import Config

log = logging.getLogger(__name__)

#: Modes or groups of modes to handle in :func:`.prepare_computer`,
#: :func:`capacity_factor`, and :func:`stock`.
MODE = ["F", "P ex LDV", "LDV"]

# Shorthand
Vi = "vehicle+ixmp"

#: Target key that collects all data generated in this module.
TARGET = f"transport::{Vi}"


collect = Collector(TARGET, "{}+ixmp".format)


def prepare_computer(c: "Computer") -> None:
    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    context = c.graph["context"]
    techs = context.transport.spec.add.set["technology"]
    k = K.exo.activity_vehicle

    for mode in MODE:
        # Select only the "t" dimension coords according to `mode`
        mode_code = techs[techs.index(mode)]
        modes = ["LDV"] if mode == "LDV" else list(map(str, mode_code.child))

        # One of the sums is used in .disutility.prepare_computer()
        c.add(k[mode], "select", k, indexers={"t": modes}, sums=True)

        # Further operations based on k[mode]
        capacity_factor(c, mode)
        stock(c, mode)

    # Add data for MESSAGE parameter ``inv_cost``
    ic = "inv_cost"
    # Convert to MESSAGE data structure
    collect(
        f"{ic}::vehicle", "as_message_df", K.exo.inv_cost, name=ic, dims=DIMS, common={}
    )

    # Add data for MESSAGE parameter ``technical_lifetime``
    tl = "technical_lifetime"
    # Convert to MESSAGE data structure
    collect(
        f"{tl}::vehicle", "as_message_df", K.exo.lifetime, name=tl, dims=DIMS, common={}
    )

    # commented: handle data from stock-cap.csv; currently unused.
    # # total stock = stock per capita × total population
    # stock_total = exo.stock_cap - "cap"
    # c[stock_total] = "mul", exo.stock_cap, pop


def capacity_factor(c: "Computer", mode: str) -> None:
    """Add data for MESSAGE parameter ``capacity_factor``."""
    cf = "capacity_factor"
    k = Keys(
        cf=Key(cf, K.exo.activity_vehicle.dims, mode),
        bcast_y=f"broadcast:y-yv-ya:{cf}+{mode}",
        coords_y=f"coords:yv:{cf}+{mode}",
    )

    # Use only vintages from the maximum technical lifetime (1995)
    # This reduces the result size from 195k to about 90k observations
    c.add(k.coords_y, lambda years: dict(yv=[y for y in years if y >= 1995]), "y")
    c.add(k.bcast_y, "select", K.bcast_y.all, k.coords_y)

    # Expand from "t" modes to all actual technologies
    # TODO Move this into ActivityVehicle
    c.add(k.cf[0], "call", "t::transport map", K.exo.activity_vehicle[mode])

    # Broadcast y → (yV, yA)
    prev = c.add(k.cf[1], "mul", k.cf[0], k.bcast_y)

    # Convert to MESSAGE data structure
    dims = DIMS | dict(node_loc="n")
    collect(f"{cf}::{mode}", "as_message_df", prev, name=cf, dims=dims, common=COMMON)


#: 2-:class:`tuple` of keys for each :data:`MODE`:
#:
#: 1. Key for total service activity, dimensions (n, y) or (n, y, t).
#: 2. Key for load factor or occupancy.
STOCK_KEYS = {
    "F": (K.fv, K.exo.load_factor_f),
    "P ex LDV": (K.pdt_nyt, K.exo.load_factor_p),
    "LDV": (K.ldv_ny + "total", K.exo.load_factor_ldv),
}


def stock(c: "Computer", mode: str, *, margin: float = 0.2) -> None:
    """Prepare `c` to compute base-period stock and historical sales for `mode`.

    Parameters
    ----------
    mode :
        An element of :data:`MODE`.
    margin :
        Fractional margin by which to increase the resulting sales values. Because these
        values are used to compute ``historical_new_capacity`` and
        ``bound_new_capacity_{lo,up}``, this relaxes the resulting constraints on LDV
        technologies in the first model period.
    """
    context = c.graph["context"]
    config: "Config" = context.transport
    info = config.base_model_info

    k = Keys(
        stock=f"stock:n-t-y:{mode}",
        sales_nty=f"sales:n-t-y:{mode}",
        sales=f"sales:nl-t-yv:{mode}",
    )

    # Retrieve starting keys specific to `mode`
    k.total_activity, k.load_factor = STOCK_KEYS[mode]

    # - Divide total activity by (1) annual driving distance per vehicle and (2) load
    #   factor (occupancy) to obtain implied stock.
    # - Correct units: "load factor ldv:n-y" is dimensionless, should be
    #   passenger/vehicle
    # - Select only the base-period value.
    c.add(k.stock[0], "div", k.total_activity, K.exo.activity_vehicle[mode])
    c.add(k.stock[1], "div", k.stock[0], k.load_factor)
    c.add(k.stock[2] / "y", "select", k.stock[1], "y0::coord", sums=True)

    if mode == "LDV":
        # Multiply by exogenous technology shares to obtain stock with (n, t) dimensions
        c.add(k.stock, "mul", k.stock[2] / ("t", "y"), K.exo.t_share_ldv)

        # Age of vehicles as of the model base period
        k.age = K.exo.age_ldv

        # Subset of values from ldv-new-capacity.csv for 1 period after the model base
        # period (e.g. 2025 for 2020 base period)
        c.add(k.sales["exo"], "select", K.exo.cap_new_ldv, K.coord.yv_1plus)
    else:
        # Total stock: no data flow for exogenous technology shares
        c.add(k.stock, k.stock[2])

        k.age = K.exo.lifetime

        # Remainder not yet implemented for non-LDV
        return

    # Fraction of sales in preceding years (annual, not MESSAGE 'year' referring to
    # multi-year periods)
    c.add(k.sales_nty[0], "sales_fraction_annual", k.age)
    # Absolute sales in preceding years
    c.add(k.sales_nty[1], "mul", k.stock, k.sales_nty[0], 1.0 + margin)
    # Aggregate to model periods; total sales across the period
    c.add(k.sales_nty[2], "aggregate", k.sales_nty[1], K.y_.annual_agg, keep=False)
    # Divide by duration_period for the equivalent of CAP_NEW/historical_new_capacity
    c.add(k.sales_nty, "div", k.sales_nty[2], "duration_period:y")

    # Rename dimensions to match those expected in prepare_computer(), above
    c.add(k.sales, "rename_dims", k.sales_nty, name_dict={"n": "nl", "y": "yv"})

    # Convert units
    c.add(k.sales[0], "convert_units", k.sales, units="million * vehicle / year")

    # historical_new_capacity: select only data prior to y₀
    kw: dict[str, Any] = dict(
        common={},
        dims=dict(node_loc="nl", technology="t", year_vtg="yv"),
        name="historical_new_capacity",
    )
    c.add(k.sales[1], "select", k.sales[0], K.coord.yv_hist)
    collect(f"{kw['name']}::{mode}", "as_message_df", k.sales[1], **kw)

    # CAP_NEW/bound_new_capacity_{lo,up}
    # - Select only data from y₀ and later.
    # - Discard values for ICE_conv.
    #   TODO Do not hard code this label; instead, identify the technology with the
    #   largest share and avoid setting constraints on it.
    # - Add both upper and lower constraints to ensure the solution contains exactly
    #   the given value.
    # - Concatenate data from ldv-new-capacity.csv
    c.add(k.sales[2], "select", k.sales[0], indexers=dict(yv=info.Y))
    indexers = dict(t=["ICE_conv"])
    c.add(k.sales[3], "select", k.sales[2], indexers=indexers, inverse=True)
    c.add(k.sales[4], "concat", k.sales[3], k.sales["exo"])

    for kw["name"] in map("bound_new_capacity_{}".format, ("lo", "up")):
        collect(f"{kw['name']}::{mode}", "as_message_df", k.sales[4], **kw)
