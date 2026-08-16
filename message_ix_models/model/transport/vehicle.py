"""Operational parameters (capacity factor, technical lifetime) and stock of vehicles.

Some calculations for LDVs are more complex, and are handled in :mod:`.transport.ldv`.
"""

import logging
from operator import add
from typing import TYPE_CHECKING, Any

from genno import Key, Keys

from message_ix_models.util import convert_units
from message_ix_models.util.genno import Collector

from . import key as K
from . import util
from .util import COMMON, wildcard

if TYPE_CHECKING:
    from genno import Computer

    from message_ix_models import ScenarioInfo

log = logging.getLogger(__name__)

#: Mapping from :mod:`message_ix` parameter dimensions to source dimensions in some
#: quantities.
DIMS = util.DIMS | dict(node_loc="n", node_dest="n", node_origin="n")

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

    # Data for ``input`` and ``output``
    input_output(c)

    # Add data for MESSAGE parameter ``inv_cost``
    ic = "inv_cost"
    # Convert to MESSAGE data structure
    collect(
        f"{ic}::vehicle",
        "as_message_df",
        K.exo.inv_cost,
        name=ic,
        dims=util.DIMS,
        common={},
    )

    # Add data for MESSAGE parameter ``technical_lifetime``
    tl = "technical_lifetime"
    # Convert to MESSAGE data structure
    dims = DIMS | dict(year_vtg="y")
    collect(
        f"{tl}::vehicle", "as_message_df", K.exo.lifetime, name=tl, dims=dims, common={}
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

    # Expand from "t" modes to all actual technologies
    # TODO Move this into ActivityVehicle
    c.add(k.cf[0], "call", "t::transport map", K.exo.activity_vehicle[mode])

    # Broadcast y → (yV, yA)
    # prev = c.add(k.cf[1], "mul", k.cf[0], k.bcast_y) # Using limited vintages
    prev = c.add(k.cf[1], "mul", k.cf[0], K.bcast_y.all)  # Using all vintages

    # Convert to MESSAGE data structure
    collect(f"{cf}::{mode}", "as_message_df", prev, name=cf, dims=DIMS, common=COMMON)


def input_output(c: "Computer") -> None:
    """Prepare calculation of ``input``, ``output`` parameters for vehicle technologies.

    For the ``input`` parameter, this uses data from:

    - :class:`.InputVehicle` for "2W" and "F RAIL" technologies.
    - :class:`.IEA_Future_of_Trucks` for "F ROAD" technologies.
    """
    from .data import InputVehicle

    NTY = tuple("nty")
    ### `input`
    k = Key("input", NTY, "vehicle")

    # Concatenate data from (a) file (InputVehicle.key) and (b) IEA Future of Trucks
    c.add(k[0], "concat", InputVehicle.key, "energy intensity of VDT:n-t")

    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(k[1], "mul", k[0], K.bcast_tcl.input, K.bcast_y.all)

    # Convert to MESSAGE data structure; add to `target`
    collect(
        "input::vehicle", "as_message_df", prev, name="input", dims=DIMS, common=COMMON
    )

    ### `output`
    k = Key("output", NTY, "vehicle")

    # Create base quantity
    c.add(k[0], wildcard(1.0, "dimensionless", NTY))
    # Freight and P ex LDV technologies; omit LDV which are handled in .ldv
    c.add("t::vehicle", add, K.t["F"], K.t["P ex LDV"])
    # Broadcast over all nodes, technologies, and periods (including historical)
    c.add(k[1], "broadcast_wildcard", k[0], K.n, "t::vehicle", "y", dim=NTY)
    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(k[2], "mul", k[1], K.bcast_tcl.output, K.bcast_y.all)
    # Convert to MESSAGE data structure
    prev = c.add(k[3], "as_message_df", prev, name="output", dims=DIMS, common=COMMON)
    # Reduce entries to a diagonal band
    prev = c.add(k[4], "yv_ya_banded", prev, "y0", diff=30)
    # Convert units; add to `TARGET`
    # TODO convert_units appears to have no effect; check and adjust/remove
    collect("output::vehicle", convert_units, prev, "transport info")


#: 3-:class:`tuple` of keys for each :data:`MODE`:
#:
#: 1. Key for total service activity, dimensions (n, y) or (n, y, t).
#: 2. Key for load factor or occupancy.
#: 3. Key for average age of stock as of the initial period.
STOCK_KEYS = {
    "F": (K.fv, K.exo.load_factor_f, K.exo.lifetime),
    "P ex LDV": (K.pdt_nyt, K.exo.load_factor_p, K.exo.lifetime),
    "LDV": (K.ldv_ny + "total", K.exo.load_factor_ldv, K.exo.age_ldv),
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
    info: "ScenarioInfo" = c.graph["context"].transport.base_model_info

    k = Keys(
        stock=f"stock:n-t-y:{mode}",
        sales_nty=f"sales:n-t-y:{mode}",
        sales=f"sales:nl-t-yv:{mode}",
    )

    # Retrieve starting keys specific to `mode`
    # - Total activity
    # - Load factor
    # - Age of vehicles as of the model base period
    k.total_activity, k.load_factor, k.age = STOCK_KEYS[mode]

    # - Divide total activity by (1) annual driving distance per vehicle and (2) load
    #   factor (occupancy) to obtain implied stock.
    # - Correct units: "load factor ldv:n-y" is dimensionless, should be
    #   passenger/vehicle
    # - Replace t = {F RAIL, F ROAD, LDV, ...} with coords for individual techs.
    # - Select only the base-period value.
    # - Multiply by exogenous technology shares.
    c.add(k.stock[0], "div", k.total_activity, K.exo.activity_vehicle[mode])
    c.add(k.stock[1], "div", k.stock[0], k.load_factor)
    c.add(k.stock[2], "call", "t::transport map", k.stock[1])
    c.add(k.stock[3] / "y", "select", k.stock[2], "y0::coord", sums=True)
    c.add(k.stock, "mul", k.stock[3] / "y", K.exo.cap_share_t)

    # Select age values for y=y₀ only
    c.add(k.age["y0"], "select", k.age, K.coord.y_0)
    # Fraction of sales in preceding years (annual, not MESSAGE 'year' referring to
    # multi-year periods)
    c.add(k.sales_nty[0], "sales_fraction_annual", k.age["y0"])
    # Absolute sales in preceding years
    c.add(k.sales_nty[1], "mul", k.stock, k.sales_nty[0])
    # Aggregate to model periods; total sales across the period
    c.add(k.sales_nty[2], "aggregate", k.sales_nty[1], K.agg.y_annual, keep=False)
    # Divide by duration_period for the equivalent of CAP_NEW/historical_new_capacity
    c.add(k.sales_nty, "div", k.sales_nty[2], "duration_period:y")

    # Rename dimensions to match those expected in prepare_computer(), above
    c.add(k.sales, "rename_dims", k.sales_nty, name_dict={"n": "nl", "y": "yv"})

    # Convert units
    c.add(k.sales[0], "convert_units", k.sales, units="million * vehicle / year")

    # historical_new_capacity: select only data prior to y₀
    kw: dict[str, Any] = dict(common={}, dims=util.DIMS, name="historical_new_capacity")
    # Select only data prior to y₀
    c.add(k.sales[1], "select", k.sales[0], K.coord.yv_hist)
    # Adjust for MESSAGE representation of historical capacity.
    # - sales_fraction_annual(), above, assigns some historical new capacity to vintages
    #   that are beyond the technical_lifetime. For example, some vintages may be >20
    #   years old such that the mean age is e.g. 15 years.
    # - MESSAGE considers these older vintages past end-of-life and not usable in y₀,
    #   thus would choose a larger CAP_NEW for a feasible solution.
    # - We apply a fixed correction factor to obtain a smooth(er) transition from
    #   historical_new_capacity to CAP_NEW.
    c.add(k.sales[2], "mul", k.sales[1], 1.5)
    collect(f"{kw['name']}::{mode}", "as_message_df", k.sales[2], **kw)

    # CAP_NEW/bound_new_capacity_{lo,up}
    # - Select only data from y₀ and later.
    # - Discard values for dominant technologies.
    #   TODO Do not hard code these labels; instead, identify the technologies with the
    #   largest share.
    c.add(k.sales[3], "select", k.sales[0], indexers=dict(yv=info.Y))
    indexers = dict(t=["FR_ICH", "ICE_H_moto", "ICE_conv", "f rail lightoil"])
    c.add(k.sales[4], "select", k.sales[3], indexers=indexers, inverse=True)

    # Subset of values from new-capacity.csv: 1 period after the model initial period
    # (e.g. 2025 for 2020 initial period)
    c.add(k.sales["exo+0"], "select", K.exo.cap_new, K.coord.yv_1plus)
    c.add(k.sales["exo"], "select", k.sales["exo+0"], K.coord.t[mode])

    # Concatenate to previous
    c.add(k.sales[5], "concat", k.sales[4], k.sales["exo"])

    # Add both upper and lower constraints to ensure the solution contains the given
    # values.
    for kind in ("lo", "up"):
        kw["name"] = f"bound_new_capacity_{kind}"
        factor = 1 + (-1 if kind == "lo" else 1) * margin
        c.add(k.sales[kind], "mul", k.sales[5], factor)
        collect(f"{kw['name']}::{mode}", "as_message_df", k.sales[kind], **kw)
