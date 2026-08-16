"""Freight transport data."""

from typing import TYPE_CHECKING

import genno
import numpy as np
from genno import Key, literal, quote

from message_ix_models.report.key import GDP
from message_ix_models.util.genno import Collector

from . import key as K
from . import util
from .demand import _DEMAND_KW
from .util import COMMON, EXTRAPOLATE, wildcard

if TYPE_CHECKING:
    from genno import Computer


#: Mapping from :mod:`message_ix` parameter dimensions to source dimensions in some
#: quantities.
DIMS = util.DIMS | dict(node_loc="n", node_dest="n", node_origin="n")

NTY = tuple("nty")

#: Target key that collects all data generated in this module.
TARGET = "transport::F+ixmp"


collect = Collector(TARGET, "{}::F+ixmp".format)


def demand(c: "Computer") -> None:
    """Prepare calculation of freight activity/``demand``."""
    # commented: Base freight activity from IEA EEI
    # c.add("iea_eei_fv", "fv:n-y:historical", quote("tonne-kilometres"), "config")
    # Base year freight activity from file (n, t), with modes for the 't' dimension
    c.add("fv:n-t:historical", "mul", K.exo.mode_share_freight, K.exo.activity_freight)
    c.add(K.fv["log y0"], np.log, "fv:n-t:historical")

    ### Apply pseudo 'elasticity' of freight activity

    # Log GDP(PPP). NB This is the total value, in contrast to
    # .transport.demand.pdt_per_capita(), which manipulates per-capita values.
    gdp = GDP + "F"
    c.add(gdp["log"], np.log, GDP)

    # Log GDP indexed to values at y=y0. By construction the values for y=y0 are 1.0.
    c.add(gdp[0], "index_to", gdp["log"], literal("y"), "y0")

    # Delta log GDP minus y=y0 value. By construction the values for y=y0 are 0.0.
    c.add(gdp[1], "sub", gdp[0], genno.Quantity(1.0))

    ### Prepare exo.elasticity_f
    k_e = Key(K.exo.elasticity_f.name, "ny", "F")

    # Broadcast elasticity to all (node, technology, scenario)
    # TODO Move scenario broadcasting/selection into a .data.ExoDataSource subclass
    freight_modes = ["F RAIL", "F ROAD"]
    coords = ["scenario::all", K.n, quote(freight_modes)]
    dim = ("scenario", "n", "t")
    c.add(k_e[0], "broadcast_wildcard", K.exo.elasticity_f, *coords, dim=dim)

    # Select values for the current scenario
    c.add(k_e[1], "select", k_e[0], K.coord.scenario_label_A)

    # Interpolate on "y" dimension
    c.add(k_e[2], "interpolate", k_e[1], "y::coords", **EXTRAPOLATE)

    ###

    # Adjust GDP by multiplying by 'elasticity'
    c.add(gdp[2], "mul", gdp[1], k_e[2])

    # Projected delta log freight activity is exactly the same
    c.add(K.fv[0], gdp[2])

    # Reverse the transformation
    c.add(K.fv[1], "add", K.fv[0], genno.Quantity(1.0))
    c.add(K.fv[2], "mul", K.fv[1], K.fv["log y0"])
    c.add(K.fv[3], np.exp, K.fv[2])

    # (NAVIGATE) Scenario-specific adjustment factor for freight activity
    c.add("fv factor:n-t-y", "factor_fv", K.n, K.y, "config")

    # Apply the adjustment factor
    c.add(K.fv[4], "mul", K.fv[3], "fv factor:n-t-y")

    # Select certain modes. NB Do not drop so 't' labels can be used for 'c', next.
    c.add(K.fv, "select", K.fv[4], indexers=dict(t=freight_modes))

    # Relabel
    c.add(K.fv_cny, "relabel2", K.fv, new_dims={"c": "transport {t}"})

    # Convert to ixmp format
    collect("demand", "as_message_df", K.fv_cny, **_DEMAND_KW)

    # Compute indices, e.g. for use in .other.prepare_computer()
    for t in freight_modes:
        c.add(K.fv[t], "select", K.fv, indexers=dict(t=t))
        c.add(K.fv[f"{t} index"], "index_to", K.fv[t], literal("y"), "y0")


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` to calculate and add data for freight transport."""
    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    # Call further functions to set up tasks for categories of freight data
    usage(c)
    demand(c)


def usage(c: "Computer") -> None:
    """Prepare calculation of 'usage' pseudo-technologies for freight activity."""
    u = "usage "  # Shorthand for collect()

    # Output intensity
    k = Key("output", NTY, "F usage")

    # Relabel values from load-factor-f.csv
    # TODO Retrieve freight mode names/construct labels from K.t["F usage"]
    labels = {"t": {mode: f"transport {mode} usage" for mode in ("F RAIL", "F ROAD")}}
    c.add(k[0], "relabel", K.exo.load_factor_f, labels=labels)

    # Overwrite original units (tonne / vehicle) with output units (Gt km)
    c.add(k[1], "assign_units", k[0], units="Gt km")

    # - Broadcast from (t,) over the (c, l) (output commodity and level) dimensions.
    # - Broadcast from (y,) over the (yV, yA) (vintage and active year) dimensions.
    # Key `prev` includes the dimensions added by this operation.
    prev = c.add(k[2], "mul", k[1], K.bcast_tcl.output, K.bcast_y.no_vintage)

    # Convert to MESSAGE data structure
    collect(u + k.name, "as_message_df", prev, name=k.name, dims=DIMS, common=COMMON)

    # Input intensity
    k = Key("input", NTY, "F usage")

    # - Construct a quantity with value 1.0 and "*" for each dimension (n, t, y).
    # - Broadcast over all nodes, usage technologies, and model periods.
    # - Broadcast over the (c, l, yV, yA) dimensions, same as above.
    # - Convert to MESSAGE data structure.
    c.add(k[0], wildcard(1.0, "gigavehicle km", NTY))
    c.add(k[1], "broadcast_wildcard", k[0], K.n, K.t["F usage"], K.y, dim=NTY)
    prev = c.add(k[2], "mul", k[1], K.bcast_tcl.input, K.bcast_y.no_vintage)
    collect(u + k.name, "as_message_df", prev, name=k.name, dims=DIMS, common=COMMON)
