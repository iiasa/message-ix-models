"""Freight transport data."""

from functools import partial
from operator import itemgetter
from typing import TYPE_CHECKING

import genno
import numpy as np
from genno import Key, literal
from iam_units import registry

from message_ix_models.report.key import GDP
from message_ix_models.util import convert_units, make_matched_dfs, same_node, same_time
from message_ix_models.util.genno import Collector

from . import util
from .demand import _DEMAND_KW
from .key import bcast_tcl, bcast_y, exo, fv, fv_cny, n, y
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
    c.add("fv:n-t:historical", "mul", exo.mode_share_freight, exo.activity_freight)
    c.add(fv["log y0"], np.log, "fv:n-t:historical")

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
    k_e = Key(exo.elasticity_f.name, "ny", "F")

    # Broadcast elasticity to all scenarios
    coords = ["scenario::all", "n::ex world"]
    c.add(
        k_e[0], "broadcast_wildcard", exo.elasticity_f, *coords, dim=("scenario", "n")
    )

    # Select values for the current scenario
    c.add(k_e[1], "select", k_e[0], "indexers:scenario:LED")

    # Interpolate on "y" dimension
    c.add(k_e[2], "interpolate", k_e[1], "y::coords", **EXTRAPOLATE)

    ###

    # Adjust GDP by multiplying by 'elasticity'
    c.add(gdp[2], "mul", gdp[1], k_e[2])

    # Projected delta log freight activity is exactly the same
    c.add(fv[0], gdp[2])

    # Reverse the transformation
    c.add(fv[1], "add", fv[0], genno.Quantity(1.0))
    c.add(fv[2], "mul", fv[1], fv["log y0"])
    c.add(fv[3], np.exp, fv[2])

    # (NAVIGATE) Scenario-specific adjustment factor for freight activity
    c.add("fv factor:n-t-y", "factor_fv", n, y, "config")

    # Apply the adjustment factor
    c.add(fv[4], "mul", fv[3], "fv factor:n-t-y")

    # Select certain modes. NB Do not drop so 't' labels can be used for 'c', next.
    c.add(fv[5], "select", fv[4], indexers=dict(t=["RAIL", "ROAD"]))

    # Relabel
    c.add(fv_cny, "relabel2", fv[5], new_dims={"c": "transport F {t}"})

    # Convert to ixmp format
    collect("demand", "as_message_df", fv_cny, **_DEMAND_KW)

    # Compute indices, e.g. for use in .non_ldv.other()
    for t in ["RAIL", "ROAD"]:
        c.add(fv[5][t], "select", fv[5], indexers=dict(t=t))
        c.add(fv[f"{t} index"], "index_to", fv[5][t], literal("y"), "y0")


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` to calculate and add data for freight transport."""
    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    # Call further functions to set up tasks for categories of freight data
    tech_econ(c)
    usage(c)
    demand(c)


def tech_econ(c: "Computer") -> None:
    """Prepare calculation of technoeconomic parameters for freight technologies."""

    ### `input`
    k = Key("input", NTY, "F")

    # Add a technology dimension with certain labels to the energy intensity of VDT
    # NB "energy intensity of VDT" actually has dimension (n,) only
    t_F_ROAD = "t::transport F ROAD"
    c.add(k[0], "expand_dims", "energy intensity of VDT:n-y", t_F_ROAD)
    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(k[1], "mul", k[0], bcast_tcl.input, bcast_y.model)
    # Convert MESSAGE data structure
    c.add(k[2], "as_message_df", prev, name="input", dims=DIMS, common=COMMON)
    # Convert units; add to `TARGET`
    collect(k.name, convert_units, k[2], "transport info")

    ### `output`
    k = Key("output", NTY, "F")

    # Create base quantity
    c.add(k[0], wildcard(1.0, "dimensionless", NTY))
    coords = ["n::ex world", "t::F", "y::model"]
    c.add(k[1], "broadcast_wildcard", k[0], *coords, dim=NTY)
    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(k[2], "mul", k[1], bcast_tcl.output, bcast_y.all)
    # Convert to MESSAGE data structure
    prev = c.add(k[3], "as_message_df", prev, name="output", dims=DIMS, common=COMMON)
    # Convert units; add to `TARGET`
    k_output = collect(k.name, convert_units, prev, "transport info")

    ### `capacity_factor` and `technical_lifetime`
    k = Key("other::F")

    # Extract the 'output' data frame
    c.add(k[0], itemgetter("output"), k_output)

    # Produce corresponding capacity_factor and technical_lifetime
    c.add(
        k[1],
        partial(
            make_matched_dfs,
            capacity_factor=registry.Quantity("1"),
            technical_lifetime=registry("10 year"),
        ),
        k[0],
    )
    # Convert units
    collect(k.name, convert_units, k[1], "transport info")


def usage(c: "Computer") -> None:
    """Prepare calculation of 'usage' pseudo-technologies for freight activity."""
    ### `output`
    k = Key("F usage output:t")

    # Base values
    c.add(k[0], "freight_usage_output", "context")
    # Broadcast from (t,) → (t, c, l) dimensions
    prev = c.add(k[1], "mul", k[0], bcast_tcl.output)

    # Broadcast over the (n, yv, ya) dimensions
    d = bcast_tcl.output.dims + tuple("ny")
    prev = c.add(k[2] * d, "expand_dims", prev, dim=dict(n=["*"], y=["*"]))
    coords = ["n::ex world", "y::model"]
    prev = c.add(k[3] * d, "broadcast_wildcard", prev, *coords, dim=tuple("ny"))
    prev = c.add(k[4], "mul", prev, bcast_y.no_vintage)
    # Convert to MESSAGE data structure
    c.add(k[5], "as_message_df", prev, name="output", dims=DIMS, common=COMMON)
    # Fill node_dest, time_dest key values
    collect("usage output", lambda v: same_time(same_node(v)), k[5])

    ### `input`
    k = Key("F usage input", NTY)

    c.add(k[0], wildcard(1.0, "gigavehicle km", NTY))
    coords = ["n::ex world", "t::F usage", "y::model"]
    c.add(k[1], "broadcast_wildcard", k[0], *coords, dim=NTY)
    # Broadcast (t,) → (t, c, l) and (y,) → (yv, ya) dimensions
    prev = c.add(k[2], "mul", k[1], bcast_tcl.input, bcast_y.no_vintage)
    # Convert to MESSAGE data structure
    collect(
        "usage input", "as_message_df", prev, name="input", dims=DIMS, common=COMMON
    )
