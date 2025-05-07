"""Constraints on ``ACT`` and ``CAP_NEW`` of transport technologies."""

from typing import TYPE_CHECKING

from . import util

if TYPE_CHECKING:
    from genno import Computer


#: Common, fixed values for some dimensions of MESSAGE parameters.
COMMON = util.COMMON.copy()

#: Mapping from :mod:`message_ix` parameter dimensions to source dimensions in some
#: quantities.
DIMS = util.DIMS | dict(node_loc="n", year_vtg="y", year_act="y")

#: Constraint parameters handled by :func:`.prepare_computer`.
PARAM = (
    "initial_new_capacity_lo",
    "growth_new_capacity_lo",
    "bound_new_capacity_up",
    "initial_new_capacity_up",
    "growth_new_capacity_up",
    "initial_activity_lo",
    "growth_activity_lo",
    "initial_activity_up",
    "growth_activity_up",
)

#: Target key that collects all data generated in this module.
TARGET = "transport::constraint+ixmp"


def prepare_computer(c: "Computer") -> None:
    """Prepare calculation of constraint data for transport technologies.

    The data is processed from :any:`.data.transport.constraint_dynamic`.
    """
    from genno import Key, Keys

    from message_ix_models.util.genno import Collector

    from .key import bcast_tcl, exo

    collect = Collector(TARGET, "{}::constraint+ixmp".format)
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    k = Keys(a=Key("constraints", exo.constraint_dynamic.dims, "transport"))
    k.b = k.a * tuple("lny")

    # 't'echnology dimension: broadcast labels like "F ROAD" to full lists of techs
    c.add(k.a[0], "call", "t::transport map", exo.constraint_dynamic)

    # 'c'ommodity dimension: broadcast "*" values to full lists of transport commodities
    c.add(k.a[1], "call", "c::transport wildcard", k.a[0])

    # Keep only the (t, c) combinations which are actual inputs to specific transport
    # techs
    c.add(k.a[2] * "l", "mul", k.a[1], bcast_tcl.input)

    # Add and broadcast over:
    # - 'n'ode dimension including all nodes
    # - 'y'ear dimension including all model periods
    c.add(k.b[0], "expand_dims", k.a[2] * "l", dim=dict(n="*", y="*"))
    coords = ["n::ex world", "y::model"]
    c.add(k.b[1], "broadcast_wildcard", k.b[0], *coords, dim=tuple("ny"))

    # Iterate over each MESSAGE constraint parameter
    for name in PARAM:
        k.single_par, n = Key(name, k.b.dims, "transport") / "name", dict(name=name)
        # Select only this subset of data
        c.add(k.single_par, "select_allow_empty", k.b[1], indexers=n)
        # Convert to MESSAGE-format data frame
        collect(name, "as_message_df", k.single_par, **n, dims=DIMS, common=COMMON)
