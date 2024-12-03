"""Freight transport data."""

from functools import partial

import genno
from iam_units import registry

from message_ix_models.util import convert_units, make_matched_dfs, same_node, same_time

from .util import wildcard

COMMON = dict(
    mode="all",
    time="year",
    time_dest="year",
    time_origin="year",
)
DIMS = dict(
    node_loc="n",
    node_dest="n",
    node_origin="n",
    year_vtg="yv",
    year_act="ya",
    technology="t",
    commodity="c",
    level="l",
)


def prepare_computer(c: genno.Computer):
    from genno.core.attrseries import AttrSeries

    to_add = []  # Keys for ixmp-structured data to add to the target scenario
    k = genno.KeySeq("F")  # Sequence of temporary keys for the present function

    ### Produce the full quantity for input efficiency

    # Add a technology dimension with certain labels to the energy intensity of VDT
    # NB "energy intensity of VDT" actually has dimension (n,) only
    t_F_ROAD = "t::transport F ROAD"
    c.add(k[0], AttrSeries.expand_dims, "energy intensity of VDT:n-y", t_F_ROAD)
    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(
        k[1], "mul", k[0], "broadcast:t-c-l:transport+input", "broadcast:y-yv-ya"
    )
    # Convert input to MESSAGE data structure
    c.add(k[2], "as_message_df", prev, name="input", dims=DIMS, common=COMMON)

    # Convert units
    to_add.append("input::transport F+ixmp")
    c.add(to_add[-1], convert_units, k[2], "transport info")

    # Extract the 'input' data frame
    c.add(k[3], lambda d: d["input"], to_add[-1])

    # Produce corresponding output, capacity_factor, technical_lifetime
    # FIXME Use "… F RAIL …" as appropriate
    c.add(
        k[4],
        partial(
            make_matched_dfs,
            output=dict(
                value=registry("1.0 gigatonne km"),
                commodity="transport F ROAD vehicle",
                level="useful",
                time_dest=COMMON["time_dest"],
            ),
            capacity_factor=registry.Quantity("1"),
            technical_lifetime=registry("10 year"),
        ),
        k[3],
    )

    # Convert to target units
    c.add(k[5], convert_units, k[4], "transport info")

    # Fill values
    to_add.append("other::transport F+ixmp")
    c.add(to_add[-1], same_node, k[5])

    # Base values for conversion technologies
    prev = c.add("F usage output:t:base", "freight_usage_output", "context")
    # Broadcast from (t,) to (t, c, l) dimensions
    prev = c.add(k[6], "mul", prev, "broadcast:t-c-l:transport+output")

    # Broadcast over the (n, yv, ya) dimensions
    dim = dict(n=["*"], y=[None], ya=[None], yv=[None])
    prev = c.add(k[7], "expand_dims", prev, dim=dim)
    prev = c.add(k[8], "broadcast_wildcard", prev, "n", dim="n")
    prev = c.add(k[9], "broadcast", prev, "broadcast:y-yv-ya:no vintage")

    # Convert output to MESSAGE data structure
    c.add(k[10], "as_message_df", prev, name="output", dims=DIMS, common=COMMON)
    to_add.append("usage output::transport F+ixmp")
    c.add(to_add[-1], lambda v: same_time(same_node(v)), k[10])

    # Create corresponding input values in Gv km
    prev = c.add(k[11], wildcard(1.0, "gigavehicle km", tuple("nty")))
    for i, coords in enumerate(["n::ex world", "t::F usage", "y::model"], start=11):
        prev = c.add(k[i + 1], "broadcast_wildcard", k[i], coords, dim=coords[0])
    prev = c.add(
        k[i + 2],
        "mul",
        prev,
        "broadcast:t-c-l:transport+input",
        "broadcast:y-yv-ya:no vintage",
    )
    prev = c.add(
        k[i + 3], "as_message_df", prev, name="input", dims=DIMS, common=COMMON
    )
    to_add.append("usage input::transport F+ixmp")
    c.add(to_add[-1], prev)

    # Merge data to one collection
    k_all = "transport F::ixmp"
    c.add(k_all, "merge_data", *to_add)

    # Append to the "add transport data" key
    c.add("transport_data", __name__, key=k_all)
