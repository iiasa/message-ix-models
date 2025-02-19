"""Freight transport data."""

from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING

import genno
import numpy as np
import pandas as pd
from iam_units import registry
from message_ix import make_df

from message_ix_models.util import (
    broadcast,
    convert_units,
    make_matched_dfs,
    same_node,
    same_time,
)

from .util import has_input_commodity, wildcard

if TYPE_CHECKING:
    from sdmx.model.common import Code

    from message_ix_models.model.transport import Config

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

#: Shorthand for tags on keys
Fi = "::F+ixmp"


def prepare_computer(c: genno.Computer):
    from genno.core.attrseries import AttrSeries

    from .key import bcast_tcl, bcast_y, n, y

    to_add = []  # Keys for ixmp-structured data to add to the target scenario
    k = genno.KeySeq("F")  # Sequence of temporary keys for the present function

    ### Produce the full quantity for input efficiency

    # Add a technology dimension with certain labels to the energy intensity of VDT
    # NB "energy intensity of VDT" actually has dimension (n,) only
    t_F_ROAD = "t::transport F ROAD"
    c.add(k[0], AttrSeries.expand_dims, "energy intensity of VDT:n-y", t_F_ROAD)
    # Broadcast over dimensions (c, l, y, yv, ya)
    prev = c.add(k[1], "mul", k[0], bcast_tcl.input, bcast_y.model)
    # Convert input to MESSAGE data structure
    c.add(k[2], "as_message_df", prev, name="input", dims=DIMS, common=COMMON)

    # Convert units
    to_add.append(f"input{Fi}")
    c.add(to_add[-1], convert_units, k[2], "transport info")

    # Create base quantity for "output" parameter
    # TODO Combine in a loop with "input", above—similar to .ldv
    k_output = genno.KeySeq("F output")
    nty = tuple("nty")
    c.add(k_output[0] * nty, wildcard(1.0, "dimensionless", nty))
    for i, coords in enumerate(["n::ex world", "t::F", "y::model"]):
        c.add(
            k_output[i + 1] * nty,
            "broadcast_wildcard",
            k_output[i] * nty,
            coords,
            dim=coords[0],
        )

    for par_name, base, ks, i in (("output", k_output[3] * nty, k_output, 3),):
        # Produce the full quantity for input/output efficiency
        prev = c.add(ks[i + 1], "mul", ks[i], getattr(bcast_tcl, par_name), bcast_y.all)

        # Convert to ixmp/MESSAGEix-structured pd.DataFrame
        # NB quote() is necessary with dask 2024.11.0, not with earlier versions
        c.add(ks[i + 2], "as_message_df", prev, name=par_name, dims=DIMS, common=COMMON)

        # Convert to target units
        to_add.append(f"output{Fi}")
        c.add(to_add[-1], convert_units, ks[i + 2], "transport info")

    # Extract the 'output' data frame
    c.add(k[3], lambda d: d["output"], to_add[-1])

    # Produce corresponding capacity_factor and technical_lifetime
    c.add(
        k[4],
        partial(
            make_matched_dfs,
            capacity_factor=registry.Quantity("1"),
            technical_lifetime=registry("10 year"),
        ),
        k[3],
    )

    # Convert to target units
    c.add(k[5], convert_units, k[4], "transport info")

    # Fill values
    to_add.append(f"other{Fi}")
    c.add(to_add[-1], same_node, k[5])

    # Base values for conversion technologies
    prev = c.add("F usage output:t:base", "freight_usage_output", "context")
    # Broadcast from (t,) to (t, c, l) dimensions
    prev = c.add(k[6], "mul", prev, bcast_tcl.output)

    # Broadcast over the (n, yv, ya) dimensions
    d = tuple("tcl") + tuple("ny")
    prev = c.add(k[7] * d, "expand_dims", prev, dim=dict(n=["*"], y=["*"]))
    prev = c.add(k[8] * d, "broadcast_wildcard", prev, "n::ex world", dim="n")
    prev = c.add(k[9] * d, "broadcast_wildcard", prev, "y::model", dim="y")
    prev = c.add(k[10] * (d + ("ya", "yv")), "mul", prev, bcast_y.no_vintage)

    # Convert output to MESSAGE data structure
    c.add(k[11], "as_message_df", prev, name="output", dims=DIMS, common=COMMON)
    to_add.append(f"usage output{Fi}")
    c.add(to_add[-1], lambda v: same_time(same_node(v)), k[11])

    # Create corresponding input values in Gv km
    prev = c.add(k[12], wildcard(1.0, "gigavehicle km", tuple("nty")))
    for i, coords in enumerate(["n::ex world", "t::F usage", "y::model"], start=12):
        prev = c.add(k[i + 1], "broadcast_wildcard", k[i], coords, dim=coords[0])
    prev = c.add(k[i + 2], "mul", prev, bcast_tcl.input, bcast_y.no_vintage)
    prev = c.add(
        k[i + 3], "as_message_df", prev, name="input", dims=DIMS, common=COMMON
    )
    to_add.append(f"usage input{Fi}")
    c.add(to_add[-1], prev)

    # Constraint data
    k_constraint = f"constraints{Fi}"
    to_add.append(k_constraint)
    c.add(k_constraint, constraint_data, "t::transport", n, y, "config")

    # Merge data to one collection
    k_all = f"transport{Fi}"
    c.add(k_all, "merge_data", *to_add)

    # Append to the "add transport data" key
    c.add("transport_data", __name__, key=k_all)


def constraint_data(
    t_all, nodes, years: list[int], genno_config: dict
) -> dict[str, pd.DataFrame]:
    """Return constraints on growth of ACT and CAP_NEW for non-LDV technologies.

    Responds to the :attr:`.Config.constraint` keys :py:`"non-LDV *"`; see description
    there.
    """
    config: "Config" = genno_config["transport"]

    # Freight modes
    modes = ["F ROAD", "F RAIL"]

    # Sets of technologies to constrain
    # All technologies under the non-LDV modes
    t_0: set["Code"] = set(filter(lambda t: t.parent and t.parent.id in modes, t_all))
    # Only the technologies that input c=electr
    t_1: set["Code"] = set(
        filter(partial(has_input_commodity, commodity="electr"), t_0)
    )
    # Only the technologies that input c=gas
    t_2: set["Code"] = set(filter(partial(has_input_commodity, commodity="gas"), t_0))

    assert all(len(t) for t in (t_0, t_1, t_2)), "Technology groups are empty"

    common = dict(year_act=years, year_vtg=years, time="year", unit="-")
    dfs = defaultdict(list)

    # Iterate over:
    # 1. Parameter name
    # 2. Set of technologies to be constrained.
    # 3. A fixed value, if any, to be used.
    for name, techs, fixed_value in (
        # These 2 entries set:
        # - 0 for the t_1 (c=electr) technologies
        # - The value from config for all others
        ("growth_activity_lo", list(t_0 - t_1), np.nan),
        ("growth_activity_lo", list(t_1), 0.0),
        # This 1 entry sets the value from config for all technologies
        # ("growth_activity_lo", t_0, np.nan),
        # This entry sets the value from config for certain technologies
        ("growth_activity_up", list(t_1 | t_2), np.nan),
        # For this parameter, no differentiation
        ("growth_new_capacity_up", list(t_0), np.nan),
    ):
        # Use the fixed_value, if any, or a value from configuration
        value = np.nan_to_num(fixed_value, nan=config.constraint[f"non-LDV {name}"])

        # Assemble the data
        dfs[name].append(
            make_df(name, value=value, **common).pipe(
                broadcast, node_loc=nodes, technology=techs
            )
        )

        # Add initial_* values corresponding to growth_{activity,new_capacity}_up, to
        # set the starting point of dynamic constraints.
        if name.endswith("_up"):
            name_init = name.replace("growth", "initial")
            value = config.constraint[f"non-LDV {name_init}"]
            for n, df in make_matched_dfs(dfs[name][-1], **{name_init: value}).items():
                dfs[n].append(df)

    result = {k: pd.concat(v) for k, v in dfs.items()}

    assert not any(v.isna().any(axis=None) for v in result.values()), "Missing labels"

    return result
