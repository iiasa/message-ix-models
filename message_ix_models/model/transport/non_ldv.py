"""Data for transport modes and technologies outside of LDVs."""
import logging
from functools import partial
from operator import itemgetter
from typing import Dict, List, Mapping

import pandas as pd
from genno import Computer, Key, Quantity
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    merge_data,
    same_node,
    same_time,
)
from sdmx.model.v21 import Code

from .emission import ef_for_input
from .util import path_fallback

log = logging.getLogger(__name__)


#: Target units for data produced for non-LDV technologies.
#:
#: .. todo: this should be read from general model configuration.
UNITS = dict(
    # Appearing in input file
    inv_cost="GUSD_2010 / (Gv km)",  # Gv km of CAP
    fix_cost="GUSD_2010 / (Gv km)",  # Gv km of CAP
    var_cost="GUSD_2010 / (Gv km)",  # Gv km of ACT
    technical_lifetime="a",
    input="1.0 GWa / (Gv km)",
    output="Gv km",
    capacity_factor="",
)


def prepare_computer(c: Computer):
    source = c.graph["context"].transport.data_source.non_LDV
    log.info(f"non-LDV data from {source}")

    # Load the load-factor data
    k_lf = c.add(
        "load_file",
        path_fallback(c.graph["context"].model.regions, "load-factor-nonldv.csv"),
        key="load factor:t:nonldv",
        dims=RENAME_DIMS,
        name="load factor",
    )

    keys = []

    if source == "IKARUS":
        keys.append("transport nonldv::ixmp+ikarus")
    elif source is None:
        pass  # Don't add any data
    else:
        raise ValueError(f"Unknown source for non-LDV data: {source!r}")

    # Dummy/placeholder data for 2-wheelers (not present in IKARUS)
    keys.append(c.add("transport 2W::ixmp", get_2w_dummies, "context"))

    # Compute CO₂ emissions factors
    for k in map(Key.from_str_or_key, list(keys[:-1])):
        key = c.add(k.add_tag("input"), itemgetter("input"), k)
        keys.append(
            c.add(
                k.add_tag("emi"), partial(ef_for_input, species="CO2"), "context", key
            )
        )

    # Data for usage technologies
    keys.append(
        c.add(
            "transport nonldv usage::ixmp",
            usage_data,
            k_lf,
            "t::transport modes",
            "n::ex world",
            "y::model",
        )
    )

    return c.add("merge_data", "transport nonldv::ixmp", *keys)


def get_2w_dummies(context) -> Dict[str, pd.DataFrame]:
    """Generate dummy, equal-cost output for 2-wheeler technologies.

    **NB** this is analogous to :func:`.ldv.get_dummy`.
    """
    # Information about the target structure
    info = context["transport build info"]

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))

    # List of 2-wheeler technologies
    all_techs = context.transport.set["technology"]["add"]
    techs = list(map(str, all_techs[all_techs.index("2W")].child))

    # 'output' parameter values: all 1.0 (ACT units == output units)
    # - Broadcast across nodes.
    # - Broadcast across LDV technologies.
    # - Add commodity ID based on technology ID.
    output = (
        make_df(
            "output",
            value=1.0,
            commodity="transport vehicle 2w",
            year_act=years,
            year_vtg=years,
            unit="Gv * km",
            level="useful",
            mode="all",
            time="year",
            time_dest="year",
        )
        .pipe(broadcast, node_loc=info.N[1:], technology=techs)
        .pipe(same_node)
    )

    # Add matching data for 'capacity_factor' and 'var_cost'
    data = make_matched_dfs(output, capacity_factor=1.0, var_cost=1.0)
    data["output"] = output

    return data


def usage_data(
    load_factor: Quantity, modes: List[Code], nodes: List[str], years: List[int]
) -> Mapping[str, pd.DataFrame]:
    """Generate data for non-LDV usage "virtual" technologies.

    These technologies convert commodities like "transport vehicle rail" (i.e.
    vehicle-distance traveled) into "transport pax rail" (i.e. passenger-distance
    traveled), through use of a load factor in the ``output`` efficiency.

    They are "virtual" in the sense they have no cost, lifetime, or other physical
    properties.
    """
    common = dict(year_vtg=years, year_act=years, mode="all", time="year")

    data = []
    for mode in filter(lambda m: m != "LDV", modes):
        data.append(
            make_io(
                src=(f"transport vehicle {mode.lower()}", "useful", "Gv km"),
                dest=(f"transport pax {mode.lower()}", "useful", "Gp km"),
                efficiency=load_factor.sel(t=mode.upper()).item(),
                on="output",
                technology=f"transport {mode.lower()} usage",
                # Other data
                **common,
            )
        )

    result = dict()
    merge_data(result, *data)

    for k, v in result.items():
        result[k] = v.pipe(broadcast, node_loc=nodes).pipe(same_node).pipe(same_time)

    return result
