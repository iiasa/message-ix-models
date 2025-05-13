"""Data for passenger transport modes and technologies, excepting LDVs."""

import logging
from collections.abc import Mapping
from functools import lru_cache
from typing import TYPE_CHECKING

import pandas as pd
from genno import Computer, Key, KeySeq, Quantity, quote
from message_ix import make_df
from sdmx.model.v21 import Code

from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    merge_data,
    package_data_path,
    same_node,
    same_time,
)
from message_ix_models.util.genno import Collector

from .key import exo

if TYPE_CHECKING:
    from message_ix_models import Context
    from message_ix_models.types import ParameterData

    from .config import Config

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

ENERGY_OTHER_HEADER = """2020 energy demand for OTHER transport

Source: Extracted from IEA EWEB, 2022 OECD edition

Units: TJ
"""

#: Shorthand for tags on keys.
Pi = "::P+ixmp"

#: Target key that collects all data generated in this module.
TARGET = f"transport{Pi}"

collect = Collector(TARGET, "{}::P+ixmp".format)


def prepare_computer(c: Computer):
    from .key import n, t_modes, y

    context: "Context" = c.graph["context"]

    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    source = context.transport.data_source.non_LDV
    log.info(f"non-LDV data from {source}")

    if source == "IKARUS":
        collect("ikarus", "transport nonldv::ixmp+ikarus")
    elif source is None:
        pass  # Don't add any data
    else:
        raise ValueError(f"Unknown source for non-LDV data: {source!r}")

    # Dummy/placeholder data for 2-wheelers (not present in IKARUS)
    collect("2W", get_2w_dummies, "context")

    # TODO add these steps within the above, using a utility function
    # # Compute CO₂ emissions factors
    # for k in map(Key, list(keys[:-1])):
    #     c.add(k + "input", itemgetter("input"), k)
    #     c.add(k + "emi", ef_for_input, "context", k + "input", species="CO2")
    #     keys.append(k + "emi")

    # Data for usage pseudo-technologies
    collect("usage", usage_data, exo.load_factor_nonldv, t_modes, n, y)

    #### NB lines below duplicated from .transport.base
    e_iea = Key("energy:n-y-product-flow:iea")
    e_fnp = KeySeq(e_iea.drop("y"))
    e = KeySeq("energy:commodity-flow-node_loc:iea")

    # Transform IEA EWEB data for comparison

    c.add(e_fnp[0], "select", e_iea, indexers=dict(y=2020), drop=True)
    c.add(e_fnp[1], "aggregate", e_fnp[0], "groups::iea to transport", keep=False)
    c.add(
        e[0],
        "rename_dims",
        e_fnp[1],
        quote(dict(n="node_loc", product="commodity")),
        sums=True,
    )
    ####
    c.add(e[1] / "flow", "select", e[0], indexers=dict(flow="OTHER"), drop=True)
    path = package_data_path("transport", context.regions, "energy-other.csv")
    kw = dict(header_comment=ENERGY_OTHER_HEADER)
    c.add("energy other csv", "write_report", e[1] / "flow", path=path, kwargs=kw)

    # Handle data from the file energy-other.csv

    # Add minimum activity for transport technologies
    c.apply(bound_activity_lo)

    # Add other constraints on activity of non-LDV technologies
    bound_activity(c)


def get_2w_dummies(context) -> "ParameterData":
    """Generate dummy, equal-cost output for 2-wheeler technologies.

    **NB** this is analogous to :func:`.ldv.get_dummy`.
    """
    # Information about the target structure
    config: "Config" = context.transport
    info = config.base_model_info

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))

    # List of 2-wheeler technologies
    all_techs = config.spec.add.set["technology"]
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


def bound_activity(c: "Computer") -> None:
    """Constrain activity of non-LDV technologies based on :file:`act-non_ldv.csv`."""
    base = exo.act_non_ldv

    # Produce MESSAGE parameters bound_activity_{lo,up}:nl-t-ya-m-h
    kw = dict(
        dims=dict(node_loc="n", technology="t", year_act="y"),
        common=dict(mode="all", time="year"),
    )
    k_bau = Key(f"bound_activity_up{Pi}")
    collect(k_bau.name, "as_message_df", base, name=k_bau.name, **kw)


def bound_activity_lo(c: Computer) -> None:
    """Set minimum activity for certain technologies to ensure |y0| energy use.

    Responds to values in :attr:`.Config.minimum_activity`.
    """

    @lru_cache
    def techs_for(mode: Code, commodity: str) -> list[Code]:
        """Return techs that are (a) associated with `mode` and (b) use `commodity`."""
        result = []
        for t in mode.child:
            if input_info := t.eval_annotation(id="input"):
                if input_info["commodity"] == commodity:
                    result.append(t.id)
        return result

    def _(nodes, technologies, y0, config: dict) -> Quantity:
        """Quantity with dimensions (c, n, t, y), values from `config`."""
        # Extract MESSAGEix-Transport configuration
        cfg: "Config" = config["transport"]

        # Construct a set of all (node, technology, commodity) to constrain
        rows: list[list] = []
        cols = ["n", "t", "c", "value"]
        for (n, modes, c), value in cfg.minimum_activity.items():
            for m in ["2W", "BUS", "F ROAD"] if modes == "ROAD" else ["RAIL"]:
                m_idx = technologies.index(m)
                rows.extend([n, t, c, value] for t in techs_for(technologies[m_idx], c))

        # Assign y and value; convert to Quantity
        return Quantity(
            pd.DataFrame(rows, columns=cols)
            .assign(y=y0)
            .set_index(cols[:3] + ["y"])["value"],
            units="GWa",
        )

    k = KeySeq("bound_activity_lo:n-t-y:transport minimum")
    c.add(next(k), _, "n::ex world", "t::transport", "y0", "config")

    # Produce MESSAGE parameter bound_activity_lo:nl-t-ya-m-h
    kw = dict(
        dims=dict(node_loc="n", technology="t", year_act="y"),
        common=dict(mode="all", time="year"),
    )

    collect(k.name, "as_message_df", k[0], name=k.name, **kw)


def usage_data(
    load_factor: Quantity, modes: list[Code], nodes: list[str], years: list[int]
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
    for mode in filter(lambda m: m != "LDV", map(str, modes)):
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

    result: dict[str, pd.DataFrame] = dict()
    merge_data(result, *data)

    for k, v in result.items():
        result[k] = v.pipe(broadcast, node_loc=nodes).pipe(same_node).pipe(same_time)

    return result
