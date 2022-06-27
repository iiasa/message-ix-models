"""Compute MESSAGEix-compatible input data for MESSAGEix-Transport."""
import logging
from collections import defaultdict
from functools import partial
from typing import Dict, List, Mapping

import pandas as pd
from dask.core import quote
from genno import Computer
from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    add_par_data,
    broadcast,
    make_io,
    make_matched_dfs,
    make_source_tech,
    same_node,
)

from . import freight, ldv, non_ldv

log = logging.getLogger(__name__)

#: CSV files containing data for input calculations and assumptions.
DATA_FILES = [
    ("demand-scale.csv",),
    ("ldv-class.csv",),
    ("mer-to-ppp.csv",),
    ("population-suburb-share.csv",),
    ("ma3t", "population.csv"),
    ("ma3t", "attitude.csv"),
    ("ma3t", "driver.csv"),
    ("mode-share", "default.csv"),
    ("mode-share", "A---.csv"),
]

#: Genno computations that generate model-ready data in ixmp format.
DATA_FUNCTIONS = {
    "ldv": (ldv.get_ldv_data, "context"),
    "non_ldv": (non_ldv.get_non_ldv_data, "context"),
    "freight": (freight.get_freight_data, "context"),
    "demand": ("demand::ixmp",),
}


def provides_data(*args):
    """Decorator that adds a function to :data:`DATA_FUNCTIONS`."""

    def decorator(f):
        DATA_FUNCTIONS[f.__name__] = tuple([f] + list(args))
        return f

    return decorator


def add_data(scenario, context, dry_run=False):
    """Populate `scenario` with MESSAGE-Transport data."""
    from message_data.model.transport import demand

    # First strip existing emissions data
    strip_emissions_data(scenario, context)

    # Information about the base `scenario`
    info = ScenarioInfo(scenario)
    context["transport build info"] = info

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")

    c = Computer()

    # Reference values: the Context, Scenario, ScenarioInfo, and dry_run parameter
    for key, value in dict(
        context=context,
        scenario=scenario,
        info=info,
        dry_run=dry_run,
    ).items():
        c.add(key, quote(value))

    # Data-generating calculations
    all_keys = []
    add = partial(add_par_data, dry_run=dry_run)
    for name, comp in DATA_FUNCTIONS.items():
        # Add 2 computations: one to generate the data
        k1 = c.add(f"{name}::ixmp", *comp)
        # …one to add it to `scenario`
        all_keys.append(c.add(f"add {name}", add, "scenario", k1))

    # Prepare demand calculations
    demand.prepare_reporter(c, context, exogenous_data=True, info=info)

    # Add a key to trigger generating and adding all data
    c.add("add transport data", all_keys)

    # commented: extremely verbose
    # log.debug(c.describe("add transport data"))

    # Actually add the data
    result = c.get("add transport data")
    log.info(f"Added {sum(result)} total obs")


def strip_emissions_data(scenario, context):
    """Remove base model's parametrization of freight transport emissions.

    They are re-added by :func:`get_freight_data`.
    """
    log.warning("Not implemented")
    pass


@provides_data("n::ex world", "y::model", "config")
def conversion(nodes: List[str], y: List[int], config: dict) -> Dict[str, pd.DataFrame]:
    """Input and output data for conversion technologies:

    The technologies are named 'transport {mode} load factor'.
    """
    common = dict(
        year_vtg=y,
        year_act=y,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    mode_info = [
        ("freight", config["transport"]["freight load factor"], "t km"),
        ("pax", 1.0, "km"),
    ]

    data: Mapping[str, List] = defaultdict(list)
    for mode, factor, output_unit in mode_info:
        i_o = make_io(
            (f"transport {mode} vehicle", "useful", "km"),
            (f"transport {mode}", "useful", output_unit),
            factor,
            on="output",
            technology=f"transport {mode} load factor",
            **common,
        )
        for par, df in i_o.items():
            data[par].append(df.pipe(broadcast, node_loc=nodes).pipe(same_node))

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data["input"],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data


@provides_data("info")
def dummy_supply(info) -> Dict[str, pd.DataFrame]:
@provides_data("context", "info")
def dummy_supply(context, info) -> Dict[str, pd.DataFrame]:
    """Dummy fuel supply for the bare RES."""
    # TODO read the 'level' from config
    # TODO read the list of 'commodity' from context/config
    # TODO separate dummy supplies by commodity

    if context["transport config"]["data source"]["dummy supply"] is not True:
        return dict()

    data = make_source_tech(
        info,
        common=dict(
            level="secondary",
            mode="all",
            technology="DUMMY transport fuel",
            time="year",
            time_dest="year",
            unit="GWa",
        ),
        output=1.0,
        var_cost=1.0,
    )

    # Broadcast across all fuel commodities
    for par_name in data:
        if "commodity" not in data[par_name].columns:
            continue

        data[par_name] = data[par_name].pipe(
            broadcast,
            commodity=["lightoil", "gas", "methanol", "hydrogen", "electr"],
        )

    return data
