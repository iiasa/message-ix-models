"""Compute MESSAGEix-compatible input data for MESSAGEix-Transport."""
import logging
from collections import defaultdict
from functools import partial
from typing import Dict, List, Mapping

import pandas as pd
from genno import Computer, quote
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    add_par_data,
    broadcast,
    make_io,
    make_matched_dfs,
    make_source_tech,
    same_node,
)

from . import freight, non_ldv

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

#: Genno computations that generate model-ready data in ixmp format. This mapping is
#: extended by functions decorated with @provides_data() in this module.
DATA_FUNCTIONS = {
    # Keys added by demand.prepare_computer()
    "demand": ("transport demand passenger::ixmp",),
    "dummy demand": ("dummy demand::ixmp",),
    "freight demand": ("transport demand freight::ixmp",),
    # Key added by ldv.prepare_computer()
    "ldv": ("transport ldv::ixmp",),
    # Key added by ikarus.prepare_computer()
    # "non_ldv": ("transport nonldv::ixmp",),  # In progress
    # Data-generating functions in other modules
    "non_ldv": (non_ldv.get_non_ldv_data, "context"),
    "freight": (freight.get_freight_data, "n::ex world", "y::model", "context"),
}


def provides_data(*args):
    """Decorator that adds a function to :data:`DATA_FUNCTIONS`."""

    def decorator(f):
        DATA_FUNCTIONS[f.__name__] = tuple([f] + list(args))
        return f

    return decorator


def prepare_computer(c: Computer):
    """Populate `scenario` with MESSAGE-Transport data."""
    context = c.graph["context"]
    scenario = c.graph.get("scenario")

    # First strip existing emissions data
    strip_emissions_data(scenario, context)

    # Information about the base scenario
    info = context["transport build info"]

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")

    # Reference values: the Context, Scenario, ScenarioInfo, and dry_run parameter
    for key, value in dict(info=info, dry_run=context.dry_run).items():
        c.add(key, quote(value))

    # Data-generating calculations
    all_keys = []
    add = partial(add_par_data, dry_run=context.dry_run)
    for name, comp in DATA_FUNCTIONS.items():
        if len(comp) > 1:
            # Add 2 computations: one to generate the data
            k1 = c.add(f"{name}::ixmp", *comp)
        else:
            k1 = comp[0]
        # …one to add it to `scenario`
        all_keys.append(c.add(f"add {name}", add, "scenario", k1))

    # Add a key to trigger generating and adding all data
    key = c.add("add transport data", all_keys)

    # commented: extremely verbose
    # log.debug(c.describe("add transport data"))

    return key


def strip_emissions_data(scenario, context):
    """Remove base model's parametrization of freight transport emissions.

    They are re-added by :func:`get_freight_data`.
    """
    log.warning("Not implemented")
    pass


@provides_data("n::ex world", "y::model", "config")
def conversion(
    nodes: List[str], years: List[int], config: dict
) -> Dict[str, pd.DataFrame]:
    """Input and output data for conversion technologies:

    The technologies are named 'transport {service} load factor'.
    """
    common = dict(
        year_vtg=years,
        year_act=years,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    service_info = [
        ("freight", config["transport"].load_factor["freight"], "Gt km"),
        ("pax", 1.0, "Gp km / a"),
    ]

    data0: Mapping[str, List] = defaultdict(list)
    for service, factor, output_unit in service_info:
        i_o = make_io(
            (f"transport {service} vehicle", "useful", "Gv km"),
            (f"transport {service}", "useful", output_unit),
            factor,
            on="output",
            technology=f"transport {service} load factor",
            **common,
        )
        for par, df in i_o.items():
            data0[par].append(df.pipe(broadcast, node_loc=nodes).pipe(same_node))

    data1 = {par: pd.concat(dfs) for par, dfs in data0.items()}

    data1.update(
        make_matched_dfs(
            base=data1["input"],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data1


# @provides_data("info", "n::ex world", "y::model")
def misc(info: ScenarioInfo, nodes: List[str], y: List[int]):
    """Miscellaneous bounds for calibration/vetting."""

    # Limit activity of methanol LDVs in the model base year
    # TODO investigate the cause of the underlying behaviour; then remove this
    name = "bound_activity_up"
    data = {
        name: make_df(
            name,
            technology="ICAm_ptrp",
            year_act=y[0],
            mode="all",
            time="year",
            value=0.0,
            # unit=info.units_for("technology", "ICAm_ptrp"),
            unit="Gv km",
        ).pipe(broadcast, node_loc=nodes)
    }

    log.info("Miscellaneous bounds for calibration/vetting")
    return data


@provides_data("config", "info")
def dummy_supply(config, info) -> Dict[str, pd.DataFrame]:
    """Dummy fuel supply for the bare RES."""
    if not config["transport"].data_source.dummy_supply:
        return dict()

    # TODO read the list of 'commodity' from context/config
    commodities = ["electr", "gas", "hydrogen", "lightoil", "methanol"]

    # TODO separate dummy supplies by commodity
    data = make_source_tech(
        info,
        common=dict(
            level="final",  # TODO read the level from config
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
        try:
            data[par_name] = data[par_name].pipe(broadcast, commodity=commodities)
        except ValueError:
            pass  # No 'commodity' dimension

    return data
