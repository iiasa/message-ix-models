"""Generate input data."""
import logging
from collections import defaultdict

import pandas as pd
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

from .ldv import get_ldv_data
from .non_ldv import get_non_ldv_data
from .freight import get_freight_data

log = logging.getLogger(__name__)

DATA_FUNCTIONS = [
    get_ldv_data,
    get_non_ldv_data,
    get_freight_data,
]


def provides_data(f):
    """Decorator that adds a function to :data:`DATA_FUNCTIONS`."""
    DATA_FUNCTIONS.append(f)
    return f


def add_data(scenario, context, dry_run=False):
    """Populate `scenario` with MESSAGE-Transport data."""
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

    for func in DATA_FUNCTIONS:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        add_par_data(scenario, func(context), dry_run=dry_run)

    log.info("done")


def strip_emissions_data(scenario, context):
    """Remove base model's parametrization of freight transport emissions.

    They are re-added by :func:`get_freight_data`.
    """
    log.warning("Not implemented")
    pass


@provides_data
def demand(context):
    """Return transport demands.

    Parameters
    ----------
    context : .Context
    """
    from message_data.model.transport.demand import dummy, from_external_data

    config = context["transport config"]["data source"]

    if config.get("demand dummy", False):
        return dict(demand=dummy())

    # Retrieve a Reporter configured do to the calculation for the input data
    rep = from_external_data(context["transport build info"], context)

    # Generate the demand data; convert to pd.DataFrame
    pdt1 = rep.get("transport pdt:n-y-t")
    data = pdt1.to_series().reset_index(name="value")

    common = dict(
        level="useful",
        time="year",
        unit="km",  # TODO reduce this from the units of pdt1
    )

    # Convert to message_ix layout
    # TODO combine the two below in a loop or push the logic to demand.py
    data = make_df(
        "demand",
        node=data["n"],
        commodity="transport pax " + data["t"].str.lower(),
        year=data["y"],
        value=data["value"],
        **common,
    )
    data = data[~data["commodity"].str.contains("ldv")]

    data2 = rep.get("transport ldv pdt:n-y-cg").to_series().reset_index(name="value")

    data2 = make_df(
        "demand",
        node=data2["n"],
        commodity="transport pax " + data2["cg"],
        year=data2["y"],
        value=data2["value"],
        **common,
    )

    return dict(demand=pd.concat([data, data2]))


@provides_data
def conversion(context):
    """Input and output data for conversion technologies:

    The technologies are named 'transport {mode} load factor'.
    """
    cfg = context["transport config"]
    info = context["transport build info"]

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    mode_info = [
        ("freight", cfg["factor"]["freight load"], "t km"),
        ("pax", 1.0, "km"),
    ]

    data = defaultdict(list)
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
            data[par].append(df.pipe(broadcast, node_loc=info.N[1:]).pipe(same_node))

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data["input"],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data


@provides_data
def dummy_supply(context):
    """Dummy fuel supply for the bare RES."""
    # TODO read the 'level' from config
    # TODO read the list of 'commodity' from context/config
    # TODO separate dummy supplies by commodity

    data = make_source_tech(
        context["transport build info"],
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
