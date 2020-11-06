"""Generate input data."""
from collections import defaultdict
import logging

import pandas as pd

from message_data.tools import (
    ScenarioInfo,
    add_par_data,
    broadcast,
    get_context,
    make_io,
    make_matched_dfs,
    make_source_tech,
    same_node,
)
from message_data.model.transport.utils import add_commodity_and_level
from .groups import get_consumer_groups  # noqa: F401
from .ldv import get_ldv_data
from .non_ldv import get_non_ldv_data


log = logging.getLogger(__name__)

DATA_FUNCTIONS = [
    get_ldv_data,
    get_non_ldv_data,
]


def add_data(scenario, dry_run=False):
    """Populate `scenario` with MESSAGE-Transport data."""
    # Information about `scenario`
    info = ScenarioInfo(scenario)

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")

    from message_data.model.transport import demand

    DATA_FUNCTIONS.insert(0, demand.demand)

    for func in DATA_FUNCTIONS:
        # Generate or load the data; add to the Scenario
        log.info(f'from {func.__name__}()')
        add_par_data(scenario, func(info), dry_run=dry_run)

    log.info('done')


def demand(info):
    """Return transport demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    import message_data.transport.demand as demand_module

    config = get_context()["transport config"]["data source"]
    func = getattr(demand_module, config["demand"])

    return dict(demand=func(info))


DATA_FUNCTIONS.append(demand)


def conversion(info):
    """Input and output data for conversion technologies:

    The technologies are named 'transport {mode} load factor'.
    """
    cfg = get_context()['transport config']

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        mode='all',
        # No subannual detail
        time='year',
        time_origin='year',
        time_dest='year',
    )

    mode_info = [
        ('freight', cfg['factor']['freight load'], 't km'),
        ('pax', 1.0, 'km'),
    ]

    data = defaultdict(list)
    for mode, factor, output_unit in mode_info:
        i_o = make_io(
            (f'transport {mode} vehicle', 'useful', 'km'),
            (f'transport {mode}', 'useful', output_unit),
            factor,
            on='output',
            technology=f'transport {mode} load factor',
            **common
        )
        for par, df in i_o.items():
            data[par].append(
                df.pipe(broadcast, node_loc=info.N[1:])
                .pipe(same_node)
            )

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data['input'],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data


DATA_FUNCTIONS.append(conversion)


def freight(info):
    """Data for freight technologies."""
    codes = get_context()["transport set"]["technology"]["add"]
    freight_truck = codes[codes.index("freight truck")]

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        mode='all',
        time='year',  # no subannual detail
        time_dest='year',
        time_origin='year',
    )

    data = defaultdict(list)
    for tech in freight_truck.child:
        i_o = make_io(
            src=(None, None, 'GWa'),
            dest=('transport freight vehicle', 'useful', 'km'),
            efficiency=1.,
            on='input',
            technology=tech.id,
            **common,
        )

        i_o['input'] = add_commodity_and_level(i_o['input'], 'final')

        for par, df in i_o.items():
            data[par].append(
                df.pipe(broadcast, node_loc=info.N[1:])
                .pipe(same_node)
            )

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data['input'],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data


DATA_FUNCTIONS.append(freight)


def dummy_supply(info):
    """Dummy fuel supply for the bare RES."""
    return make_source_tech(
        info,
        common=dict(
            commodity="lightoil",
            level="final",
            mode="all",
            technology="DUMMY transport fuel",
            time="year",
            time_dest="year",
            unit="GWa",
        ),
        output=1.0,
        var_cost=1.0,
        technical_lifetime=1.0,
    )


DATA_FUNCTIONS.append(dummy_supply)
