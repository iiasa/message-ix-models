"""Generate input data."""
from collections import defaultdict
from itertools import product
import logging

import pandas as pd

from message_data.tools import (
    ScenarioInfo,
    add_par_data,
    broadcast,
    copy_column,
    get_context,
    make_df,
    make_io,
    make_matched_dfs,
    same_node,
)
from message_data.model.transport.build import generate_set_elements
from message_data.model.transport.demand import demand
from message_data.model.transport.utils import add_commodity_and_level
from .groups import get_consumer_groups  # noqa: F401
from .ldv import get_ldv_data
from .non_ldv import get_non_ldv_data


log = logging.getLogger(__name__)


DATA_FUNCTIONS = [
    demand,
    'conversion',
    'freight',
    get_ldv_data,
    get_non_ldv_data,
    'dummy_supply',
]


def add_data(scenario, dry_run=False):
    """Populate *senario* with MESSAGE-Transport data."""
    # Add data
    info = ScenarioInfo(scenario)

    for func in DATA_FUNCTIONS:
        func = globals()[func] if isinstance(func, str) else func
        log.info(f'from {func.__name__}()')

        # Generate or load the data; add to the Scenario
        add_par_data(scenario, func(info), dry_run=dry_run)

    log.info('done')


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


def disutility_conversion(info):
    """Input and output data for disutility conversion technologies.

    The technologies are named 'transport {mode} load factor'.
    """
    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        # No subannual detail
        time='year',
        time_origin='year',
        time_dest='year',
    )

    technology = generate_set_elements("technology", "LDV")

    modes = list(map(str, generate_set_elements("consumer_group")))
    # Identical values, currently:
    # modes = list(map(str, generate_set_elements("mode")))

    data = defaultdict(list)
    for t in technology:
        i_o = make_io(
            (f"transport vehicle {t.id}", 'useful', 'km'),
            (None, "useful", "km"),
            1.0,
            on="output",
            technology=f"transport {t.id} load factor",
            **common
        )
        for par, df in i_o.items():
            df = df.pipe(broadcast, node_loc=info.N[1:]).pipe(same_node)
            if par == "input":
                # Common across modes
                data[par].append(df.assign(mode="all"))
                # Disutility inputs differ by mode
                data[par].append(
                    df.assign(commodity="disutility")
                    .pipe(broadcast, mode=modes)
                )
            elif par == "output":
                data[par].append(
                    df.pipe(broadcast, mode=modes)
                    .assign(commodity=lambda df: "transport pax " + df["mode"])
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


def dummy_supply(info):
    """Dummy fuel supply for the bare RES."""
    common = dict(
        commodity='lightoil',
        level='final',
        mode='all',
        technology='DUMMY transport fuel',
        time='year',
        time_dest='year',
        unit='GWa',
        value=1.0,
        year_act=info.Y,
        year_vtg=info.Y,
    )

    result = dict(
        output=(
            make_df('output', **common)
            .pipe(broadcast, node_loc=info.N[1:])
            .pipe(same_node)
        )
    )

    result.update(
        make_matched_dfs(
            base=result['output'],
            capacity_factor=1,
            var_cost=1,
            technical_lifetime=10,
        )
    )

    return result
